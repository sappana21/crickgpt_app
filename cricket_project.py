# Databricks notebook source
import zipfile
import os

volume_path = "/Volumes/workspace/cricket_project/cricket_project_volume"
zip_file_path = f"{volume_path}/deliveries.csv.zip"
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(volume_path)
    print("Extracted files:", zip_ref.namelist())

# COMMAND ----------

matches_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{volume_path}/matches.csv")
matches_df.write.format("delta").mode("overwrite").saveAsTable("cricket_project.matches_bronze")

# COMMAND ----------

deliveries_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{volume_path}/deliveries.csv")
deliveries_df.write.format("delta").mode("overwrite").saveAsTable("cricket_project.deliveries_bronze")

# COMMAND ----------

display(spark.sql("SELECT * FROM cricket_project.matches_bronze LIMIT 5"))
display(spark.sql("SELECT * FROM cricket_project.deliveries_bronze LIMIT 5"))

# COMMAND ----------

matches_bronze = spark.table("cricket_project.matches_bronze")
deliveries_bronze = spark.table("cricket_project.deliveries_bronze")

print("MATCHES columns:")
print(matches_bronze.columns)
print("\nDELIVERIES columns:")
print(deliveries_bronze.columns)

print("\nMatches count:", matches_bronze.count())
print("Deliveries count:", deliveries_bronze.count())

# COMMAND ----------

from pyspark.sql.functions import col, to_date

matches_silver = matches_bronze \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .dropDuplicates(["id"]) \
    .filter(col("team1").isNotNull() & col("team2").isNotNull())

matches_silver.write.format("delta").mode("overwrite").saveAsTable("cricket_project.matches_silver")


deliveries_silver = deliveries_bronze \
    .dropDuplicates(["match_id", "inning", "over", "ball", "batter"]) \
    .filter(col("batting_team").isNotNull() & col("bowling_team").isNotNull())

deliveries_silver.write.format("delta").mode("overwrite").saveAsTable("cricket_project.deliveries_silver")

print("Matches silver count:", matches_silver.count())
print("Deliveries silver count:", deliveries_silver.count())

# COMMAND ----------

match_ids_in_matches = matches_silver.select("id").distinct().count()
match_ids_in_deliveries = deliveries_silver.select("match_id").distinct().count()

print("Unique match IDs in matches_silver:", match_ids_in_matches)
print("Unique match IDs in deliveries_silver:", match_ids_in_deliveries)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, when, round as _round

batting_stats = deliveries_silver.groupBy("batter") \
    .agg(
        _sum("batsman_runs").alias("total_runs"),
        count("ball").alias("balls_faced"),
        _sum(when(col("batsman_runs") == 4, 1).otherwise(0)).alias("fours"),
        _sum(when(col("batsman_runs") == 6, 1).otherwise(0)).alias("sixes")
    ) \
    .withColumn("strike_rate", _round((col("total_runs") / col("balls_faced")) * 100, 2)) \
    .orderBy(col("total_runs").desc())

batting_stats.write.format("delta").mode("overwrite").saveAsTable("cricket_project.batting_stats_gold")
display(batting_stats.limit(10))

# COMMAND ----------

wickets_only = deliveries_silver.filter(
    (col("is_wicket") == 1) & 
    (~col("dismissal_kind").isin(["run out", "retired hurt", "obstructing the field"]))
)

bowling_stats = deliveries_silver.groupBy("bowler") \
    .agg(
        _sum("total_runs").alias("runs_conceded"),
        count("ball").alias("balls_bowled")
    ) \
    .join(
        wickets_only.groupBy("bowler").agg(count("is_wicket").alias("wickets")),
        on="bowler", how="left"
    ) \
    .fillna(0, subset=["wickets"]) \
    .withColumn("economy", _round(col("runs_conceded") / (col("balls_bowled") / 6), 2)) \
    .orderBy(col("wickets").desc())

bowling_stats.write.format("delta").mode("overwrite").saveAsTable("cricket_project.bowling_stats_gold")
display(bowling_stats.limit(10))

# COMMAND ----------

head_to_head = matches_silver.groupBy("team1", "team2", "winner").count() \
    .orderBy("team1", "team2")

head_to_head.write.format("delta").mode("overwrite").saveAsTable("cricket_project.head_to_head_gold")
display(head_to_head.limit(10))

# COMMAND ----------

season_summary = matches_silver.filter(col("match_type") == "Final") \
    .select("season", "team1", "team2", "winner", "venue") \
    .orderBy("season")

season_summary.write.format("delta").mode("overwrite").saveAsTable("cricket_project.season_summary_gold")
display(season_summary)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="databricks-llama-4-maverick",
    messages=[
        ChatMessage(role=ChatMessageRole.USER, content="Say hello in one line")
    ],
    max_tokens=100
)

print(response.choices[0].message.content)

# COMMAND ----------

schema_info = """
Table: cricket_project.batting_stats_gold
Columns: batter (string), total_runs (int), balls_faced (int), fours (int), sixes (int), strike_rate (float)

Table: cricket_project.bowling_stats_gold
Columns: bowler (string), runs_conceded (int), balls_bowled (int), wickets (int), economy (float)

Table: cricket_project.matches_silver
Columns: id, season, city, date, match_type, player_of_match, venue, team1, team2, toss_winner, toss_decision, winner, result, result_margin

Table: cricket_project.head_to_head_gold
Columns: team1, team2, winner, count

Table: cricket_project.season_summary_gold
Columns: season, team1, team2, winner, venue
"""

user_question = "Who scored the most runs?"

prompt = f"""You are a SQL expert. Given this schema:
{schema_info}

Write ONLY a valid Spark SQL query (no explanation, no markdown, just the raw SQL) to answer this question:
{user_question}
"""

response = w.serving_endpoints.query(
    name="databricks-llama-4-maverick",
    messages=[
        ChatMessage(role=ChatMessageRole.USER, content=prompt)
    ],
    max_tokens=300
)

sql_query = response.choices[0].message.content.strip()
print("Generated SQL:")
print(sql_query)

# COMMAND ----------

import re

# Agar LLM ne ```sql wagera add kiya hai to clean karo
clean_sql = re.sub(r"```sql|```", "", sql_query).strip()

print("Clean SQL:")
print(clean_sql)

# Ab isse actually run karo
try:
    result = spark.sql(clean_sql)
    display(result)
except Exception as e:
    print("Error running SQL:", e)

# COMMAND ----------

def ask_cricket_question(user_question):
    prompt = f"""You are a SQL expert. Given this schema:
{schema_info}

Write ONLY a valid Spark SQL query (no explanation, no markdown, just the raw SQL) to answer this question:
{user_question}
"""
    
    response = w.serving_endpoints.query(
        name="databricks-llama-4-maverick",
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=300
    )
    
    sql_query = response.choices[0].message.content.strip()
    clean_sql = re.sub(r"```sql|```", "", sql_query).strip()
    
    try:
        result_df = spark.sql(clean_sql)
        return clean_sql, result_df
    except Exception as e:
        return clean_sql, f"Error: {e}"

# Test karo
sql, result = ask_cricket_question("Which team has won the most matches?")
print("SQL:", sql)
display(result)

# COMMAND ----------

def ask_cricket_question_v2(user_question):
    # Step 1: SQL generate karo
    prompt = f"""You are a SQL expert. Given this schema:
{schema_info}

Write ONLY a valid Spark SQL query (no explanation, no markdown, just the raw SQL) to answer this question:
{user_question}
"""
    
    response = w.serving_endpoints.query(
        name="databricks-llama-4-maverick",
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=300
    )
    
    sql_query = response.choices[0].message.content.strip()
    clean_sql = re.sub(r"```sql|```", "", sql_query).strip()
    
    # Step 2: SQL run karo
    try:
        result_df = spark.sql(clean_sql)
        result_data = result_df.limit(10).toPandas().to_dict(orient="records")
    except Exception as e:
        return f"Sorry, I couldn't process that question. Error: {e}"
    
    # Step 3: Result ko natural language me convert karo
    explain_prompt = f"""The user asked: "{user_question}"

The query result is: {result_data}

Give a short, friendly, one-2 sentence answer in plain English based on this data. Don't mention SQL or technical details."""
    
    explain_response = w.serving_endpoints.query(
        name="databricks-llama-4-maverick",
        messages=[ChatMessage(role=ChatMessageRole.USER, content=explain_prompt)],
        max_tokens=150
    )
    
    return explain_response.choices[0].message.content.strip()

# Test karo!
answer = ask_cricket_question_v2("Who has the best strike rate among players with more than 500 runs?")
print(answer)