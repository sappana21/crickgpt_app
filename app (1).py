import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks import sql as dbsql
import re

st.set_page_config(page_title="CrickGPT 🏏", page_icon="🏏")
st.title(" CrickGPT — Ask Anything About IPL")

w = WorkspaceClient()

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

def run_sql_query(query):
    connection = dbsql.connect(
        server_hostname="dbc-ed3df5bc-5c71.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/c9e515f30b6856b4",
        credentials_provider=lambda: w.config.authenticate
    )
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    result_data = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    connection.close()
    return result_data

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
        result_data = run_sql_query(clean_sql)[:10]
    except Exception as e:
        return f"Sorry, I couldn't process that. Error: {e}", None

    explain_prompt = f"""The user asked: "{user_question}"
The query result is: {result_data}

Answer like a knowledgeable cricket friend chatting casually. Be specific with numbers when relevant,
sound natural and enthusiastic, 1-3 sentences. Don't mention SQL, queries, tables, or databases at all."""

    explain_response = w.serving_endpoints.query(
        name="databricks-llama-4-maverick",
        messages=[ChatMessage(role=ChatMessageRole.USER, content=explain_prompt)],
        max_tokens=150
    )
    return explain_response.choices[0].message.content.strip(), result_data
with st.sidebar:
    st.header("🏏 CrickGPT")
    st.write("Ask me anything about IPL stats!")
    if st.button("🔄 Clear Chat"):
        st.session_state.messages = []
        st.rerun()
if "messages" not in st.session_state:
    st.session_state.messages = []
clicked_question = None
if len(st.session_state.messages) == 0:
    st.write("**Try asking:**")
    col1, col2 = st.columns(2)
    suggestions = [
        "Who has scored the most runs?",
        "Who has taken the most wickets?",
        "CSK vs MI head to head record?",
        "Who won IPL 2023 final?"
    ]
    with col1:
        if st.button(suggestions[0]):
            clicked_question = suggestions[0]
        if st.button(suggestions[1]):
            clicked_question = suggestions[1]
    with col2:
        if st.button(suggestions[2]):
            clicked_question = suggestions[2]
        if st.button(suggestions[3]):
            clicked_question = suggestions[3]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

typed_question = st.chat_input("Ask about IPL stats, players, teams...")
question = typed_question or clicked_question

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            answer, data = ask_cricket_question(question)
            st.write(answer)

    st.session_state.messages.append({"role": "assistant", "content": answer})
    st.rerun()
