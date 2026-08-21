#  CrickGPT — IPL Cricket Analytics Chatbot

CrickGPT is a GenAI-powered chatbot that lets you ask natural language questions about IPL (Indian Premier League) cricket stats — player performances, team records, match results, and more — and get instant, conversational answers backed by real data.

Built end-to-end on the **Databricks Lakehouse Platform**, this project demonstrates a complete data + AI pipeline: raw data ingestion, Delta Lake medallion architecture (Bronze → Silver → Gold), LLM-powered Text-to-SQL, and a deployed interactive Databricks App.

##  What it does

Ask questions like:
- "Who has scored the most runs in IPL history?"
- "Who has taken the most wickets?"
- "What's the CSK vs MI head-to-head record?"
- "Who won the IPL 2023 final?"

CrickGPT converts your question into a SQL query, runs it against curated cricket stats tables, and responds with a friendly, natural-language answer — no SQL or database knowledge needed.

##  Architecture

1. **Data Source** — [IPL Complete Dataset (2008–2024)](https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020) from Kaggle (match-level and ball-by-ball delivery data)
2. **Bronze Layer** — Raw CSV/Excel data ingested into Delta tables as-is
3. **Silver Layer** — Cleaned, deduplicated, and type-corrected Delta tables
4. **Gold Layer** — Aggregated analytics tables:
   - `batting_stats_gold` — player-wise runs, strike rate, boundaries
   - `bowling_stats_gold` — player-wise wickets, economy rate
   - `head_to_head_gold` — team vs team win records
   - `season_summary_gold` — season-wise finals and winners
5. **LLM Layer** — Databricks-hosted **Llama 4 Maverick** model used for:
   - Converting natural language questions into Spark SQL (Text-to-SQL)
   - Converting query results back into natural, conversational answers
6. **App Layer** — A **Streamlit** chat interface deployed as a **Databricks App**, connected to the Gold tables via Databricks SQL Warehouse

##  Tech Stack

- **Databricks** (Free Edition) — Delta Lake, Unity Catalog, SQL Warehouse, Model Serving, Databricks Apps
- **PySpark** — data cleaning and transformation
- **Delta Lake** — medallion architecture (Bronze/Silver/Gold)
- **Llama 4 Maverick** (via Databricks Foundation Model APIs) — Text-to-SQL and natural language generation
- **Streamlit** — chatbot front-end
- **Databricks SQL Connector** — querying Gold tables from the deployed app

##  Project Structure

```
crickgpt/
├── app.py              # Streamlit chatbot application
├── app.yaml            # Databricks App configuration
├── requirements.txt    # Python dependencies
└── README.md
```

##  How it Works (Pipeline)

```
Kaggle CSV/Excel → Bronze Delta Tables → Silver (cleaned) → Gold (aggregated stats)
                                                                     │
User Question → Llama 4 Maverick (Text-to-SQL) → SQL Warehouse → Gold Tables
                                                                     │
                                            Query Result → Llama 4 Maverick (Natural Language) → Chat Response
```

##  Features

- Conversational Q&A over real IPL statistics
- LLM-generated SQL queries with no manual query writing
- Natural, friendly responses instead of raw tables
- Suggested starter questions for new users
- Chat reset/clear functionality
- Fully deployed as a live Databricks App

##  Future Improvements

- Add player photos/team logos to responses
- Support for more advanced multi-turn conversational context
- Add data visualizations (charts) alongside answers
- Expand dataset with ball-by-ball commentary for richer RAG-based answers

##  Keywords

`databricks` `genai` `llm` `text-to-sql` `chatbot` `cricket-analytics` `ipl` `delta-lake` `pyspark` `streamlit` `data-engineering` `medallion-architecture` `llama` `unity-catalog` `databricks-apps` `data-analytics` `nlp` `sql` `machine-learning` `lakehouse`

##  License

This project uses publicly available IPL data for educational and portfolio purposes.
