# Real-Time AI Anomaly Detection Pipeline (Medallion Architecture)

## Project Overview
An end-to-end data pipeline designed to ingest, process, and secure web traffic logs using a Medallion Architecture. This project processes simulated ticketing logs to detect heuristic anomalies (ticket scalping bots) and delivers a highly concurrent serving layer for Business Intelligence teams.

*Note: Live cloud infrastructure (Databricks clusters/Snowflake warehouses) has been spun down post-development to optimize and avoid unnecessary ongoing cloud billing.*

## Tech Stack
* **Ingestion & Processing:** Databricks, PySpark (Structured Streaming), Auto Loader
* **Data Warehousing:** Snowflake
* **Cloud Infrastructure:** Azure Data Lake Storage (ADLS Gen2)
* **Language:** Python, SQL

## Architectural Highlights

### 1. Cost-Optimized Streaming (`availableNow=True`)
The Bronze-to-Silver ETL is built on PySpark's Structured Streaming API to leverage robust checkpointing, but utilizes the `.trigger(availableNow=True)` configuration. This allows the pipeline to act as a scheduled batch job—processing the entire queue of new files and immediately shutting the cluster down to save compute costs.

### 2. Explicit Schema Definition & Flattening
Instead of relying on expensive schema inference, the pipeline uses explicitly defined `StructType` schemas to parse heavily nested, stringified JSON logs via `from_json`. This resolves schema collisions and creates a highly queryable tabular format.

### 3. Vectorized Feature Engineering
To prepare the data for downstream machine learning and immediate security analytics, vectorized PySpark functions (`when`, `col`) evaluate HTTP status codes natively to generate an `anomaly_score` for every request, flagging suspected bots prior to database insertion.

### 4. Zero-Trust Security (Snowflake RBAC)
The handoff between Databricks and Snowflake utilizes strict Role-Based Access Control (RBAC). A dedicated `ticketing_pipeline_role` enforces the Principle of Least Privilege, granting the ingestion service account `INSERT` permissions exclusively to the required schema, isolating the blast radius.

### 5. Analytics Engineering (ELT)
The final Gold layer utilizes a CTAS operation inside Snowflake to materialize an aggregated data mart (`gold_ticketing_insights`). Pre-calculating complex metrics (like server success rate percentages) drastically reduces BI dashboard load times and Snowflake compute usage. 

### 6. Circuit Breaker Data Quality (DQ)
The pipeline utilizes strict schema validation upon ingestion. If unexpected schema drift occurs that violates the explicit `StructType` definitions, the pipeline breaks the circuit, preventing corrupted data from entering the Silver layer.
