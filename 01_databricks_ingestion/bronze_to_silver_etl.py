from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Define Explicit Schemas (Optimization over inference)
raw_schema = StructType([
    StructField("id", StringType()),
    StructField("client_session", StringType()),
    StructField("event_context", StringType()),
    StructField("transaction_details", StringType())
])

session_schema = StructType([
    StructField("ip_address", StringType()),
    StructField("user_id", StringType())
])

context_schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("request_id", StringType()),
    StructField("server_node", StringType())
])

transaction_schema = StructType([
    StructField("http_status_code", IntegerType())
])

# 2. Bronze Layer: Ingest via Databricks Auto Loader
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .schema(raw_schema) 
    .option("cloudFiles.schemaLocation", f"{checkpoint_base_path}/schema_bronze")
    .load(bronze_path)
)

# 3. Silver Layer: Flatten Nested JSON
df_silver = df_raw.select(
    from_json(col("event_context"), context_schema).alias("ctx"),
    from_json(col("client_session"), session_schema).alias("sess"),
    from_json(col("transaction_details"), transaction_schema).alias("tx")
).select(
    col("ctx.event_id").alias("id"),
    col("ctx.timestamp"),
    col("ctx.request_id"),
    col("ctx.server_node"),
    col("sess.ip_address"),
    col("sess.user_id"),
    col("tx.http_status_code")
)

# 4. Feature Engineering: Vectorized AI Anomaly Tagging
df_silver_tagged = df_silver.withColumn(
    "anomaly_score",
    when(col("http_status_code") == 429, "SUSPECTED_BOT")
    .when(col("http_status_code") >= 500, "SERVER_ERROR")
    .otherwise("NORMAL")
)

# 5. Cost-Optimized Write Stream (Batch Processing via Streaming API)
query = (df_silver_tagged.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base_path}/silver_checkpoint")
    .trigger(availableNow=True) # Ensures cluster shuts down after queue is empty
    .toTable("ticket_sales_fact")
)

query.awaitTermination()
