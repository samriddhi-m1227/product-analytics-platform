from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RAW_PATH = "data/raw/events" #ingested data 
CLEAN_PATH = "data/clean/events" #loading here 

ALLOWED_EVENT_NAMES = ["signup", "login", "feature_use", "purchase", "logout"]
ALLOWED_PLATFORMS = ["web", "ios", "android"]


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("clean-events")
        # For local runs; Spark will pick a sensible default if you omit this,
        # but setting it explicitly avoids confusion.
        .master("local[*]") #run on my machine using all CPU
        .getOrCreate() #use existing spark session otherwise create 
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1) Read raw JSON (Spark will read all partitions under RAW_PATH)
    df_raw = spark.read.json(RAW_PATH)

    print("Raw schema:")
    df_raw.printSchema()

    # 2) Standardize strings
    # - trim whitespace
    # - lowercase event_name + platform
    df_std = (
        df_raw
        .withColumn("event_name", F.lower(F.trim(F.col("event_name")))) #make it lowercase, no whitespace and reassign these updates to event_name col
        .withColumn("platform", F.lower(F.trim(F.col("platform"))))
    )

    # 3) Parse timestamps
    # event_time format is like: 2026-01-06T14:29:45Z
    # Spark can parse this with a format string.
    df_time = df_std.withColumn(
        "event_ts",
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX")
    )

    # 4) Derive event_date (for partitioning)
    df_enriched = df_time.withColumn("event_date", F.to_date(F.col("event_ts")))

    # 5) Validate required fields (drop bad rows that are not accepted)
    # Required fields: event_id, event_name, event_ts, user_id, session_id, platform, schema_version, properties
    df_valid = (
        df_enriched
        .filter(F.col("event_id").isNotNull() & (F.col("event_id") != ""))
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull() & (F.col("session_id") != ""))
        .filter(F.col("schema_version").isNotNull())
        .filter(F.col("schema_version") == 1) #for now atleast
        .filter(F.col("event_name").isin(ALLOWED_EVENT_NAMES))
        .filter(F.col("platform").isin(ALLOWED_PLATFORMS))
        .filter(F.trim(F.col("event_id")) != "")
        .filter(F.trim(F.col("session_id")) != "")

    )

    # 6) Deduplicate on event_id (simulated retries) these need to be unique 
    df_dedup = df_valid.dropDuplicates(["event_id"])

    # Spark may infer properties as struct/map; convert to JSON string for stability.
    df_out = df_dedup.withColumn("properties_json", F.to_json(F.col("properties")))

    # final columns in clean order 
    df_out = df_out.select(
        "event_id",
        "event_name",
        "event_time", #raw timestamp sent by the "app", kept for debugging and data integrity
        "event_ts", #spark timestamp (use this mostly)
        "event_date",
        "user_id",
        "session_id",
        "platform",
        "schema_version",
        "properties_json",
    )

    # Debug counts so you can see what cleaning did
    raw_count = df_raw.count()
    valid_count = df_valid.count()
    out_count = df_out.count()

    print(f"Raw rows:   {raw_count:,}")
    print(f"Valid rows: {valid_count:,}")
    print(f"Output rows after dedupe: {out_count:,}")

    # 7) Write clean Parquet partitioned by event_date
    (
        df_out.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(CLEAN_PATH) #write the parquet file here
    )

    print(f"[OK] Wrote clean parquet to: {CLEAN_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
