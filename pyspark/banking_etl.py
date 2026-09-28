"""
banking_etl.py
---------------
PySpark ETL pipeline for the Banking Transaction Data Pipeline project.

WHY PYSPARK (even though this dataset fits in memory on a laptop)?
--------------------------------------------------------------------
The raw dataset here is only ~55k rows, so Pandas alone would technically
work. PySpark is used deliberately in this project to demonstrate the
distributed-processing skill set expected of a Data Engineer:
    - Explicit schema enforcement (instead of relying on inferSchema)
    - Declarative, lazily-evaluated transformations (select/filter/
      withColumn/when/groupBy/agg) that scale from thousands to billions
      of rows with the same code
    - SQL-style DataFrame API that maps directly onto the SQL analysis
      done later in MySQL
    - A pattern (ingest -> validate -> clean -> transform -> aggregate ->
      load) that mirrors real production banking ETL pipelines, which
      typically run on Spark/Databricks-style clusters over much larger
      transaction volumes than a single CSV file.
In short: the tools here are chosen to reflect how this pipeline would be
built in a real banking data-engineering role, not because 55k rows
strictly require a distributed engine.

PIPELINE STAGES
----------------
RAW CSV -> Spark ingestion (explicit schema) -> validation -> cleaning
-> transformation (derived columns) -> aggregation -> output
(CSV/Parquet locally, optionally MySQL via JDBC).

OUTPUTS
--------
data/processed/spark_output/
    cleaned_transactions/            (cleaned, transformed transaction grain)
    customer_summary/                (per-customer aggregates)
    monthly_summary/                 (per-month aggregates)
    branch_summary/                  (per-branch aggregates)
    payment_method_summary/          (per-payment-method aggregates)
    risk_transaction_summary/        (rule-based risk aggregates)

MYSQL LOADING
--------------
Set the following environment variables to also load the outputs into
MySQL via JDBC (see sql/create_database.sql and sql/create_tables.sql for
the target schema):
    MYSQL_HOST      (default: localhost)
    MYSQL_PORT      (default: 3306)
    MYSQL_DB        (default: banking_analytics)
    MYSQL_USER      (default: root)
    MYSQL_PASSWORD  (no default - must be set, never hardcoded)
    MYSQL_JAR_PATH  (path to mysql-connector-j-*.jar, required for JDBC)

If MYSQL_PASSWORD is not set, the script skips the MySQL load step and
only writes local CSV/Parquet outputs.
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "banking_transactions_raw.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "spark_output")

VALID_TRANSACTION_TYPES = ["Debit", "Credit", "Transfer", "Withdrawal"]
VALID_ACCOUNT_TYPES = ["Savings", "Current"]
VALID_STATUSES = ["Success", "Failed", "Pending"]
VALID_PAYMENT_METHODS = ["UPI", "Debit Card", "Credit Card", "NEFT", "IMPS", "RTGS", "Cash"]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BankingTransactionETL")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        # ANSI mode off so that invalid date/time strings parse to NULL
        # instead of throwing errors (behaviour consistent across Spark
        # 3.x and 4.x; invalid inputs are then filtered explicitly below)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def get_raw_schema() -> StructType:
    """Explicit schema instead of relying on inferSchema, which is slow
    and unreliable on messy real-world data (e.g. it can silently coerce
    invalid dates/numbers to null or the wrong type)."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_time", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("balance_before", DoubleType(), True),
        StructField("balance_after", DoubleType(), True),
        StructField("account_type", StringType(), True),
        StructField("customer_age", DoubleType(), True),
        StructField("customer_gender", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("is_international", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("failed_attempts", IntegerType(), True),
        StructField("previous_transaction_amount", DoubleType(), True),
        StructField("customer_risk_score", DoubleType(), True),
    ])


def ingest(spark: SparkSession):
    """STAGE 1-2: Ingest raw CSV with explicit schema."""
    df = (
        spark.read
        .option("header", True)
        .schema(get_raw_schema())
        .csv(RAW_PATH)
    )
    print(f"[INGEST] Raw rows read: {df.count()}")
    return df


def validate_and_clean(df):
    """STAGE 3-4: Validation + cleaning.

    - dropDuplicates on full row and on transaction_id
    - normalize categorical values (case-insensitive mapping)
    - cast/clean numeric fields
    - filter out invalid amounts / unparsable dates
    """
    # Remove fully duplicated rows, then duplicate transaction_id (keep first)
    df = df.dropDuplicates()
    df = df.dropDuplicates(["transaction_id"])

    def standardize_col(colname, valid_values):
        col = F.trim(F.col(colname))
        expr = F.lit(None).cast(StringType())
        for v in valid_values:
            expr = F.when(F.lower(col) == v.lower(), F.lit(v)).otherwise(expr)
        return expr

    df = (
        df.withColumn("transaction_type", standardize_col("transaction_type", VALID_TRANSACTION_TYPES))
          .withColumn("account_type", standardize_col("account_type", VALID_ACCOUNT_TYPES))
          .withColumn("transaction_status", standardize_col("transaction_status", VALID_STATUSES))
          .withColumn("payment_method", standardize_col("payment_method", VALID_PAYMENT_METHODS))
    )

    df = df.withColumn(
        "customer_gender",
        F.when(F.lower(F.trim(F.col("customer_gender"))).isin("male", "m"), "Male")
         .when(F.lower(F.trim(F.col("customer_gender"))).isin("female", "f"), "Female")
         .otherwise("Unknown"),
    )

    # Drop rows where critical categorical standardization failed
    df = df.filter(
        F.col("transaction_type").isNotNull()
        & F.col("account_type").isNotNull()
        & F.col("transaction_status").isNotNull()
    )

    # Fill non-critical missing values
    df = (
        df.fillna({"city": "Unknown", "merchant_category": "Unknown", "payment_method": "Unknown"})
    )

    # Parse transaction_date, drop unparsable dates
    # (ANSI mode is disabled above so invalid strings -> NULL, which we
    # then filter out explicitly)
    df = df.withColumn("transaction_date_parsed", F.to_date("transaction_date", "yyyy-MM-dd"))
    df = df.filter(F.col("transaction_date_parsed").isNotNull())

    # Valid amount only
    df = df.filter(F.col("amount") > 0)

    # Clip customer_age to realistic range, fill nulls with median-like constant (45)
    median_age = df.filter(F.col("customer_age").between(15, 100)).agg(F.expr("percentile_approx(customer_age, 0.5)")).first()[0]
    # Some Spark versions return an array from percentile_approx - unwrap it
    if isinstance(median_age, (list, tuple)):
        median_age = median_age[0]
    median_age = median_age if median_age is not None else 45.0
    df = df.withColumn(
        "customer_age",
        F.when(F.col("customer_age").isNull(), F.lit(median_age))
         .when(F.col("customer_age") < 15, F.lit(median_age))
         .when(F.col("customer_age") > 100, F.lit(median_age))
         .otherwise(F.col("customer_age")),
    )

    # Recalculate balance_after where missing
    expected_balance_after = F.when(
        F.col("transaction_type") == "Credit",
        F.col("balance_before") + F.col("amount"),
    ).otherwise(F.col("balance_before") - F.col("amount"))
    df = df.withColumn(
        "balance_after",
        F.when(F.col("balance_after").isNull(), expected_balance_after).otherwise(F.col("balance_after")),
    )
    df = df.withColumn("balance_after", F.when(F.col("balance_after") < 0, F.lit(0.0)).otherwise(F.col("balance_after")))

    row_count = df.count()
    print(f"[CLEAN] Rows after validation & cleaning: {row_count}")
    return df


def transform(df):
    """STAGE 5: Derived columns / feature engineering."""
    df = (
        df.withColumn("transaction_year", F.year("transaction_date_parsed"))
          .withColumn("transaction_month", F.month("transaction_date_parsed"))
          .withColumn("transaction_day", F.dayofmonth("transaction_date_parsed"))
          .withColumn("transaction_hour", F.hour(F.to_timestamp("transaction_time", "HH:mm:ss")))
          .withColumn(
              "transaction_amount_bucket",
              F.when(F.col("amount") < 1000, "Very Low (<1K)")
               .when(F.col("amount") < 5000, "Low (1K-5K)")
               .when(F.col("amount") < 20000, "Medium (5K-20K)")
               .when(F.col("amount") < 50000, "High (20K-50K)")
               .otherwise("Very High (50K+)"),
          )
          .withColumn(
              "customer_age_group",
              F.when(F.col("customer_age") <= 25, "18-25")
               .when(F.col("customer_age") <= 35, "26-35")
               .when(F.col("customer_age") <= 45, "36-45")
               .when(F.col("customer_age") <= 55, "46-55")
               .when(F.col("customer_age") <= 65, "56-65")
               .otherwise("66+"),
          )
    )
    return df


def aggregate(df):
    """STAGE 6: Aggregated datasets for reporting / MySQL loading."""

    customer_summary = (
        df.groupBy("customer_id")
          .agg(
              F.count("transaction_id").alias("total_transactions"),
              F.sum("amount").alias("total_amount"),
              F.avg("amount").alias("avg_amount"),
              F.max("amount").alias("max_amount"),
              F.sum(F.when(F.col("transaction_status") == "Failed", 1).otherwise(0)).alias("failed_transactions"),
              F.sum(F.col("is_international")).alias("international_transactions"),
          )
          .orderBy(F.desc("total_amount"))
    )

    monthly_summary = (
        df.groupBy("transaction_year", "transaction_month")
          .agg(
              F.count("transaction_id").alias("total_transactions"),
              F.sum("amount").alias("total_amount"),
              F.avg("amount").alias("avg_amount"),
              F.sum(F.when(F.col("transaction_status") == "Success", 1).otherwise(0)).alias("successful_transactions"),
          )
          .orderBy("transaction_year", "transaction_month")
    )

    branch_summary = (
        df.groupBy("branch", "city", "state")
          .agg(
              F.count("transaction_id").alias("total_transactions"),
              F.sum("amount").alias("total_amount"),
              F.avg("amount").alias("avg_amount"),
          )
          .orderBy(F.desc("total_amount"))
    )

    payment_method_summary = (
        df.groupBy("payment_method")
          .agg(
              F.count("transaction_id").alias("total_transactions"),
              F.sum("amount").alias("total_amount"),
              F.avg("amount").alias("avg_amount"),
              F.sum(F.when(F.col("transaction_status") == "Failed", 1).otherwise(0)).alias("failed_transactions"),
          )
          .orderBy(F.desc("total_transactions"))
    )

    # Simple rule-based risk summary at the transaction grain (mirrors
    # sql/risk_analysis.sql rules, kept intentionally simple here since
    # the authoritative risk scoring is implemented in SQL). Flags a
    # transaction as "high value" if it exceeds 3x the customer's average.
    window_avg = df.groupBy("customer_id").agg(F.avg("amount").alias("customer_avg_amount"))
    risk_df = df.join(window_avg, on="customer_id", how="left")
    risk_df = risk_df.withColumn(
        "is_high_value_outlier",
        F.when(F.col("amount") > 3 * F.col("customer_avg_amount"), 1).otherwise(0),
    )
    risk_transaction_summary = (
        risk_df.groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("is_high_value_outlier").alias("high_value_outlier_count"),
            F.sum(F.when(F.col("transaction_status") == "Failed", 1).otherwise(0)).alias("failed_transactions"),
            F.sum(F.col("is_international")).alias("international_transactions"),
            F.max("customer_risk_score").alias("customer_risk_score"),
        )
        .orderBy(F.desc("high_value_outlier_count"))
    )

    return {
        "customer_summary": customer_summary,
        "monthly_summary": monthly_summary,
        "branch_summary": branch_summary,
        "payment_method_summary": payment_method_summary,
        "risk_transaction_summary": risk_transaction_summary,
    }


def write_outputs(df, aggregates: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cleaned_path = os.path.join(OUTPUT_DIR, "cleaned_transactions")
    (
        df.drop("transaction_date_parsed")
          .coalesce(1)
          .write.mode("overwrite").option("header", True)
          .csv(cleaned_path)
    )
    print(f"[WRITE] cleaned_transactions -> {cleaned_path}")

    for name, agg_df in aggregates.items():
        out_path = os.path.join(OUTPUT_DIR, name)
        agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        print(f"[WRITE] {name} -> {out_path}")


def load_to_mysql(df, aggregates: dict):
    """STAGE 7 (optional): Load cleaned transactions + aggregates into
    MySQL via JDBC. Requires a MySQL Connector/J jar on the classpath
    (set MYSQL_JAR_PATH) and MYSQL_PASSWORD to be set as an environment
    variable. Credentials are NEVER hardcoded here."""
    mysql_password = os.environ.get("MYSQL_PASSWORD")
    if not mysql_password:
        print("[MYSQL] MYSQL_PASSWORD not set - skipping MySQL load. "
              "Set MYSQL_HOST/MYSQL_PORT/MYSQL_DB/MYSQL_USER/MYSQL_PASSWORD "
              "and MYSQL_JAR_PATH env vars to enable this step.")
        return

    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    db = os.environ.get("MYSQL_DB", "banking_analytics")
    user = os.environ.get("MYSQL_USER", "root")

    jdbc_url = f"jdbc:mysql://{host}:{port}/{db}"
    connection_props = {
        "user": user,
        "password": mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    df.drop("transaction_date_parsed").write.jdbc(
        url=jdbc_url, table="transactions", mode="append", properties=connection_props
    )
    print("[MYSQL] Loaded transactions table")

    table_map = {
        "customer_summary": "customer_summary",
        "monthly_summary": "monthly_transaction_summary",
        "risk_transaction_summary": "transaction_risk_summary",
    }
    for agg_name, table_name in table_map.items():
        aggregates[agg_name].write.jdbc(
            url=jdbc_url, table=table_name, mode="overwrite", properties=connection_props
        )
        print(f"[MYSQL] Loaded {table_name} table")


def main():
    spark = build_spark_session()
    try:
        raw_df = ingest(spark)
        clean_df = validate_and_clean(raw_df)
        transformed_df = transform(clean_df)
        aggregates = aggregate(transformed_df)

        write_outputs(transformed_df, aggregates)
        load_to_mysql(transformed_df, aggregates)

        print("\n[ETL COMPLETE] Banking transaction ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
