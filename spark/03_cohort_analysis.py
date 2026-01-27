from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, min as f_min, countDistinct,
    months_between, floor
)

def main():
    spark = (
        SparkSession.builder
        .appName("03_cohort_analysis")
        .getOrCreate()
    )

    PROJECT_DIR = Path(__file__).resolve().parents[1]
    DATA_DIR = PROJECT_DIR / "data_raw"
    OUT_DIR = PROJECT_DIR / "outputs"

    tx_path = str(DATA_DIR / "transactions.json")

    # 1) Read transactions
    tx = spark.read.option("multiLine", True).json(tx_path)

    # 2) Parse timestamp (your generator uses ISO like: 2025-03-12T14:37:22)
    tx = tx.withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Keep only what we need
    tx = tx.select(
        col("transaction_id"),
        col("user_id"),
        col("ts")
    ).dropna(subset=["user_id", "ts"])

    # 3) Order month
    tx = tx.withColumn("order_month", date_trunc("month", col("ts")))

    # 4) Cohort month = user's first order month
    first_order = (
        tx.groupBy("user_id")
          .agg(f_min("order_month").alias("cohort_month"))
    )

    tx2 = tx.join(first_order, on="user_id", how="inner")

    # 5) Cohort index = months since first purchase
    tx2 = tx2.withColumn(
        "cohort_index",
        floor(months_between(col("order_month"), col("cohort_month")))
    )

    # 6) Retention table: unique users per cohort_month and cohort_index
    retention = (
        tx2.groupBy("cohort_month", "cohort_index")
           .agg(countDistinct("user_id").alias("active_users"))
           .orderBy("cohort_month", "cohort_index")
    )

    # 7) Also compute cohort size (index 0)
    cohort_size = (
        retention.where(col("cohort_index") == 0)
                 .select(col("cohort_month").alias("cm"), col("active_users").alias("cohort_size"))
    )

    retention2 = (
        retention.join(cohort_size, retention.cohort_month == cohort_size.cm, "left")
                 .drop("cm")
                 .withColumn("retention_rate", (col("active_users") / col("cohort_size")))
    )

    # 8) Save outputs
    out_csv_dir = str(OUT_DIR / "cohort_retention_csv")
    retention2.coalesce(1).write.mode("overwrite").option("header", True).csv(out_csv_dir)

    print(f"\n Saved cohort retention CSV folder: {out_csv_dir}")

    spark.stop()

if __name__ == "__main__":
    main()


