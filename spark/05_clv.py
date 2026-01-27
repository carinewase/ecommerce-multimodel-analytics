from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum as Fsum, countDistinct, min as Fmin, max as Fmax, datediff

PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data_raw"
OUT_DIR  = PROJECT_DIR / "outputs"

def main():
    spark = SparkSession.builder.appName("05_clv").getOrCreate()

    tx = spark.read.json(str(DATA_DIR / "transactions.json"))

    # user-level revenue + orders + active span
    clv = (
        tx.withColumn("ts", to_timestamp("timestamp"))
          .groupBy("user_id")
          .agg(
              Fsum(col("total").cast("double")).alias("total_revenue"),
              countDistinct("transaction_id").alias("num_orders"),
              Fmin("ts").alias("first_purchase"),
              Fmax("ts").alias("last_purchase"),
          )
          .withColumn("active_days", datediff(col("last_purchase"), col("first_purchase")))
          .orderBy(col("total_revenue").desc())
    )

    clv.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUT_DIR / "clv_csv"))
    print(" Saved outputs/clv_csv (CSV folder)")

    spark.stop()

if __name__ == "__main__":
    main()
