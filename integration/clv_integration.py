# spark/clv_integration.py
# ----------------------------------------------------
# Customer Lifetime Value (CLV) Integration
# ----------------------------------------------------
# CLV = Sum of total spending per user
# Uses Spark batch outputs (NO live DB dependency)
# ----------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct
from pathlib import Path


def main():
    spark = (
        SparkSession.builder
        .appName("CLV Integration")
        .getOrCreate()
    )

    BASE_DIR = Path(__file__).resolve().parents[1]
    OUTPUTS = BASE_DIR / "outputs"

    # ---------------------------------------------
    # 1️⃣ Load normalized line items (REQUIRED)
    # ---------------------------------------------
    line_items_path = str(OUTPUTS / "line_items_parquet")

    print("Loading line items from:", line_items_path)

    line_items = spark.read.parquet(line_items_path)

    # Expected columns:
    # user_id, transaction_id, product_id, quantity, unit_price, subtotal

    # ---------------------------------------------
    # 2️⃣ Compute CLV
    # ---------------------------------------------
    clv_df = (
        line_items
        .groupBy("user_id")
        .agg(
            _sum("subtotal").alias("clv"),
            countDistinct("transaction_id").alias("num_orders")
        )
        .orderBy(col("clv").desc())
    )

    # ---------------------------------------------
    # 3️⃣ Save CLV (CSV)
    # ---------------------------------------------
    clv_out = str(OUTPUTS / "clv_csv")

    print("Saving CLV to:", clv_out)

    (
        clv_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(clv_out)
    )

    print("CLV integration completed successfully ✅")

    spark.stop()


if __name__ == "__main__":
    main()
