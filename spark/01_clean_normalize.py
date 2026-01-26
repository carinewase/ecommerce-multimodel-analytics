from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp

def main():
    spark = (
        SparkSession.builder
        .appName("01_clean_normalize")
        .getOrCreate()
    )

    PROJECT_DIR = Path(__file__).resolve().parents[1]
    DATA_DIR = PROJECT_DIR / "data_raw"
    OUT_DIR = PROJECT_DIR / "outputs"

    transactions_path = str(DATA_DIR / "transactions.json")
    products_path     = str(DATA_DIR / "products.json")

    # 1) Read transactions
    tx = spark.read.json(transactions_path)

    # If your timestamp is already ISO string, this works.
    # If it's already a timestamp, Spark will keep it.
    tx = tx.withColumn("timestamp", to_timestamp(col("timestamp")))

    # 2) Explode items -> line items
    # Your items schema: product_id, quantity, subtotal, unit_price
    line_items = (
        tx
        .select(
            col("transaction_id"),
            col("user_id"),
            col("timestamp"),
            explode(col("items")).alias("item")
        )
        .select(
            col("transaction_id"),
            col("user_id"),
            col("timestamp"),
            col("item.product_id").alias("product_id"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unit_price").cast("double").alias("unit_price"),
            col("item.subtotal").cast("double").alias("subtotal")
        )
    )

    # 3) Join product info (for category + name)
    products = (
        spark.read.json(products_path)
        .select(
            col("product_id").alias("p_product_id"),
            col("name").alias("product_name"),
            col("category_id")
        )
    )

    enriched = (
        line_items
        .join(products, line_items.product_id == products.p_product_id, "left")
        .drop("p_product_id")
    )

    # 4) Save outputs (Parquet)
    out_line_items = str(OUT_DIR / "line_items_parquet")
    enriched.write.mode("overwrite").parquet(out_line_items)

    print(f"\n Saved Parquet: {out_line_items}")

    spark.stop()

if __name__ == "__main__":
    main()
