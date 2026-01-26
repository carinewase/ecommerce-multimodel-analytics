from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as f_count, least, greatest

def main():
    spark = (
        SparkSession.builder
        .appName("02_also_bought")
        .getOrCreate()
    )

    PROJECT_DIR = Path(__file__).resolve().parents[1]
    OUT_DIR = PROJECT_DIR / "outputs"

    line_items_path = str(OUT_DIR / "line_items_parquet")

    # Read normalized line items
    li = spark.read.parquet(line_items_path).select(
        "transaction_id", "product_id", "product_name"
    ).dropna(subset=["transaction_id", "product_id"])

    # (Optional) remove duplicates of same product in same transaction
    li = li.dropDuplicates(["transaction_id", "product_id"])

    # Self-join within each transaction to get pairs
    a = li.alias("a")
    b = li.alias("b")

    pairs = (
        a.join(b, on=col("a.transaction_id") == col("b.transaction_id"), how="inner")
         .where(col("a.product_id") < col("b.product_id"))  # avoid self + duplicates
         .select(
            col("a.transaction_id").alias("transaction_id"),
            least(col("a.product_id"), col("b.product_id")).alias("product_a"),
            greatest(col("a.product_id"), col("b.product_id")).alias("product_b"),
            col("a.product_name").alias("product_a_name"),
            col("b.product_name").alias("product_b_name"),
         )
    )

    # Count co-occurrence
    counts = (
        pairs.groupBy("product_a", "product_a_name", "product_b", "product_b_name")
             .agg(f_count("*").alias("co_count"))
             .orderBy(col("co_count").desc())
    )

    top10 = counts.limit(10)

    out_csv_dir = str(OUT_DIR / "also_bought_pairs_top10_csv")
    top10.coalesce(1).write.mode("overwrite").option("header", True).csv(out_csv_dir)

    print(f"\n Saved Spark CSV folder: {out_csv_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
