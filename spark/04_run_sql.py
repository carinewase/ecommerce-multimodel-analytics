from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_FILE = PROJECT_DIR / "spark" / "04_spark_sql.sql"

spark = (
    SparkSession.builder
    .appName("04_spark_sql_runner")
    .config("spark.sql.catalogImplementation", "in-memory")  # avoids Hive
    .config("spark.sql.warehouse.dir", "file:/C:/spark_warehouse")
    .getOrCreate()
)

sql_text = SQL_FILE.read_text(encoding="utf-8")

# Split by ; and run each statement
for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
    print("\nRunning SQL:\n", stmt[:250], "...\n")
    df = spark.sql(stmt)
    # If it returns rows, show them
    if df is not None and len(df.columns) > 0:
        df.show(20, truncate=False)

spark.stop()
print("\n Done.")
