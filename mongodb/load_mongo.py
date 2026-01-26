import json
from pathlib import Path
from pymongo import MongoClient, ASCENDING

# -------------------------------------------------------------------
# Paths
# mongodb/load_mongo.py  -> project root is parents[1]
# -------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data_raw"

# -------------------------------------------------------------------
# Mongo config
# -------------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def load_json_array(file_path: Path, collection: str, batch_size: int = 5000):
    """
    Loads a JSON array file: [ {...}, {...}, ... ] into MongoDB collection.
    Uses batch insert to stay safe on 8GB RAM.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    print(f"\nImporting {collection} from {file_path.name} ...")

    # Read JSON array (works fine for your sizes: users/products/categories/transactions)
    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # reset collection
    db[collection].drop()

    if not data:
        print(f"{collection}: file empty -> skipped")
        return

    # insert in batches
    total = 0
    for i in range(0, len(data), batch_size):
        chunk = data[i:i + batch_size]
        db[collection].insert_many(chunk)
        total += len(chunk)

    print(f"{collection}: inserted {total:,} documents")


def create_indexes():
    print("\nCreating indexes...")

    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])

    db.categories.create_index([("category_id", ASCENDING)], unique=True)

    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])

    print("Indexes created.")


if __name__ == "__main__":
    # IMPORTANT: sessions stay in HBase, not Mongo (too big)
    load_json_array(DATA_DIR / "users.json", "users")
    load_json_array(DATA_DIR / "categories.json", "categories")
    load_json_array(DATA_DIR / "products.json", "products")
    load_json_array(DATA_DIR / "transactions.json", "transactions")

    create_indexes()

    print("\nCounts:")
    print("users       =", db.users.count_documents({}))
    print("categories  =", db.categories.count_documents({}))
    print("products    =", db.products.count_documents({}))
    print("transactions=", db.transactions.count_documents({}))
