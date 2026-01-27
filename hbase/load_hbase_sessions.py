# hbase/load_hbase_sessions.py
import os
import json
from pathlib import Path
from decimal import Decimal
import happybase

PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data_raw"

HBASE_HOST = "localhost"
HBASE_PORT = 9090

TABLE_NAME = "user_sessions"

# Tune for 8GB RAM (safe)
BATCH_SIZE = 500   # 500 rows per commit is stable on laptops
PRINT_EVERY = 5000

def json_default(o):
    """Make json.dumps handle types like Decimal."""
    if isinstance(o, Decimal):
        return float(o)  # or: str(o)
    return str(o)

def put_from_session(batch, s: dict):
    user_id = s.get("user_id", "unknown")
    start_time = s.get("start_time", "unknown")
    session_id = s.get("session_id", "unknown")

    # RowKey pattern: user#start#session
    row_key = f"{user_id}#{start_time}#{session_id}".encode("utf-8")

    meta = {
        "session_id": session_id,
        "user_id": user_id,
        "start_time": s.get("start_time"),
        "end_time": s.get("end_time"),
        "duration_seconds": s.get("duration_seconds"),
        "conversion_status": s.get("conversion_status"),
        "referrer": s.get("referrer"),
        "device_profile": s.get("device_profile", {}),
        "geo_data": s.get("geo_data", {})
    }

    page_views = s.get("page_views", [])
    cart_contents = s.get("cart_contents", {})

    batch.put(row_key, {
        b"meta:data": json.dumps(meta, ensure_ascii=False, default=json_default).encode("utf-8"),
        b"pv:page_views": json.dumps(page_views, ensure_ascii=False, default=json_default).encode("utf-8"),
        b"cart:contents": json.dumps(cart_contents, ensure_ascii=False, default=json_default).encode("utf-8"),
    })

def main():
    print("Loader started")
    print("Current file:", __file__)
    print("Working dir:", os.getcwd())
    print("Sessions folder:", DATA_DIR)

    session_files = sorted(DATA_DIR.glob("sessions_*.json"))
    print("Found session files:", len(session_files))
    if not session_files:
        print(" No sessions_*.json found in data_raw")
        return

    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=20000)
    conn.open()
    print(f"Connected to HBase Thrift at {HBASE_HOST}:{HBASE_PORT}")

    tables = [t.decode() if isinstance(t, (bytes, bytearray)) else t for t in conn.tables()]
    if TABLE_NAME not in tables:
        raise RuntimeError(f"Table {TABLE_NAME} not found. Create it in HBase shell first.")

    table = conn.table(TABLE_NAME)
    print("Table exists:", TABLE_NAME)

    total_loaded = 0

    for fp in session_files:
        print("\nLoading:", fp.name)

        # Each sessions_*.json is a JSON array of sessions
        with fp.open("r", encoding="utf-8") as f:
            data = json.load(f)

        with table.batch(batch_size=BATCH_SIZE) as batch:
            for s in data:
                put_from_session(batch, s)
                total_loaded += 1

                if total_loaded % PRINT_EVERY == 0:
                    print(f"Loaded rows: {total_loaded:,}")

    print(f"\n Done. Total rows loaded: {total_loaded:,}")

if __name__ == "__main__":
    main()
