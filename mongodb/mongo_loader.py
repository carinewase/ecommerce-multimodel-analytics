# -*- coding: utf-8 -*-
"""
Mongo Loader (separate file)
===========================

Purpose
-------
- Load JSON array (or JSONL) files into MongoDB collections.
- Create helpful indexes for analytics.
- Keep data_raw at **project root** by default.

Usage Examples
--------------
# Ingest everything from default project-root data_raw and create indexes
python mongo_loader.py ingest --create-indexes

# Ingest with explicit data folder
python mongo_loader.py ingest --data-dir "../data_raw" --create-indexes

# Create indexes only
python mongo_loader.py create-indexes

Notes
-----
- Expects files: users.json, categories.json, products.json, transactions.json
- Supports JSON array files and JSONL (one json per line).
- Sessions are intentionally **not** loaded here (they go to HBase in your project).
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pymongo import MongoClient, ASCENDING, DESCENDING

# ------------------------------
# Defaults (project-root data_raw)
# ------------------------------
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / 'data_raw'
MONGO_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
DB_NAME = os.environ.get('MONGODB_DB', 'ecommerce')

# ------------------------------
# JSON readers
# ------------------------------

def iter_json_records(path: Path):
    """Yield records from a JSON **array** file or **JSONL** file.
    """
    with path.open('r', encoding='utf-8') as f:
        head = f.read(1)
        f.seek(0)
        if head == '[':
            data = json.load(f)
            if isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict):
                        yield obj
            return
        # JSON Lines fallback
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def bulk_insert(coll, docs: Iterable[Dict[str, Any]], batch_size: int = 5000) -> int:
    buf: List[Dict[str, Any]] = []
    inserted = 0
    for d in docs:
        buf.append(d)
        if len(buf) >= batch_size:
            coll.insert_many(buf, ordered=False)
            inserted += len(buf)
            buf = []
    if buf:
        coll.insert_many(buf, ordered=False)
        inserted += len(buf)
    return inserted


def load_collection(db, name: str, file_path: Path, batch_size: int = 5000, drop_first: bool = True) -> int:
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    print(f"\nImporting {name} from {file_path} ...")

    if drop_first:
        db[name].drop()

    inserted = bulk_insert(db[name], iter_json_records(file_path), batch_size=batch_size)
    print(f"{name}: inserted {inserted:,} documents")
    return inserted


def create_indexes(db):
    print("\nCreating indexes ...")
    # users
    db.users.create_index([('user_id', ASCENDING)], unique=True)

    # products
    db.products.create_index([('product_id', ASCENDING)], unique=True)
    db.products.create_index([('category_id', ASCENDING)])

    # categories
    db.categories.create_index([('category_id', ASCENDING)], unique=True)

    # transactions
    db.transactions.create_index([('transaction_id', ASCENDING)], unique=True)
    db.transactions.create_index([('user_id', ASCENDING), ('timestamp', ASCENDING)])
    db.transactions.create_index([('items.product_id', ASCENDING)])
    db.transactions.create_index([('timestamp', DESCENDING)])

    print('Indexes created.')


def run_counts(db):
    print("\nCounts:")
    for c in ('users', 'categories', 'products', 'transactions'):
        print(f"{c:<12}= {db[c].count_documents({}):,}")


def main():
    parser = argparse.ArgumentParser(description='Mongo Loader (separate)')
    parser.add_argument('--mongo-uri', default=MONGO_URI)
    parser.add_argument('--db', default=DB_NAME)

    sub = parser.add_subparsers(dest='cmd', required=True)

    p_ing = sub.add_parser('ingest', help='Load JSON data into MongoDB')
    p_ing.add_argument('--data-dir', default=str(DATA_DIR), help='Folder containing users.json, products.json, etc.')
    p_ing.add_argument('--batch-size', type=int, default=5000)
    p_ing.add_argument('--no-drop', action='store_true', help="Do not drop collections before insert")
    p_ing.add_argument('--create-indexes', action='store_true', help='Create indexes after ingest')

    sub.add_parser('create-indexes', help='Create indexes only')

    args = parser.parse_args()
    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    if args.cmd == 'ingest':
        dd = Path(args.data_dir)
        drop_first = not args.no_drop
        load_collection(db, 'users', dd / 'users.json', batch_size=args.batch_size, drop_first=drop_first)
        load_collection(db, 'categories', dd / 'categories.json', batch_size=args.batch_size, drop_first=drop_first)
        load_collection(db, 'products', dd / 'products.json', batch_size=args.batch_size, drop_first=drop_first)
        load_collection(db, 'transactions', dd / 'transactions.json', batch_size=args.batch_size, drop_first=drop_first)
        if args.create_indexes:
            create_indexes(db)
        run_counts(db)
        return

    if args.cmd == 'create-indexes':
        create_indexes(db)
        run_counts(db)
        return


if __name__ == '__main__':
    main()
