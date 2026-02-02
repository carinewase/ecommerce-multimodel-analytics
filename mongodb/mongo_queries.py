# -*- coding: utf-8 -*-
"""
Mongo Queries (separate file)
============================

Purpose
-------
- Run analytics queries independently of loader.
- Robust handling of date filtering whether `timestamp` is stored as **string** or **BSON Date**.
- Optional CSV export.

Queries
-------
1) top-products            -> Top selling products (revenue & quantity)
2) user-segmentation       -> Users bucketed by purchase frequency
3) revenue-by-category     -> Monthly revenue per category

Usage Examples
--------------
# Top products overall
python mongo_queries.py top-products --limit 20

# Top products in 2025 (auto-detect date type)
python mongo_queries.py top-products --start 2025-01-01 --end 2025-12-31 --limit 20

# Segmentation with custom boundaries (H2 2025)
python mongo_queries.py user-segmentation --start 2025-07-01 --end 2025-12-31 --boundaries 0 1 2 3 5 8 13 21 34 55 89 144 233 1000

# Revenue by category per month with names and CSV
python mongo_queries.py revenue-by-category --start 2025-01-01 --end 2025-12-31 --with-category-names --to-csv outputs/revenue_2025.csv
"""
from __future__ import annotations
import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pymongo import MongoClient

# Defaults
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'ecommerce'

# ---------------------------------
# Helpers: date-type & match stages
# ---------------------------------

def detect_date_type(db) -> str:
    """Return 'date' if transactions.timestamp is a BSON datetime, else 'string'."""
    doc = db.transactions.find_one({}, {'timestamp': 1})
    ts = doc.get('timestamp') if doc else None
    return 'date' if isinstance(ts, datetime) else 'string'


def build_time_match(start: Optional[str], end: Optional[str], date_type: str):
    """Return a $match stage (or empty dict) for the given time window.
    - If date_type == 'date', parse to datetime.
    - If 'string', compare raw ISO-like strings (YYYY-MM-DD or ISO).
    """
    if not start and not end:
        return {"$match": {}}

    key = 'timestamp'
    cond: Dict[str, Any] = {}
    if date_type == 'date':
        if start:
            cond['$gte'] = _parse_dt(start)
        if end:
            cond['$lte'] = _parse_dt(end)
    else:
        # string compare
        if start:
            cond['$gte'] = start
        if end:
            cond['$lte'] = end

    return {"$match": {key: cond}}


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if len(s) == 10:
            return datetime.strptime(s, '%Y-%m-%d')
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ---------------------------------
# Pipelines
# ---------------------------------

def top_products(db, start: Optional[str], end: Optional[str], limit: int, date_type: str):
    # build match stage based on date type
    match_stage = build_time_match(start, end, date_type)

    pipeline = [
        match_stage,
        {"$unwind": "$items"},
        {"$set": {
            "_qty": {"$ifNull": ["$items.quantity", 0]},
            "_unit_price": {"$ifNull": ["$items.unit_price", {"$ifNull": ["$items.price", 0]}]},
        }},
        {"$set": {
            "_line_total": {"$ifNull": ["$items.line_total", {"$multiply": ["$_qty", "$_unit_price"]}]}
        }},
        {"$group": {
            "_id": "$items.product_id",
            "total_qty": {"$sum": "$_qty"},
            "total_revenue": {"$sum": "$_line_total"},
            "orders": {"$addToSet": "$transaction_id"}
        }},
        {"$set": {"orders": {"$size": "$orders"}}},
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "prod"
        }},
        {"$set": {
            "product_name": {"$first": "$prod.name"},
            "category_id": {"$first": "$prod.category_id"}
        }},
        {"$project": {
            "_id": 0,
            "product_id": "$_id",
            "product_name": 1,
            "category_id": 1,
            "total_qty": 1,
            "total_revenue": 1,
            "orders": 1
        }},
        {"$sort": {"total_revenue": -1, "total_qty": -1}},
        {"$limit": int(limit)}
    ]
    return list(db.transactions.aggregate(pipeline, allowDiskUse=True))


def user_segmentation(db, start: Optional[str], end: Optional[str], boundaries: List[int], date_type: str):
    match_stage = build_time_match(start, end, date_type)

    pipeline = [
        match_stage,
        {"$unwind": "$items"},
        {"$set": {
            "_qty": {"$ifNull": ["$items.quantity", 0]},
            "_unit_price": {"$ifNull": ["$items.unit_price", {"$ifNull": ["$items.price", 0]}]},
        }},
        {"$set": {"_line_total": {"$ifNull": ["$items.line_total", {"$multiply": ["$_qty", "$_unit_price"]}]}}},
        {"$group": {
            "_id": "$transaction_id",
            "user_id": {"$first": "$user_id"},
            "tx_total": {"$sum": "$_line_total"},
            "created_at": {"$first": "$timestamp"}
        }},
        {"$group": {
            "_id": "$user_id",
            "orders": {"$sum": 1},
            "total_spend": {"$sum": "$tx_total"},
            "last_order_at": {"$max": "$created_at"}
        }},
        {"$bucket": {
            "groupBy": "$orders",
            "boundaries": boundaries,
            "default": "other",
            "output": {
                "customers": {"$sum": 1},
                "avg_orders": {"$avg": "$orders"},
                "avg_spend": {"$avg": "$total_spend"},
                "max_last_order_at": {"$max": "$last_order_at"}
            }
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db.transactions.aggregate(pipeline, allowDiskUse=True))


def revenue_by_category(db, start: Optional[str], end: Optional[str], with_cat_names: bool, date_type: str):
    match_stage = build_time_match(start, end, date_type)

    # Build year-month expression depending on date type
    if date_type == 'date':
        ym_expr = {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}
    else:
        # string timestamp assumed ISO-like -> YYYY-MM at start
        ym_expr = {"$substr": ["$timestamp", 0, 7]}

    pipeline = [
        match_stage,
        {"$unwind": "$items"},
        {"$set": {
            "_qty": {"$ifNull": ["$items.quantity", 0]},
            "_unit_price": {"$ifNull": ["$items.unit_price", {"$ifNull": ["$items.price", 0]}]},
        }},
        {"$set": {"_line_total": {"$ifNull": ["$items.line_total", {"$multiply": ["$_qty", "$_unit_price"]}]}}},
        {"$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "prod"
        }},
        {"$set": {
            "category_id": {"$first": "$prod.category_id"},
            "year_month": ym_expr,
            "revenue": {"$ifNull": ["$_line_total", 0]}
        }},
        {"$group": {
            "_id": {"category_id": "$category_id", "ym": "$year_month"},
            "revenue": {"$sum": "$revenue"}
        }},
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "year_month": "$_id.ym",
            "revenue": 1
        }},
        {"$sort": {"category_id": 1, "year_month": 1}}
    ]

    res = list(db.transactions.aggregate(pipeline, allowDiskUse=True))

    if with_cat_names:
        # attach names using a small in-memory map for speed
        cats = {c['category_id']: c.get('name') for c in db.categories.find({}, {'_id':0, 'category_id':1, 'name':1})}
        for r in res:
            r['category_name'] = cats.get(r['category_id'])
    return res


# ---------------------------------
# CSV writer
# ---------------------------------

def write_csv(rows: List[Dict[str, Any]], out_path: str):
    if not rows:
        print('[INFO] No rows to write.')
        return
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # union headers
    headers = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                headers.append(k)
    with p.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f'[OK] Wrote {len(rows)} rows -> {p}')


# ---------------------------------
# CLI
# ---------------------------------

def main():
    parser = argparse.ArgumentParser(description='Mongo Analytics (separate)')
    parser.add_argument('--mongo-uri', default=MONGO_URI)
    parser.add_argument('--db', default=DB_NAME)
    parser.add_argument('--date-type', default='auto', choices=['auto','string','date'], help='How timestamp is stored in Mongo')

    sub = parser.add_subparsers(dest='cmd', required=True)

    p_top = sub.add_parser('top-products', help='Top selling products')
    p_top.add_argument('--start', default=None)
    p_top.add_argument('--end', default=None)
    p_top.add_argument('--limit', type=int, default=20)
    p_top.add_argument('--to-csv', default=None)

    p_seg = sub.add_parser('user-segmentation', help='User segmentation by purchase frequency')
    p_seg.add_argument('--start', default=None)
    p_seg.add_argument('--end', default=None)
    p_seg.add_argument('--boundaries', nargs='*', type=int, default=[0,1,3,5,10,1000])
    p_seg.add_argument('--to-csv', default=None)

    p_rev = sub.add_parser('revenue-by-category', help='Revenue by category per month')
    p_rev.add_argument('--start', default=None)
    p_rev.add_argument('--end', default=None)
    p_rev.add_argument('--with-category-names', action='store_true')
    p_rev.add_argument('--to-csv', default=None)

    args = parser.parse_args()

    db = MongoClient(args.mongo_uri)[args.db]

    # Resolve date-type
    if args.date_type == 'auto':
        dt = detect_date_type(db)
    else:
        dt = args.date_type
    print(f"[INFO] Using date-type: {dt}")

    if args.cmd == 'top-products':
        rows = top_products(db, args.start, args.end, args.limit, dt)
        print('\nTop-selling products:')
        for r in rows:
            print(r)
        if args.to_csv:
            write_csv(rows, args.to_csv)
        return

    if args.cmd == 'user-segmentation':
        rows = user_segmentation(db, args.start, args.end, args.boundaries, dt)
        print('\nUser segmentation:')
        for r in rows:
            print(r)
        if args.to_csv:
            write_csv(rows, args.to_csv)
        return

    if args.cmd == 'revenue-by-category':
        rows = revenue_by_category(db, args.start, args.end, args.with_category_names, dt)
        print('\nRevenue by category per month:')
        for r in rows[:50]:
            print(r)
        if len(rows) > 50:
            print(f"... and {len(rows)-50} more rows")
        if args.to_csv:
            write_csv(rows, args.to_csv)
        return


if __name__ == '__main__':
    main()
