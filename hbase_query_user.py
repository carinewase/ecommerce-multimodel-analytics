import happybase

import json
 
# ============================================================

# HBase Connection

# ============================================================

HBASE_HOST = 'localhost'

HBASE_PORT = 9090

TABLE_NAME = 'sessions'
 
 
def connect_to_hbase():

    try:

        connection = happybase.Connection(

            host=HBASE_HOST,

            port=HBASE_PORT,

            timeout=200000,

            autoconnect=True

        )

        print("Connected to HBase successfully!\n")

        return connection

    except Exception as e:

        print(f"Failed to connect to HBase: {e}")

        return None
 
 
# ============================================================

# QUERY 1: Sessions for a Specific User (NO SCAN)

# ============================================================

def get_user_sessions(table, user_id, limit=10):
    """
    Retrieve all sessions for a specific user.
    """
    print("=" * 60)
    print(f"QUERY 1: Get Sessions for User '{user_id}'")
    print("=" * 60)
    print("Business Question: What is the browsing history of this user?\n")
    # Create row prefix for scanning
    row_prefix = f"{user_id}_".encode()
    sessions = []
    count = 0
    for key, data in table.scan(row_prefix=row_prefix, limit=limit):
        count += 1
        session = {
            'row_key': key.decode(),
            'session_id': data.get(b'session_info:session_id', b'').decode(),
            'start_time': data.get(b'session_info:start_time', b'').decode(),
            'duration': data.get(b'session_info:duration', b'').decode(),
            'conversion_status': data.get(b'session_info:conversion_status', b'').decode(),
            'device_type': data.get(b'device:type', b'').decode(),
            'browser': data.get(b'device:browser', b'').decode(),
            'city': data.get(b'geo:city', b'').decode(),
            'state': data.get(b'geo:state', b'').decode(),
            'page_views': data.get(b'activity:page_views_count', b'').decode()
        }
        sessions.append(session)
    # Display results
    print(f"Found {count} session(s) for user {user_id}:\n")
    print("-" * 90)
    print(f"{'Session ID':<20} {'Start Time':<22} {'Duration':<10} {'Status':<12} {'Device':<10} {'City':<15}")
    print("-" * 90)
    for s in sessions:
        city = s['city'][:14] if s['city'] else ''
        print(f"{s['session_id']:<20} {s['start_time']:<22} {s['duration']:<10} {s['conversion_status']:<12} {s['device_type']:<10} {city:<15}")
    print()
    return sessions
 
# ============================================================

# QUERY 2: Converted Sessions (GUARDED SCAN)

# ============================================================

def get_converted_sessions(table, limit=10):
    """
    Find sessions that resulted in conversions (purchases).
    Note: This requires a full table scan since we're not
    filtering by row key prefix. In production, you might
    create a secondary index or use a different row key design.
    """
    print("=" * 60)
    print("QUERY 2: Get Converted Sessions (Purchases)")
    print("=" * 60)
    print("Business Question: Which sessions resulted in purchases?\n")
    converted = []
    scanned = 0
    for key, data in table.scan(limit=1000):  # Scan more to find converted
        scanned += 1
        status = data.get(b'session_info:conversion_status', b'').decode()
        if status == 'converted':
            session = {
                'row_key': key.decode(),
                'user_id': key.decode().split('_')[0] + '_' + key.decode().split('_')[1],
                'session_id': data.get(b'session_info:session_id', b'').decode(),
                'start_time': data.get(b'session_info:start_time', b'').decode(),
                'duration': data.get(b'session_info:duration', b'').decode(),
                'device_type': data.get(b'device:type', b'').decode(),
                'referrer': data.get(b'session_info:referrer', b'').decode()
            }
            converted.append(session)
            if len(converted) >= limit:
                break
    # Display results
    print(f"Scanned {scanned} rows, found {len(converted)} converted sessions:\n")
    print("-" * 100)
    print(f"{'User ID':<15} {'Session ID':<20} {'Start Time':<22} {'Duration':<10} {'Device':<10} {'Referrer':<15}")
    print("-" * 100)
    for s in converted:
        print(f"{s['user_id']:<15} {s['session_id']:<20} {s['start_time']:<22} {s['duration']:<10} {s['device_type']:<10} {s['referrer']:<15}")
    print()
    return converted
 
 
# ============================================================

# QUERY 3: Device Distribution (BEST-EFFORT)

# ============================================================

def count_by_device(table, sample_size=5000):
    """
    Count sessions by device type.
    Note: For large datasets, this would typically be done with
    a MapReduce job or Spark, not a client-side scan.
    """
    print("=" * 60)
    print("QUERY 3: Sessions by Device Type")
    print("=" * 60)
    print("Business Question: What devices do our users prefer?\n")
    device_counts = {}
    for key, data in table.scan(limit=sample_size, columns=[b'device:type']):
        device = data.get(b'device:type', b'unknown').decode()
        device_counts[device] = device_counts.get(device, 0) + 1
    total = sum(device_counts.values())
    print(f"Sample size: {total} sessions\n")
    print("-" * 40)
    print(f"{'Device Type':<15} {'Count':<10} {'Percentage':<10}")
    print("-" * 40)
    for device, count in sorted(device_counts.items(), key=lambda x: -x[1]):
        pct = (count / total) * 100
        print(f"{device:<15} {count:<10} {pct:.1f}%")
    print()
    return device_counts
 
 
# ============================================================

# MAIN (ABSOLUTELY NO STARTUP SCANS)

# ============================================================

def main():

    print("\n" + "=" * 60)

    print("   HBASE QUERIES - E-COMMERCE SESSION ANALYTICS")

    print("=" * 60 + "\n")
 
    connection = connect_to_hbase()

    if not connection:

        return
 
    table = connection.table(TABLE_NAME)
 
    # --------------------------------------------------------

    # HARD-CODED SAFE USER (YOU CONTROL DATA)

    # --------------------------------------------------------

    user_id = "user_000000"

    print(f"Using user: {user_id}\n")
 
    # --------------------------------------------------------

    # Force region open with row() ONLY

    # --------------------------------------------------------

    _ = table.row(f"{user_id}_sess_1".encode())
 
    # --------------------------------------------------------

    # Run Queries

    # --------------------------------------------------------

    user_sessions = get_user_sessions(table, user_id)

    converted = get_converted_sessions(table)

    device_stats = count_by_device(table)
 
    # --------------------------------------------------------

    # Summary

    # --------------------------------------------------------

    print("=" * 60)

    print("SUMMARY")

    print("=" * 60)

    print(f"User sessions: {len(user_sessions)}")

    print(f"Converted sessions: {len(converted)}")

    print(f"Device types: {device_stats}")
 
    connection.close()

    print("\nAll HBase queries completed successfully!")
 
 
if __name__ == "__main__":

    main()

 