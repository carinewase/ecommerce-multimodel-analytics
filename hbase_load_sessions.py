# hbase/load_hbase_sessions_fixed.py
# Fixed version combining working patterns from both scripts
#
# Key fixes:
# 1. Explicit connection.open() instead of autoconnect
# 2. Manual batch creation/renewal (not auto-batching)
# 3. Simpler row key (2-part instead of 3-part)
# 4. Checkpointing for resumability
# 5. Connection retry logic

import json
import re
import time
from pathlib import Path
from decimal import Decimal
from typing import Optional, Iterator
import happybase

# -----------------------
# CONFIG
# -----------------------
HBASE_HOST = "localhost"
HBASE_PORT = 9090
HBASE_TIMEOUT_MS = 120000  # 120 seconds

TABLE_NAME = "sessions"
SESSIONS_GLOB = "sessions_*.json"

BATCH_SIZE = 1000  # Same as working script
PROGRESS_EVERY = 5000

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60
BACKOFF_MULTIPLIER = 2

CHECKPOINT_FILE = "hbase_load_checkpoint.json"


# -----------------------
# Helpers
# -----------------------
def b(x) -> bytes:
    """Convert value to bytes for HBase."""
    if x is None:
        return b""
    return str(x).encode("utf-8")


def json_default(o):
    """Make non-JSON types serializable (Decimal, etc.)."""
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def safe_json(v) -> str:
    """Dump dict/list safely to JSON string."""
    try:
        return json.dumps(v, ensure_ascii=False, default=json_default)
    except Exception:
        return json.dumps(str(v), ensure_ascii=False)


def sanitize_rowkey_part(s: str) -> str:
    """RowKey must be simple/stable; keep safe characters only."""
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z_\-:.T]", "", s)
    return s


def make_rowkey(sess: dict) -> str:
    """
    RowKey: user_id_start_time (2-part, like working script)
    Example: user_000042_2025-03-12T14:37:22
    """
    user_id = sanitize_rowkey_part(sess.get("user_id", ""))
    start_time = sanitize_rowkey_part(sess.get("start_time", sess.get("timestamp", "")))
    return f"{user_id}_{start_time}"


def stream_json_array(path: Path) -> Iterator[dict]:
    """
    Stream a JSON array file without loading entire file into memory.
    """
    decoder = json.JSONDecoder(parse_float=Decimal)
    with path.open("r", encoding="utf-8") as f:
        # read until '['
        while True:
            ch = f.read(1)
            if not ch:
                return
            if ch == "[":
                break

        buf = ""
        while True:
            chunk = f.read(1024 * 64)
            if not chunk:
                break
            buf += chunk

            while True:
                buf = buf.lstrip()
                if not buf:
                    break
                if buf.startswith("]"):
                    return
                if buf.startswith(","):
                    buf = buf[1:]
                    continue

                try:
                    obj, idx = decoder.raw_decode(buf)
                except json.JSONDecodeError:
                    break  # need more data

                yield obj
                buf = buf[idx:]


def prepare_session_data(sess: dict) -> dict:
    """
    Prepare session data for HBase insertion.
    Handles both nested and flat structures.
    """
    # Handle nested structures (like working script expects)
    dev = sess.get("device_profile", sess.get("device", {}))
    if not isinstance(dev, dict):
        dev = {}
    
    geo = sess.get("geo_data", sess.get("geo", {}))
    if not isinstance(geo, dict):
        geo = {}
    
    # Handle page_views count
    page_views = sess.get("page_views", [])
    page_views_count = len(page_views) if isinstance(page_views, list) else sess.get("page_views_count", 0)
    
    data = {
        # session_info
        b"session_info:session_id": b(sess.get("session_id")),
        b"session_info:start_time": b(sess.get("start_time", sess.get("timestamp"))),
        b"session_info:end_time": b(sess.get("end_time")),
        b"session_info:duration": b(sess.get("duration_seconds", sess.get("duration"))),
        b"session_info:conversion_status": b(sess.get("conversion_status")),
        b"session_info:referrer": b(sess.get("referrer")),

        # device
        b"device:type": b(sess.get("device_type", dev.get("type"))),
        b"device:os": b(sess.get("device_os", dev.get("os"))),
        b"device:browser": b(sess.get("browser", dev.get("browser"))),

        # geo
        b"geo:city": b(sess.get("city", geo.get("city"))),
        b"geo:state": b(sess.get("state", geo.get("state"))),
        b"geo:country": b(sess.get("country", geo.get("country"))),
        b"geo:ip_address": b(sess.get("ip_address", geo.get("ip_address"))),

        # activity
        b"activity:viewed_products": b(safe_json(sess.get("viewed_products", []))),
        b"activity:cart_contents": b(safe_json(sess.get("cart_contents", {}))),
        b"activity:page_views_count": b(page_views_count),
    }
    
    return data


class CheckpointManager:
    """Manages progress checkpoints for resumable loading."""
    
    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = checkpoint_path
        self.checkpoint_data = self._load()
    
    def _load(self) -> dict:
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, 'r') as f:
                    data = json.load(f)
                    print(f"📍 Resuming from checkpoint: {data}")
                    return data
            except Exception as e:
                print(f"⚠ Failed to load checkpoint: {e}")
        return {"completed_files": [], "current_file": None, "rows_in_current": 0, "total_rows": 0}
    
    def save(self):
        try:
            with open(self.checkpoint_path, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            print(f"⚠ Failed to save checkpoint: {e}")
    
    def is_file_completed(self, filename: str) -> bool:
        return filename in self.checkpoint_data["completed_files"]
    
    def should_skip_row(self, current_file: str, row_num: int) -> bool:
        if self.checkpoint_data["current_file"] != current_file:
            return False
        return row_num < self.checkpoint_data["rows_in_current"]
    
    def update(self, current_file: str, rows_in_current: int, total_rows: int):
        self.checkpoint_data["current_file"] = current_file
        self.checkpoint_data["rows_in_current"] = rows_in_current
        self.checkpoint_data["total_rows"] = total_rows
    
    def mark_file_completed(self, filename: str):
        if filename not in self.checkpoint_data["completed_files"]:
            self.checkpoint_data["completed_files"].append(filename)
        self.checkpoint_data["current_file"] = None
        self.checkpoint_data["rows_in_current"] = 0
    
    def clear(self):
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()


def connect_to_hbase(retry_count: int = 0):
    """
    Connect to HBase with explicit open() call.
    Uses retry logic on failure.
    """
    try:
        print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT}...")
        
        # Create connection WITHOUT autoconnect (like working script)
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=HBASE_TIMEOUT_MS,
            autoconnect=False  # ✅ Explicit control
        )
        
        # Explicitly open connection (like working script)
        connection.open()
        
        # Verify table exists
        table = connection.table(TABLE_NAME)
        next(table.scan(limit=1), None)
        
        print(f"✓ Connected successfully to table '{TABLE_NAME}'")
        return connection, table
        
    except Exception as e:
        if retry_count < MAX_RETRIES:
            delay = min(INITIAL_RETRY_DELAY * (BACKOFF_MULTIPLIER ** retry_count), MAX_RETRY_DELAY)
            print(f"✗ Connection failed: {e}")
            print(f"  Retrying in {delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})")
            time.sleep(delay)
            return connect_to_hbase(retry_count + 1)
        else:
            raise RuntimeError(f"Failed to connect after {MAX_RETRIES} attempts: {e}")


def main():
    print("=" * 70)
    print("FIXED HBASE SESSION LOADER")
    print("(Using working script's batch pattern)")
    print("=" * 70)
    
    project_dir = Path(__file__).resolve().parents[1]
    sessions_dir = project_dir / "data_raw" / "sessions"
    session_files = sorted(sessions_dir.glob(SESSIONS_GLOB))
    checkpoint_path = Path(__file__).parent / CHECKPOINT_FILE
    
    print(f"\n📂 Sessions folder: {sessions_dir}")
    print(f"📄 Found {len(session_files)} session file(s)")
    
    if not session_files:
        raise FileNotFoundError(f"No files found: {sessions_dir / SESSIONS_GLOB}")
    
    # Initialize checkpoint
    checkpoint = CheckpointManager(checkpoint_path)
    
    # Connect to HBase
    connection, table = connect_to_hbase()
    
    total_rows = checkpoint.checkpoint_data["total_rows"]
    files_processed = 0
    
    try:
        for fp in session_files:
            filename = fp.name
            
            # Skip completed files
            if checkpoint.is_file_completed(filename):
                print(f"\n⏭  Skipping (already done): {filename}")
                files_processed += 1
                continue
            
            print(f"\n📥 Loading: {filename}")
            
            # ✅ KEY FIX: Create batch WITHOUT batch_size parameter
            # This prevents auto-batching and gives us manual control
            batch = table.batch()
            batch_count = 0
            rows_in_file = 0
            skipped_rows = 0
            
            for sess in stream_json_array(fp):
                rows_in_file += 1
                
                # Skip already processed rows
                if checkpoint.should_skip_row(filename, rows_in_file):
                    skipped_rows += 1
                    continue
                
                # Prepare data
                row_key = make_rowkey(sess)
                data = prepare_session_data(sess)
                
                # Put into batch
                batch.put(row_key.encode(), data)
                batch_count += 1
                total_rows += 1
                
                # ✅ KEY FIX: Manually send batch and CREATE NEW batch object
                if batch_count >= BATCH_SIZE:
                    try:
                        batch.send()
                    except Exception as e:
                        print(f"\n✗ Batch send failed: {e}")
                        print("  Reconnecting and retrying...")
                        
                        # Reconnect
                        connection.close()
                        connection, table = connect_to_hbase()
                        
                        # Create fresh batch and continue
                        batch = table.batch()
                        batch_count = 0
                        continue
                    
                    # ✅ Create NEW batch object (like working script)
                    batch = table.batch()
                    batch_count = 0
                    
                    # Save checkpoint periodically
                    if total_rows % (BATCH_SIZE * 5) == 0:
                        checkpoint.update(filename, rows_in_file, total_rows)
                        checkpoint.save()
                
                # Progress logging
                if total_rows % PROGRESS_EVERY == 0:
                    print(f"  Loaded: {total_rows:,} rows")
            
            # ✅ Send remaining batch
            if batch_count > 0:
                try:
                    batch.send()
                except Exception as e:
                    print(f"⚠ Error sending final batch: {e}")
                    checkpoint.save()
                    raise
            
            # Mark file completed
            checkpoint.mark_file_completed(filename)
            checkpoint.update(filename, rows_in_file, total_rows)
            checkpoint.save()
            
            files_processed += 1
            
            if skipped_rows > 0:
                print(f"  ⏭  Skipped {skipped_rows:,} already-processed rows")
            print(f"  ✓ Complete. Rows in file: {rows_in_file:,}")
        
        print("\n" + "=" * 70)
        print(f"✓✓✓ SUCCESS!")
        print(f"Total rows loaded: {total_rows:,}")
        print(f"Files processed: {files_processed}/{len(session_files)}")
        print("=" * 70)
        
        # Clear checkpoint
        checkpoint.clear()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted. Saving checkpoint...")
        checkpoint.save()
        print(f"Progress saved at {total_rows:,} rows")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        checkpoint.save()
        raise
    
    finally:
        try:
            connection.close()
            print("\n🔌 Connection closed")
        except:
            pass


if __name__ == "__main__":
    main()