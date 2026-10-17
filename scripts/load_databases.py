"""
Load the air-quality dataset into MySQL and/or MongoDB in batches.

Run from the project folder:
    python scripts/load_databases.py --target mysql
    python scripts/load_databases.py --target mongo
    python scripts/load_databases.py --target both

Safety:
- Refuses to load if a target table/collection already contains records.
- Does not delete or modify the source ZIP.
- Normalizes timestamps to UTC. MySQL DATETIME stores the UTC clock value
  without timezone metadata; MongoDB stores the corresponding UTC BSON Date.
- Keeps all source measurements, including suspicious values, unchanged.
- Loads records in batches and checks the loaded row count.

Install dependencies first:
    python -m pip install mysql-connector-python pymongo
"""
from pathlib import Path
import argparse
import csv
import gzip
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from getpass import getpass

try:
    import mysql.connector
    from pymongo import MongoClient
except ImportError as exc:
    print("Missing dependency:", exc)
    print("Install with: python -m pip install mysql-connector-python pymongo")
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]
ZIP_PATH = ROOT / "data_raw" / "data_gang_gang.zip"
EXPECTED_ROWS = 1_050_603
BATCH_SIZE = 2_000
EXPECTED_COLUMNS = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value"
]

def parse_timestamp_utc(value):
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        # Preserve a naive source timestamp as UTC only if the source lacks an offset.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def iter_records(zip_path):
    """Yield normalized records from each .csv.gz member in the ZIP archive."""
    total = 0
    with zipfile.ZipFile(zip_path, "r") as archive:
        members = sorted(
            name for name in archive.namelist()
            if name.lower().endswith(".csv.gz")
        )
        if not members:
            raise RuntimeError("No .csv.gz files found inside the ZIP archive.")

        for member in members:
            with archive.open(member, "r") as zipped_member:
                with gzip.GzipFile(fileobj=zipped_member, mode="rb") as gz:
                    text = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
                    reader = csv.DictReader(text)
                    if reader.fieldnames is None:
                        continue
                    headers = [h.strip() for h in reader.fieldnames]
                    if headers != EXPECTED_COLUMNS:
                        raise ValueError(
                            f"Unexpected columns in {member}: {headers}"
                        )

                    for raw in reader:
                        record = {
                            "location_id": int(raw["location_id"]),
                            "sensors_id": int(raw["sensors_id"]),
                            "location": raw["location"].strip(),
                            "datetime": parse_timestamp_utc(raw["datetime"]),
                            "lat": float(raw["lat"]),
                            "lon": float(raw["lon"]),
                            "parameter": raw["parameter"].strip(),
                            "units": raw["units"].strip(),
                            "value": float(raw["value"]),
                        }
                        total += 1
                        yield record

    if total != EXPECTED_ROWS:
        raise RuntimeError(
            f"Source row count mismatch: expected {EXPECTED_ROWS:,}, got {total:,}."
        )

def connect_mysql():
    host = input("MySQL host [localhost]: ").strip() or "localhost"
    port_text = input("MySQL port [3306]: ").strip() or "3306"
    user = input("MySQL username [root]: ").strip() or "root"
    password = getpass("MySQL password (input hidden): ")
    return mysql.connector.connect(
        host=host,
        port=int(port_text),
        user=user,
        password=password,
        database="tipping_point",
        autocommit=False,
    )

def load_mysql():
    print("\nConnecting to MySQL...")
    conn = connect_mysql()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM air_quality")
        existing = cursor.fetchone()[0]
        if existing != 0:
            raise RuntimeError(
                f"MySQL air_quality already contains {existing:,} rows. "
                "For safety, this script will not append or overwrite data."
            )

        sql = """
            INSERT INTO air_quality
            (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = []
        count = 0
        start = time.time()

        for record in iter_records(ZIP_PATH):
            dt_utc_naive = record["datetime"].replace(tzinfo=None)
            batch.append((
                record["location_id"], record["sensors_id"], record["location"],
                dt_utc_naive, record["lat"], record["lon"], record["parameter"],
                record["units"], record["value"]
            ))
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                batch.clear()
                if count % 100_000 == 0:
                    print(f"MySQL inserted {count:,} rows...")
        if batch:
            cursor.executemany(sql, batch)
            conn.commit()
            count += len(batch)

        cursor.execute("SELECT COUNT(*) FROM air_quality")
        final_count = cursor.fetchone()[0]
        if final_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"MySQL verification failed: expected {EXPECTED_ROWS:,}, got {final_count:,}."
            )
        print(f"MySQL load successful: {final_count:,} rows in {time.time()-start:.1f} seconds.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def load_mongo():
    print("\nConnecting to MongoDB...")
    uri = input("MongoDB URI [mongodb://localhost:27017]: ").strip() or "mongodb://localhost:27017"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        collection = client["tipping_point"]["air_quality"]
        existing = collection.count_documents({})
        if existing != 0:
            raise RuntimeError(
                f"MongoDB air_quality already contains {existing:,} documents. "
                "For safety, this script will not append or overwrite data."
            )

        batch = []
        count = 0
        start = time.time()
        for record in iter_records(ZIP_PATH):
            # PyMongo handles timezone-aware datetimes as UTC BSON Date values.
            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch, ordered=True)
                count += len(batch)
                batch.clear()
                if count % 100_000 == 0:
                    print(f"MongoDB inserted {count:,} documents...")
        if batch:
            collection.insert_many(batch, ordered=True)
            count += len(batch)

        final_count = collection.count_documents({})
        if final_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"MongoDB verification failed: expected {EXPECTED_ROWS:,}, got {final_count:,}."
            )
        print(f"MongoDB load successful: {final_count:,} documents in {time.time()-start:.1f} seconds.")
    finally:
        client.close()

def main():
    parser = argparse.ArgumentParser(description="Load air-quality data into MySQL/MongoDB.")
    parser.add_argument(
        "--target", choices=("mysql", "mongo", "both"), required=True,
        help="Which database to load. Targets must be empty."
    )
    args = parser.parse_args()

    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"Dataset ZIP not found: {ZIP_PATH}")

    print(f"Source: {ZIP_PATH}")
    print(f"Expected rows: {EXPECTED_ROWS:,}")
    print(f"Batch size: {BATCH_SIZE:,}")
    print("Policy: preserve measurements; normalize timestamps to UTC.")
    print("Safety check: target table/collection must be empty.")

    if args.target in ("mysql", "both"):
        load_mysql()
    if args.target in ("mongo", "both"):
        load_mongo()

    print("\nLoad operation completed. Verify both record counts before benchmarking.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user. Check target row counts before rerunning; partial data may remain.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nERROR: {exc}")
        print("Check the error and target row count before rerunning.")
        sys.exit(1)
