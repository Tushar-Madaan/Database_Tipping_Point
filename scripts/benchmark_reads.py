
"""
Baseline read-query benchmark: MySQL vs MongoDB.

Run from the project folder:
    python scripts/benchmark_reads.py

What it does:
- Connects once to each database.
- Runs equivalent read workloads against both databases.
- Performs warm-up runs, then repeated timed trials.
- Checks result equivalence before reporting timings.
- Saves raw timings and summary statistics to results/read_benchmark_results.csv
- Does not create indexes, change data, or modify records.

Important:
- This is a baseline benchmark using the databases' current indexes.
- MySQL's primary-key index and MongoDB's default _id index exist, but the
  fields queried below are not indexed by this script.
- Run the benchmark when no large imports or other heavy workloads are running.
"""
from pathlib import Path
from getpass import getpass
from datetime import datetime, timezone
import csv
import statistics
import time
import sys

try:
    import mysql.connector
    from pymongo import MongoClient
except ImportError as exc:
    print("Missing dependency:", exc)
    print("Install with: python -m pip install mysql-connector-python pymongo")
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"
OUT_CSV = RESULTS_DIR / "read_benchmark_results.csv"

TRIALS = 7
WARMUPS = 2
POLLUTANT = "pm25"
UNITS = "µg/m³"
SENSOR_ID = 5588
START_UTC = datetime(2020, 3, 1, tzinfo=timezone.utc)
END_UTC = datetime(2020, 4, 1, tzinfo=timezone.utc)

def connect_mysql():
    host = input("MySQL host [localhost]: ").strip() or "localhost"
    port = int(input("MySQL port [3306]: ").strip() or "3306")
    user = input("MySQL username [root]: ").strip() or "root"
    password = getpass("MySQL password (input hidden): ")
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        database="tipping_point", autocommit=True
    )

def connect_mongo():
    uri = input("MongoDB URI [mongodb://localhost:27017]: ").strip() or "mongodb://localhost:27017"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client, client["tipping_point"]["air_quality"]

def execute_mysql(cursor, workload):
    if workload == "count_pm25":
        cursor.execute(
            "SELECT COUNT(*) FROM air_quality WHERE parameter=%s AND units=%s",
            (POLLUTANT, UNITS)
        )
        return cursor.fetchall()
    if workload == "count_sensor":
        cursor.execute(
            "SELECT COUNT(*) FROM air_quality WHERE sensors_id=%s",
            (SENSOR_ID,)
        )
        return cursor.fetchall()
    if workload == "time_range":
        cursor.execute(
            """SELECT COUNT(*), AVG(value)
               FROM air_quality
               WHERE parameter=%s AND units=%s
                 AND datetime >= %s AND datetime < %s""",
            (POLLUTANT, UNITS, START_UTC.replace(tzinfo=None), END_UTC.replace(tzinfo=None))
        )
        return cursor.fetchall()
    if workload == "group_by_pollutant_unit":
        cursor.execute(
            """SELECT parameter, units, COUNT(*), AVG(value)
               FROM air_quality
               GROUP BY parameter, units
               ORDER BY parameter, units"""
        )
        return cursor.fetchall()
    raise ValueError(f"Unknown workload: {workload}")

def execute_mongo(collection, workload):
    if workload == "count_pm25":
        return [{"count": collection.count_documents({
            "parameter": POLLUTANT, "units": UNITS
        })}]
    if workload == "count_sensor":
        return [{"count": collection.count_documents({"sensors_id": SENSOR_ID})}]
    if workload == "time_range":
        pipeline = [
            {"$match": {
                "parameter": POLLUTANT,
                "units": UNITS,
                "datetime": {"$gte": START_UTC, "$lt": END_UTC}
            }},
            {"$group": {
                "_id": None,
                "count": {"$sum": 1},
                "average": {"$avg": "$value"}
            }}
        ]
        result = list(collection.aggregate(pipeline))
        if not result:
            return [{"count": 0, "average": None}]
        return [{"count": result[0]["count"], "average": result[0]["average"]}]
    if workload == "group_by_pollutant_unit":
        pipeline = [
            {"$group": {
                "_id": {"parameter": "$parameter", "units": "$units"},
                "count": {"$sum": 1},
                "average": {"$avg": "$value"}
            }},
            {"$sort": {"_id.parameter": 1, "_id.units": 1}}
        ]
        return list(collection.aggregate(pipeline))
    raise ValueError(f"Unknown workload: {workload}")

def normalize_mysql(workload, result):
    # Convert MySQL tuples into stable, comparable Python structures.
    if workload in ("count_pm25", "count_sensor"):
        return int(result[0][0])
    if workload == "time_range":
        count, avg = result[0]
        return (int(count), None if avg is None else float(avg))
    if workload == "group_by_pollutant_unit":
        return [
            (str(row[0]), str(row[1]), int(row[2]), None if row[3] is None else float(row[3]))
            for row in result
        ]
    return result

def normalize_mongo(workload, result):
    if workload in ("count_pm25", "count_sensor"):
        return int(result[0]["count"])
    if workload == "time_range":
        row = result[0]
        return (int(row["count"]), None if row.get("average") is None else float(row["average"]))
    if workload == "group_by_pollutant_unit":
        return [
            (
                str(row["_id"]["parameter"]),
                str(row["_id"]["units"]),
                int(row["count"]),
                None if row.get("average") is None else float(row["average"])
            )
            for row in result
        ]
    return result

def equivalent(a, b, tolerance=1e-7):
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) <= tolerance * max(1.0, abs(float(a)), abs(float(b)))
        except (TypeError, ValueError):
            return a == b
    if isinstance(a, tuple) and isinstance(b, tuple):
        return len(a) == len(b) and all(equivalent(x, y, tolerance) for x, y in zip(a, b))
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(equivalent(x, y, tolerance) for x, y in zip(a, b))
    return a == b

def time_one(fn):
    start = time.perf_counter()
    result = fn()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, result

def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print("Baseline read benchmark — no indexes will be created or changed.")
    print(f"Warm-up runs per database/workload: {WARMUPS}")
    print(f"Timed trials per database/workload: {TRIALS}")
    print("Keep other heavy workloads stopped while this runs.\n")

    mysql_conn = connect_mysql()
    mongo_client = None
    mysql_cursor = mysql_conn.cursor()
    try:
        mongo_client, mongo_collection = connect_mongo()
        mysql_cursor.execute("SELECT COUNT(*) FROM air_quality")
        mysql_rows = int(mysql_cursor.fetchone()[0])
        mongo_rows = mongo_collection.count_documents({})
        print(f"\nMySQL rows: {mysql_rows:,}")
        print(f"MongoDB documents: {mongo_rows:,}")
        if mysql_rows != mongo_rows:
            raise RuntimeError("Database record counts differ. Stop and validate before benchmarking.")

        workloads = [
            ("count_pm25", "Count PM2.5 records for µg/m³"),
            ("count_sensor", f"Count records for sensor_id={SENSOR_ID}"),
            ("time_range", "Count and average PM2.5 in March 2020 UTC"),
            ("group_by_pollutant_unit", "Group all records by pollutant and unit"),
        ]
        raw_rows = []
        for workload, description in workloads:
            print(f"\nWorkload: {description}")
            # Check output equivalence before collecting benchmark numbers.
            mysql_result = normalize_mysql(workload, execute_mysql(mysql_cursor, workload))
            mongo_result = normalize_mongo(workload, execute_mongo(mongo_collection, workload))
            if not equivalent(mysql_result, mongo_result):
                raise RuntimeError(
                    f"Result mismatch for {workload}.\n"
                    f"MySQL: {mysql_result}\nMongoDB: {mongo_result}\n"
                    "Stop benchmarking until the difference is understood."
                )
            print("  Result equivalence: PASS")

            for _ in range(WARMUPS):
                execute_mysql(mysql_cursor, workload)
                execute_mongo(mongo_collection, workload)

            for trial in range(1, TRIALS + 1):
                # Alternate execution order to reduce systematic order bias.
                if trial % 2 == 1:
                    mysql_ms, _ = time_one(lambda: execute_mysql(mysql_cursor, workload))
                    mongo_ms, _ = time_one(lambda: execute_mongo(mongo_collection, workload))
                else:
                    mongo_ms, _ = time_one(lambda: execute_mongo(mongo_collection, workload))
                    mysql_ms, _ = time_one(lambda: execute_mysql(mysql_cursor, workload))

                raw_rows.append({
                    "workload": workload,
                    "description": description,
                    "trial": trial,
                    "mysql_ms": round(mysql_ms, 4),
                    "mongodb_ms": round(mongo_ms, 4),
                    "results_equivalent": "yes",
                    "mysql_rows": mysql_rows,
                    "mongodb_documents": mongo_rows,
                    "indexes_changed": "no"
                })
                print(f"  Trial {trial}: MySQL {mysql_ms:.2f} ms | MongoDB {mongo_ms:.2f} ms")

        fieldnames = [
            "workload", "description", "trial", "mysql_ms", "mongodb_ms",
            "results_equivalent", "mysql_rows", "mongodb_documents", "indexes_changed"
        ]
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(raw_rows)

        print("\n========== BENCHMARK SUMMARY (MEDIAN) ==========")
        for workload, description in workloads:
            rows = [r for r in raw_rows if r["workload"] == workload]
            mysql_median = statistics.median(r["mysql_ms"] for r in rows)
            mongo_median = statistics.median(r["mongodb_ms"] for r in rows)
            if mongo_median > 0:
                faster = "MySQL" if mysql_median < mongo_median else "MongoDB"
                ratio = max(mysql_median, mongo_median) / min(mysql_median, mongo_median)
                print(f"{workload}: MySQL median={mysql_median:.2f} ms; "
                      f"MongoDB median={mongo_median:.2f} ms; {faster} faster by ~{ratio:.2f}x")
            else:
                print(f"{workload}: MySQL median={mysql_median:.2f} ms; MongoDB median={mongo_median:.2f} ms")
        print(f"\nRaw benchmark results saved to: {OUT_CSV}")
        print("These are initial local baseline measurements, not yet a tipping-point conclusion.")
    finally:
        mysql_cursor.close()
        mysql_conn.close()
        if mongo_client is not None:
            mongo_client.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBenchmark interrupted. No records were modified.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nERROR: {exc}")
        print("No records were modified. Review the error before rerunning.")
        sys.exit(1)
