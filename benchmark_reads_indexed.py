"""
Indexed Read Benchmark: MySQL vs MongoDB

Project: Database Tipping Point

Run:
    python scripts/benchmark_reads_indexed.py

New output files:
    results/read_benchmark_indexed.csv
    results/index_creation_times.csv

Features:
- Inspects existing indexes before creating any.
- Creates missing indexes for comparable workloads.
- Records index creation time separately.
- Runs 2 warm-ups and 7 timed trials per workload.
- Checks result equivalence before comparing timings.
- Never overwrites existing output files.
- Does not modify or delete measurement records.
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
except ImportError:
    print(
        "Install required packages with:\n"
        "python -m pip install mysql-connector-python pymongo"
    )
    sys.exit(1)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"

BENCHMARK_FILE = RESULTS_DIR / "read_benchmark_indexed.csv"
INDEX_FILE = RESULTS_DIR / "index_creation_times.csv"

TRIALS = 7
WARMUPS = 2

POLLUTANT = "pm25"
UNITS = "µg/m³"
SENSOR_ID = 5588

START_UTC = datetime(2020, 3, 1, tzinfo=timezone.utc)
END_UTC = datetime(2020, 4, 1, tzinfo=timezone.utc)


# --------------------------------------------------
# OUTPUT SAFETY
# --------------------------------------------------

def check_output_files():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    existing = [
        str(path)
        for path in (BENCHMARK_FILE, INDEX_FILE)
        if path.exists()
    ]

    if existing:
        raise FileExistsError(
            "To protect existing experiment results, this script "
            "will not overwrite:\n"
            + "\n".join(existing)
            + "\nRename the new experiment's output files before rerunning."
        )


# --------------------------------------------------
# DATABASE CONNECTIONS
# --------------------------------------------------

def connect_mysql():
    host = input("MySQL host [localhost]: ").strip() or "localhost"
    port = int(input("MySQL port [3306]: ").strip() or "3306")
    user = input("MySQL username [root]: ").strip() or "root"
    password = getpass("MySQL password (hidden): ")

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database="tipping_point",
        autocommit=True
    )


def connect_mongo():
    uri = (
        input("MongoDB URI [mongodb://localhost:27017]: ").strip()
        or "mongodb://localhost:27017"
    )

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")

    return client, client["tipping_point"]["air_quality"]


# --------------------------------------------------
# INDEX INSPECTION AND CREATION
# --------------------------------------------------

# These two indexes support our tested filtering workloads.
# The composite index supports:
# parameter + units filters and parameter + units + datetime filters.
INDEXES = [
    {
        "name": "idx_parameter_units_datetime",
        "mysql_columns": ("parameter", "units", "datetime"),
        "mongo_keys": [
            ("parameter", 1),
            ("units", 1),
            ("datetime", 1)
        ]
    },
    {
        "name": "idx_sensors_id",
        "mysql_columns": ("sensors_id",),
        "mongo_keys": [("sensors_id", 1)]
    }
]


def get_mysql_indexes(cursor):
    cursor.execute("SHOW INDEX FROM air_quality")

    indexes = {}

    for row in cursor.fetchall():
        # MySQL SHOW INDEX columns:
        # Table, Non_unique, Key_name, Seq_in_index, Column_name, ...
        key_name = row[2]
        sequence = int(row[3])
        column_name = row[4]

        indexes.setdefault(key_name, []).append(
            (sequence, column_name)
        )

    normalized = []

    for name, columns in indexes.items():
        ordered_columns = tuple(
            column for _, column in sorted(columns)
        )
        normalized.append((name, ordered_columns))

    return normalized


def has_mysql_index(existing, desired_columns):
    for _, columns in existing:
        # A composite index supports searches on its leading columns.
        if columns[:len(desired_columns)] == desired_columns:
            return True
    return False


def prepare_mysql_indexes(cursor):
    records = []

    for index in INDEXES:
        name = index["name"]
        columns = index["mysql_columns"]

        existing = get_mysql_indexes(cursor)
        start = time.perf_counter()

        if has_mysql_index(existing, columns):
            elapsed = (time.perf_counter() - start) * 1000

            records.append({
                "database": "MySQL",
                "index_name": name,
                "columns": ", ".join(columns),
                "status": "EXISTING_COMPATIBLE_INDEX",
                "creation_time_ms": round(elapsed, 4)
            })

            print(f"MySQL: compatible index already exists for {columns}")
            continue

        column_sql = ", ".join(f"`{col}`" for col in columns)

        sql = (
            f"CREATE INDEX `{name}` "
            f"ON `air_quality` ({column_sql})"
        )

        cursor.execute(sql)

        elapsed = (time.perf_counter() - start) * 1000

        records.append({
            "database": "MySQL",
            "index_name": name,
            "columns": ", ".join(columns),
            "status": "CREATED",
            "creation_time_ms": round(elapsed, 4)
        })

        print(
            f"MySQL: created {name} "
            f"in {elapsed / 1000:.2f} seconds"
        )

    return records


def prepare_mongo_indexes(collection):
    records = []

    for index in INDEXES:
        name = index["name"]
        keys = index["mongo_keys"]

        existing = list(collection.list_indexes())

        compatible = False

        desired_pairs = list(keys)

        for current in existing:
            current_pairs = list(current["key"].items())

            if current_pairs[:len(desired_pairs)] == desired_pairs:
                compatible = True
                break

        start = time.perf_counter()

        if compatible:
            elapsed = (time.perf_counter() - start) * 1000

            records.append({
                "database": "MongoDB",
                "index_name": name,
                "columns": ", ".join(
                    f"{field}:{direction}" for field, direction in keys
                ),
                "status": "EXISTING_COMPATIBLE_INDEX",
                "creation_time_ms": round(elapsed, 4)
            })

            print(f"MongoDB: compatible index already exists for {keys}")
            continue

        collection.create_index(keys, name=name)

        elapsed = (time.perf_counter() - start) * 1000

        records.append({
            "database": "MongoDB",
            "index_name": name,
            "columns": ", ".join(
                f"{field}:{direction}" for field, direction in keys
            ),
            "status": "CREATED",
            "creation_time_ms": round(elapsed, 4)
        })

        print(
            f"MongoDB: created {name} "
            f"in {elapsed / 1000:.2f} seconds"
        )

    return records


# --------------------------------------------------
# QUERY WORKLOADS
# --------------------------------------------------

def execute_mysql(cursor, workload):
    if workload == "count_pm25":
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM air_quality
            WHERE parameter = %s AND units = %s
            """,
            (POLLUTANT, UNITS)
        )
        return cursor.fetchall()

    if workload == "count_sensor":
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM air_quality
            WHERE sensors_id = %s
            """,
            (SENSOR_ID,)
        )
        return cursor.fetchall()

    if workload == "time_range":
        cursor.execute(
            """
            SELECT COUNT(*), AVG(value)
            FROM air_quality
            WHERE parameter = %s
              AND units = %s
              AND datetime >= %s
              AND datetime < %s
            """,
            (
                POLLUTANT,
                UNITS,
                START_UTC.replace(tzinfo=None),
                END_UTC.replace(tzinfo=None)
            )
        )
        return cursor.fetchall()

    if workload == "group_by_pollutant_unit":
        cursor.execute(
            """
            SELECT parameter, units, COUNT(*), AVG(value)
            FROM air_quality
            GROUP BY parameter, units
            ORDER BY parameter, units
            """
        )
        return cursor.fetchall()

    raise ValueError(f"Unknown workload: {workload}")


def execute_mongo(collection, workload):
    if workload == "count_pm25":
        count = collection.count_documents({
            "parameter": POLLUTANT,
            "units": UNITS
        })
        return [{"count": count}]

    if workload == "count_sensor":
        count = collection.count_documents({
            "sensors_id": SENSOR_ID
        })
        return [{"count": count}]

    if workload == "time_range":
        pipeline = [
            {
                "$match": {
                    "parameter": POLLUTANT,
                    "units": UNITS,
                    "datetime": {
                        "$gte": START_UTC,
                        "$lt": END_UTC
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1},
                    "average": {"$avg": "$value"}
                }
            }
        ]

        result = list(collection.aggregate(pipeline))

        if not result:
            return [{"count": 0, "average": None}]

        return [{
            "count": result[0]["count"],
            "average": result[0]["average"]
        }]

    if workload == "group_by_pollutant_unit":
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "parameter": "$parameter",
                        "units": "$units"
                    },
                    "count": {"$sum": 1},
                    "average": {"$avg": "$value"}
                }
            },
            {
                "$sort": {
                    "_id.parameter": 1,
                    "_id.units": 1
                }
            }
        ]

        return list(collection.aggregate(pipeline))

    raise ValueError(f"Unknown workload: {workload}")


# --------------------------------------------------
# RESULT NORMALIZATION AND VALIDATION
# --------------------------------------------------

def normalize_mysql(workload, result):
    if workload in ("count_pm25", "count_sensor"):
        return int(result[0][0])

    if workload == "time_range":
        count, avg = result[0]
        return (
            int(count),
            None if avg is None else float(avg)
        )

    if workload == "group_by_pollutant_unit":
        return [
            (
                str(row[0]),
                str(row[1]),
                int(row[2]),
                None if row[3] is None else float(row[3])
            )
            for row in result
        ]

    return result


def normalize_mongo(workload, result):
    if workload in ("count_pm25", "count_sensor"):
        return int(result[0]["count"])

    if workload == "time_range":
        row = result[0]
        return (
            int(row["count"]),
            None if row.get("average") is None
            else float(row["average"])
        )

    if workload == "group_by_pollutant_unit":
        return [
            (
                str(row["_id"]["parameter"]),
                str(row["_id"]["units"]),
                int(row["count"]),
                None if row.get("average") is None
                else float(row["average"])
            )
            for row in result
        ]

    return result


def equivalent(a, b):
    if isinstance(a, (float, int)) and isinstance(b, (float, int)):
        tolerance = 1e-7 * max(1.0, abs(float(a)), abs(float(b)))
        return abs(float(a) - float(b)) <= tolerance

    if isinstance(a, tuple) and isinstance(b, tuple):
        return (
            len(a) == len(b)
            and all(equivalent(x, y) for x, y in zip(a, b))
        )

    if isinstance(a, list) and isinstance(b, list):
        return (
            len(a) == len(b)
            and all(equivalent(x, y) for x, y in zip(a, b))
        )

    return a == b


def timed_query(function):
    start = time.perf_counter()
    result = function()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, result


# --------------------------------------------------
# SAVE RESULTS
# --------------------------------------------------

def save_csv(path, rows, fields):
    # Exclusive creation prevents accidental overwrites.
    with path.open("x", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


# --------------------------------------------------
# MAIN BENCHMARK
# --------------------------------------------------

def main():
    check_output_files()

    print("Indexed read benchmark")
    print("Existing data will not be modified.")
    print("New indexes may be created if compatible ones do not exist.")
    print(f"Warm-ups: {WARMUPS}")
    print(f"Timed trials: {TRIALS}\n")

    mysql_conn = connect_mysql()
    mongo_client = None
    cursor = mysql_conn.cursor()

    try:
        mongo_client, collection = connect_mongo()

        cursor.execute("SELECT COUNT(*) FROM air_quality")
        mysql_rows = int(cursor.fetchone()[0])
        mongo_rows = collection.count_documents({})

        print(f"\nMySQL rows: {mysql_rows:,}")
        print(f"MongoDB documents: {mongo_rows:,}")

        if mysql_rows != mongo_rows:
            raise RuntimeError(
                "Record counts differ. Stop before benchmarking."
            )

        # Build/inspect indexes and record their times separately.
        print("\nPreparing MySQL indexes...")
        index_rows = prepare_mysql_indexes(cursor)

        print("\nPreparing MongoDB indexes...")
        index_rows.extend(prepare_mongo_indexes(collection))

        save_csv(
            INDEX_FILE,
            index_rows,
            [
                "database",
                "index_name",
                "columns",
                "status",
                "creation_time_ms"
            ]
        )

        workloads = [
            ("count_pm25", "Count PM2.5 records"),
            ("count_sensor", "Count records for sensor 5588"),
            ("time_range", "March 2020 PM2.5 time range"),
            ("group_by_pollutant_unit", "Group by pollutant and unit")
        ]

        benchmark_rows = []

        for workload, description in workloads:
            print(f"\nWorkload: {description}")

            mysql_fn = lambda: execute_mysql(cursor, workload)
            mongo_fn = lambda: execute_mongo(collection, workload)

            mysql_result = normalize_mysql(workload, mysql_fn())
            mongo_result = normalize_mongo(workload, mongo_fn())

            if not equivalent(mysql_result, mongo_result):
                raise RuntimeError(
                    f"Result mismatch for {workload}.\n"
                    f"MySQL: {mysql_result}\n"
                    f"MongoDB: {mongo_result}"
                )

            print("  Result equivalence: PASS")

            for _ in range(WARMUPS):
                mysql_fn()
                mongo_fn()

            for trial in range(1, TRIALS + 1):
                # Alternate which database runs first.
                if trial % 2:
                    mysql_ms, _ = timed_query(mysql_fn)
                    mongo_ms, _ = timed_query(mongo_fn)
                else:
                    mongo_ms, _ = timed_query(mongo_fn)
                    mysql_ms, _ = timed_query(mysql_fn)

                benchmark_rows.append({
                    "workload": workload,
                    "description": description,
                    "trial": trial,
                    "mysql_ms": round(mysql_ms, 4),
                    "mongodb_ms": round(mongo_ms, 4),
                    "results_equivalent": "yes",
                    "mysql_rows": mysql_rows,
                    "mongodb_documents": mongo_rows,
                    "index_configuration": "indexed"
                })

                print(
                    f"  Trial {trial}: "
                    f"MySQL {mysql_ms:.2f} ms | "
                    f"MongoDB {mongo_ms:.2f} ms"
                )

        save_csv(
            BENCHMARK_FILE,
            benchmark_rows,
            [
                "workload",
                "description",
                "trial",
                "mysql_ms",
                "mongodb_ms",
                "results_equivalent",
                "mysql_rows",
                "mongodb_documents",
                "index_configuration"
            ]
        )

        print("\n========== INDEXED BENCHMARK SUMMARY ==========")

        for workload, description in workloads:
            rows = [
                row for row in benchmark_rows
                if row["workload"] == workload
            ]

            mysql_median = statistics.median(
                row["mysql_ms"] for row in rows
            )
            mongo_median = statistics.median(
                row["mongodb_ms"] for row in rows
            )

            print(
                f"{workload}: "
                f"MySQL median={mysql_median:.2f} ms; "
                f"MongoDB median={mongo_median:.2f} ms"
            )

        print("\nNew files created:")
        print(f"  {BENCHMARK_FILE}")
        print(f"  {INDEX_FILE}")
        print("Existing benchmark files were not overwritten.")

    finally:
        cursor.close()
        mysql_conn.close()

        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBenchmark interrupted. Measurement records were not changed.")
        sys.exit(130)
    except Exception as error:
        print(f"\nERROR: {error}")
        print("Check the database state and error before rerunning.")
        sys.exit(1)
        