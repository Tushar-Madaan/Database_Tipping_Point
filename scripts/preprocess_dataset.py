"""
Reproducible, disk-backed data quality and outlier audit.

- Reads the original ZIP in chunks.
- Uses SQLite for exact duplicate tracking and measurement storage.
- Calculates exact, linearly interpolated IQR thresholds per pollutant and unit.
- Separates missing, nonnumeric, non-finite, and range/integrality errors.
- Saves outlier summaries and bounded samples.
- Never modifies the source ZIP.
"""

import json
import math
import os
import sqlite3
import time
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from dataset_utils import (
    DATASET,
    RESULTS,
    read_chunks,
    prepare_results,
    EXPECTED_COLUMNS,
)


# ============================================================
# 1. Configuration
# ============================================================

# This is the row count previously verified for this exact dataset.
# If the source ZIP changes, independently recount it and update this.
EXPECTED_TOTAL_ROWS = 1_050_603

SAMPLES_PER_CATEGORY = 250
IQR_MULTIPLIER = 1.5
SENTINEL_CANDIDATES = [-999.0, -999000.0]
QUANTILE_METHOD = "linear interpolation at position (n - 1) * p"

# Set True if you want to retain the SQLite working database after success.
# Keeping it can help debugging, but consumes additional disk space.
KEEP_AUDIT_DB = False

NUMERIC_COLUMNS = ["location_id", "sensors_id", "lat", "lon", "value"]
IDENTIFIER_COLUMNS = ["location_id", "sensors_id"]
COORDINATE_COLUMNS = ["lat", "lon"]
MEASUREMENT_COLUMN = "value"


# ============================================================
# 2. Output paths and helpers
# ============================================================

prepare_results()

report_path = RESULTS / "data_quality_report.json"
audit_db_path = RESULTS / "audit_working.sqlite"
outlier_summary_path = RESULTS / "outlier_summary.csv"
outlier_samples_path = RESULTS / "outlier_samples.csv"


def json_value(value):
    """Convert a pandas/NumPy scalar into a stable JSON-compatible value."""
    if value is None or value is pd.NA:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, np.generic):
        value = value.item()

    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    return str(value)


def canonical_row(row):
    """Serialize every expected parsed field for exact row matching."""
    values = [json_value(row[col]) for col in EXPECTED_COLUMNS]
    return json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def finite_numeric(series):
    """Return numeric conversion and a NumPy mask identifying finite values."""
    numeric = pd.to_numeric(series, errors="coerce")
    array = numeric.to_numpy(dtype=float, na_value=np.nan)
    return numeric, np.isfinite(array)


def exact_quantile(conn, parameter, units, probability, count):
    """Calculate an exact linearly interpolated quantile from SQLite."""
    if count <= 0:
        return None

    position = (count - 1) * probability
    lower_position = math.floor(position)
    upper_position = math.ceil(position)
    fraction = position - lower_position

    lower_row = conn.execute(
        """
        SELECT value
        FROM measurements
        WHERE parameter = ? AND units = ?
        ORDER BY value
        LIMIT 1 OFFSET ?
        """,
        (parameter, units, lower_position),
    ).fetchone()

    if lower_row is None:
        raise RuntimeError(
            f"Could not retrieve lower quantile for {parameter!r}, {units!r}."
        )

    lower_value = float(lower_row[0])

    if lower_position == upper_position:
        return lower_value

    upper_row = conn.execute(
        """
        SELECT value
        FROM measurements
        WHERE parameter = ? AND units = ?
        ORDER BY value
        LIMIT 1 OFFSET ?
        """,
        (parameter, units, upper_position),
    ).fetchone()

    if upper_row is None:
        raise RuntimeError(
            f"Could not retrieve upper quantile for {parameter!r}, {units!r}."
        )

    upper_value = float(upper_row[0])
    return float(lower_value + fraction * (upper_value - lower_value))


def add_sample(samples, category, record):
    """Keep a deterministic, bounded number of examples per category."""
    if len(samples[category]) < SAMPLES_PER_CATEGORY:
        samples[category].append(record)


def remove_sqlite_files(path):
    """Remove the temporary SQLite database and any SQLite sidecar files."""
    for candidate in (
        path,
        Path(str(path) + "-wal"),
        Path(str(path) + "-shm"),
        Path(str(path) + "-journal"),
    ):
        try:
            if candidate.exists():
                candidate.unlink()
        except OSError as exc:
            raise RuntimeError(
                f"Could not remove temporary SQLite file {candidate}. "
                "Close programs using it and retry."
            ) from exc


def safe_size_bytes(path):
    """Return a file's size if it exists, otherwise zero."""
    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


# ============================================================
# 3. Initialize counters and report structures
# ============================================================

started_at = time.perf_counter()
total_rows = 0
processed_rows = 0
schema_errors = 0
chunks_processed = 0

missing = Counter()
nonnumeric = Counter()
nonfinite = Counter()
invalid = Counter()

parameters = Counter()
locations = set()
location_ids = set()
sensor_ids = set()
units_by_parameter = {}

measurement_stats = {}
iqr_thresholds = {}
outlier_counts = Counter()

outlier_samples = {
    "iqr_low": [],
    "iqr_high": [],
    "negative_reading": [],
    "candidate_sentinel": [],
}

# Ensure an old working database and its sidecars cannot contaminate this run.
# This removes only the temporary audit database, never the source ZIP or reports.
remove_sqlite_files(audit_db_path)


# ============================================================
# 4. First pass: validate and store data in bounded chunks
# ============================================================

with closing(sqlite3.connect(audit_db_path)) as conn:
    # SQLite stays disk-backed; the cache is limited to approximately 16 MiB.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = FILE")
    conn.execute("PRAGMA cache_size = -16384")

    conn.execute(
        """
        CREATE TABLE exact_rows (
            row_json TEXT PRIMARY KEY,
            occurrences INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE measurements (
            parameter TEXT NOT NULL,
            units TEXT NOT NULL,
            value REAL NOT NULL
        )
        """
    )
    conn.commit()

    for filename, chunk in read_chunks():
        chunks_processed += 1
        total_rows += len(chunk)

        if list(chunk.columns) != EXPECTED_COLUMNS:
            schema_errors += 1
            print(f"Schema mismatch: {filename}")
            continue

        processed_rows += len(chunk)

        # 4a. Count missing values in every expected column.
        missing.update({
            col: int(chunk[col].isna().sum())
            for col in EXPECTED_COLUMNS
        })

        # 4b. Validate numeric fields.
        numeric_columns = {}
        for col in NUMERIC_COLUMNS:
            numeric, finite_mask = finite_numeric(chunk[col])
            numeric_columns[col] = numeric

            nonnumeric[col] += int(
                (chunk[col].notna() & numeric.isna()).sum()
            )

            nonfinite_mask = numeric.notna().to_numpy() & ~finite_mask
            nonfinite[col] += int(nonfinite_mask.sum())

            # Broad invalid count includes missing, nonnumeric and non-finite.
            invalid[col] += int(numeric.isna().sum()) + int(nonfinite_mask.sum())

        # 4c. Identifier values must be finite integers.
        for col in IDENTIFIER_COLUMNS:
            numeric = numeric_columns[col]
            fractional = numeric.notna() & (numeric % 1 != 0)
            invalid[f"{col}_not_integer"] += int(fractional.sum())

        # 4d. Coordinate range checks, only for numeric values.
        lat = numeric_columns["lat"]
        lon = numeric_columns["lon"]
        invalid["latitude_range"] += int(
            ((lat < -90) | (lat > 90)).fillna(False).sum()
        )
        invalid["longitude_range"] += int(
            ((lon < -180) | (lon > 180)).fillna(False).sum()
        )

        # 4e. Parse timestamps as UTC for validation only.
        dates = pd.to_datetime(chunk["datetime"], errors="coerce", utc=True)
        invalid["datetime"] += int(dates.isna().sum())

        # 4f. Count categories and identifiers.
        parameters.update(
            chunk["parameter"].fillna("<MISSING>").astype(str)
        )
        locations.update(chunk["location"].dropna().astype(str))
        location_ids.update(chunk["location_id"].dropna().astype(str))
        sensor_ids.update(chunk["sensors_id"].dropna().astype(str))

        for parameter, group in chunk.groupby("parameter", dropna=False):
            parameter_key = "<MISSING>" if pd.isna(parameter) else str(parameter)
            units_by_parameter.setdefault(parameter_key, Counter())
            units_by_parameter[parameter_key].update(
                group["units"].fillna("<MISSING>").astype(str)
            )

        # 4g. Measurement statistics by pollutant, using finite numeric values.
        values = numeric_columns[MEASUREMENT_COLUMN]
        for parameter, indexes in chunk.groupby(
            "parameter", dropna=False
        ).groups.items():
            group_values = values.loc[indexes]
            group_values = group_values[
                np.isfinite(
                    group_values.to_numpy(dtype=float, na_value=np.nan)
                )
            ]
            if group_values.empty:
                continue

            parameter_key = "<MISSING>" if pd.isna(parameter) else str(parameter)
            current = measurement_stats.setdefault(
                parameter_key,
                {"count": 0, "sum": 0.0, "min": None, "max": None},
            )
            current["count"] += int(len(group_values))
            current["sum"] += float(group_values.sum())
            group_min = float(group_values.min())
            group_max = float(group_values.max())
            current["min"] = (
                group_min if current["min"] is None
                else min(current["min"], group_min)
            )
            current["max"] = (
                group_max if current["max"] is None
                else max(current["max"], group_max)
            )

        # 4h. Store finite measurements in SQLite in chunk-bounded batches.
        measurement_rows = []
        for (parameter, unit), group in chunk.groupby(
            ["parameter", "units"], dropna=False
        ):
            group_values = pd.to_numeric(group["value"], errors="coerce")
            group_values = group_values[
                np.isfinite(
                    group_values.to_numpy(dtype=float, na_value=np.nan)
                )
            ]
            parameter_key = "<MISSING>" if pd.isna(parameter) else str(parameter)
            unit_key = "<MISSING>" if pd.isna(unit) else str(unit)
            measurement_rows.extend(
                (parameter_key, unit_key, float(value))
                for value in group_values
            )

        conn.executemany(
            """
            INSERT INTO measurements (parameter, units, value)
            VALUES (?, ?, ?)
            """,
            measurement_rows,
        )

        # 4i. Count exact duplicates by matching complete serialized rows.
        row_counts = Counter(
            canonical_row(row)
            for row in chunk[EXPECTED_COLUMNS].to_dict("records")
        )
        conn.executemany(
            """
            INSERT INTO exact_rows (row_json, occurrences)
            VALUES (?, ?)
            ON CONFLICT(row_json)
            DO UPDATE SET occurrences =
                occurrences + excluded.occurrences
            """,
            [
                (row_json, occurrences)
                for row_json, occurrences in row_counts.items()
            ],
        )

        conn.commit()

        # Avoid printing once per file/chunk; report at coarse intervals.
        if total_rows // 100_000 != (total_rows - len(chunk)) // 100_000:
            print(f"First pass: processed {total_rows:,} rows...")

    # Verify row count and schema before proceeding to expensive calculations.
    if total_rows != EXPECTED_TOTAL_ROWS:
        raise RuntimeError(
            f"Dataset row-count mismatch: expected {EXPECTED_TOTAL_ROWS:,}, "
            f"found {total_rows:,}. Check that the source dataset is unchanged."
        )

    if processed_rows != total_rows or schema_errors != 0:
        raise RuntimeError(
            f"Audit cannot continue: total rows={total_rows:,}, "
            f"schema-valid rows={processed_rows:,}, "
            f"schema errors={schema_errors}."
        )

    # 4j. Calculate duplicate totals.
    duplicate_groups, extra_duplicates = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN occurrences > 1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(
                CASE WHEN occurrences > 1 THEN occurrences - 1 ELSE 0 END
            ), 0)
        FROM exact_rows
        """
    ).fetchone()

    # Create a composite index for grouped, ordered quantile lookups.
    conn.execute(
        """
        CREATE INDEX idx_measurements_group_value
        ON measurements (parameter, units, value)
        """
    )
    conn.commit()

    # ========================================================
    # 5. Calculate exact IQR thresholds from disk
    # ========================================================

    group_counts = conn.execute(
        """
        SELECT parameter, units, COUNT(*)
        FROM measurements
        GROUP BY parameter, units
        ORDER BY parameter, units
        """
    ).fetchall()

    for parameter, unit, count in group_counts:
        q1 = exact_quantile(conn, parameter, unit, 0.25, count)
        q3 = exact_quantile(conn, parameter, unit, 0.75, count)
        iqr = q3 - q1

        iqr_thresholds[(parameter, unit)] = {
            "measurement_count": int(count),
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": q1 - IQR_MULTIPLIER * iqr,
            "upper_bound": q3 + IQR_MULTIPLIER * iqr,
        }

# The SQLite connection is closed here.


# ============================================================
# 6. Calculate final measurement averages
# ============================================================

for item in measurement_stats.values():
    item["mean"] = item["sum"] / item["count"] if item["count"] else None
    # Keep the report compact; the sum is an intermediate calculation.
    item.pop("sum", None)


# ============================================================
# 7. Second pass: count outliers and save bounded samples
# ============================================================

with closing(sqlite3.connect(audit_db_path)) as conn:
    second_pass_rows = 0
    for filename, chunk in read_chunks():
        if list(chunk.columns) != EXPECTED_COLUMNS:
            continue

        second_pass_rows += len(chunk)
        values = pd.to_numeric(chunk["value"], errors="coerce")
        finite_values = np.isfinite(
            values.to_numpy(dtype=float, na_value=np.nan)
        )

        # Flag non-finite readings separately in validation; do not classify
        # them as negative or IQR outliers.
        negative_mask = (values < 0).fillna(False) & finite_values
        sentinel_mask = values.isin(SENTINEL_CANDIDATES) & finite_values

        for mask, category in (
            (negative_mask, "negative_reading"),
            (sentinel_mask, "candidate_sentinel"),
        ):
            flagged = chunk.loc[mask]
            for (parameter, unit), group in flagged.groupby(
                ["parameter", "units"], dropna=False
            ):
                parameter_key = "<MISSING>" if pd.isna(parameter) else str(parameter)
                unit_key = "<MISSING>" if pd.isna(unit) else str(unit)
                outlier_counts[(parameter_key, unit_key, category)] += int(len(group))

                remaining = SAMPLES_PER_CATEGORY - len(outlier_samples[category])
                if remaining > 0:
                    for record in group.head(remaining).to_dict("records"):
                        record["outlier_type"] = category
                        record["lower_iqr_bound"] = None
                        record["upper_iqr_bound"] = None
                        record["source_file"] = filename
                        add_sample(outlier_samples, category, record)

        # Apply IQR thresholds independently per pollutant and unit.
        for (parameter, unit), group in chunk.groupby(
            ["parameter", "units"], dropna=False
        ):
            parameter_key = "<MISSING>" if pd.isna(parameter) else str(parameter)
            unit_key = "<MISSING>" if pd.isna(unit) else str(unit)
            key = (parameter_key, unit_key)
            thresholds = iqr_thresholds.get(key)
            if thresholds is None:
                continue

            group_values = pd.to_numeric(group["value"], errors="coerce")
            finite_mask = np.isfinite(
                group_values.to_numpy(dtype=float, na_value=np.nan)
            )
            low_mask = (
                (group_values < thresholds["lower_bound"]).fillna(False)
                & finite_mask
            )
            high_mask = (
                (group_values > thresholds["upper_bound"]).fillna(False)
                & finite_mask
            )

            for mask, category in (
                (low_mask, "iqr_low"),
                (high_mask, "iqr_high"),
            ):
                flagged = group.loc[mask]
                outlier_counts[(key[0], key[1], category)] += int(len(flagged))

                remaining = SAMPLES_PER_CATEGORY - len(outlier_samples[category])
                if remaining > 0:
                    for record in flagged.head(remaining).to_dict("records"):
                        record["outlier_type"] = category
                        record["lower_iqr_bound"] = thresholds["lower_bound"]
                        record["upper_iqr_bound"] = thresholds["upper_bound"]
                        record["source_file"] = filename
                        add_sample(outlier_samples, category, record)

        # Report progress occasionally, without printing every source file.
        if second_pass_rows and second_pass_rows % 100_000 < len(chunk):
            print(f"Second pass: processed {second_pass_rows:,} rows...")

# ============================================================
# 8. Save outlier summary and bounded sample CSV files
# ============================================================

summary_rows = []
for (parameter, unit, category), count in sorted(outlier_counts.items()):
    thresholds = iqr_thresholds.get((parameter, unit), {})
    summary_rows.append(
        {
            "parameter": parameter,
            "units": unit,
            "category": category,
            "count": int(count),
            "q1": thresholds.get("q1"),
            "q3": thresholds.get("q3"),
            "iqr": thresholds.get("iqr"),
            "lower_iqr_bound": thresholds.get("lower_bound"),
            "upper_iqr_bound": thresholds.get("upper_bound"),
        }
    )

summary_columns = [
    "parameter", "units", "category", "count",
    "q1", "q3", "iqr", "lower_iqr_bound", "upper_iqr_bound",
]
pd.DataFrame(summary_rows, columns=summary_columns).to_csv(
    outlier_summary_path, index=False
)

sample_rows = []
for category in (
    "iqr_low", "iqr_high", "negative_reading", "candidate_sentinel"
):
    sample_rows.extend(outlier_samples[category])

sample_columns = [
    *EXPECTED_COLUMNS,
    "outlier_type",
    "lower_iqr_bound",
    "upper_iqr_bound",
    "source_file",
]
pd.DataFrame(sample_rows, columns=sample_columns).to_csv(
    outlier_samples_path, index=False
)


# ============================================================
# 9. Assemble and save the final quality report
# ============================================================

audit_db_size_bytes = safe_size_bytes(audit_db_path)
elapsed_seconds = time.perf_counter() - started_at

report = {
    "source": str(DATASET),
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "total_rows": int(total_rows),
    "processed_rows": int(processed_rows),
    "expected_total_rows": EXPECTED_TOTAL_ROWS,
    "chunks_processed_first_pass": int(chunks_processed),
    "schema_errors": int(schema_errors),
    "missing_values": dict(missing),
    "nonnumeric_values": dict(nonnumeric),
    "nonfinite_values": dict(nonfinite),
    "invalid_values": dict(invalid),
    "invalid_values_note": (
        "For numeric columns, invalid_values includes missing, nonnumeric, "
        "and non-finite values; these categories can overlap with the "
        "separate missing_values and nonnumeric_values counters. Range and "
        "identifier-integrality errors are counted separately."
    ),
    "exact_duplicate_definition": (
        "All expected parsed field values match after stable JSON "
        "serialization; this is not byte-for-byte CSV comparison."
    ),
    "exact_duplicate_groups": int(duplicate_groups),
    "extra_exact_duplicate_rows": int(extra_duplicates),
    "unique_location_names": len(locations),
    "unique_location_ids": len(location_ids),
    "unique_sensor_ids": len(sensor_ids),
    "parameter_counts": dict(parameters),
    "units_by_parameter": {
        key: dict(value) for key, value in units_by_parameter.items()
    },
    "measurement_statistics": measurement_stats,
    "outlier_analysis": {
        "method": "Tukey IQR",
        "multiplier": IQR_MULTIPLIER,
        "quantile_method": QUANTILE_METHOD,
        "grouping": ["parameter", "units"],
        "thresholds": {
            f"{parameter}|{unit}": thresholds
            for (parameter, unit), thresholds in iqr_thresholds.items()
        },
        "flagged_counts": {
            f"{parameter}|{unit}|{category}": int(count)
            for (parameter, unit, category), count in sorted(outlier_counts.items())
        },
        "sample_limit_per_category": SAMPLES_PER_CATEGORY,
        "summary_csv": str(outlier_summary_path),
        "samples_csv": str(outlier_samples_path),
        "note": (
            "Flags are for investigation only. No measurements were deleted "
            "or corrected. Negative values and sentinel candidates are not "
            "automatically considered errors."
        ),
    },
    "resource_usage": {
        "audit_elapsed_seconds": round(elapsed_seconds, 3),
        "sqlite_working_database_size_bytes_before_cleanup": audit_db_size_bytes,
        "sqlite_working_database_retained": KEEP_AUDIT_DB,
        "sqlite_cache_limit_mib": 16,
        "note": (
            "Elapsed time covers the audit script through report preparation. "
            "It is not a controlled CPU benchmark. Database size is measured "
            "before optional cleanup; WAL/SHM sidecars may also consume space."
        ),
    },
    "reproducibility": {
        "expected_columns": EXPECTED_COLUMNS,
        "chunk_reader": "dataset_utils.read_chunks",
        "measurement_storage": "SQLite disk-backed table",
        "duplicate_storage": "SQLite full-row serialized key",
        "source_modified": False,
    },
}

with open(report_path, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=2, ensure_ascii=False, allow_nan=False)


# ============================================================
# 10. Print the final summary and clean up temporary files
# ============================================================

print("\n========== DATA QUALITY AUDIT COMPLETE ==========")
print(f"Total rows: {total_rows:,}")
print(f"Processed rows: {processed_rows:,}")
print(f"Schema errors: {schema_errors}")
print(f"Duplicate groups: {duplicate_groups:,}")
print(f"Extra duplicate rows: {extra_duplicates:,}")
print(f"Unique pollutants: {len(parameters):,}")
print(f"Elapsed time: {elapsed_seconds:.2f} seconds")
print(f"Outlier summary: {outlier_summary_path}")
print(f"Outlier samples: {outlier_samples_path}")
print(f"JSON report: {report_path}")
if KEEP_AUDIT_DB:
    print(f"SQLite working database retained: {audit_db_path}")
else:
    try:
        remove_sqlite_files(audit_db_path)
        print("Temporary SQLite database removed successfully.")
    except RuntimeError as exc:
        print(f"Warning: temporary database cleanup failed: {exc}")
        print("The temporary database may still exist.")

print("Original ZIP was not modified.")
