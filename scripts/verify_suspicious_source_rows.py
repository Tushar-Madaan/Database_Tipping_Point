"""
Verify suspicious sample records against the original compressed CSV files.

Run from your Database_Tipping_Point project folder:
    python scripts/verify_suspicious_source_rows.py

Expected inputs:
    data_raw/data_gang_gang.zip
    results/outlier_samples.csv

Outputs:
    results/source_row_verification.csv
    results/source_row_verification_summary.txt

This script does not modify the source archive or change any measurements.
"""
from pathlib import Path
import csv
import gzip
import io
import zipfile
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]
ZIP_PATH = ROOT / "data_raw" / "data_gang_gang.zip"
SAMPLE_PATH = ROOT / "results" / "outlier_samples.csv"
OUT_CSV = ROOT / "results" / "source_row_verification.csv"
OUT_SUMMARY = ROOT / "results" / "source_row_verification_summary.txt"

KEY_FIELDS = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value"
]

def norm(value):
    return str(value).strip() if value is not None else ""

def close_numeric(a, b, tolerance=1e-8):
    try:
        return abs(float(a) - float(b)) <= tolerance
    except (TypeError, ValueError):
        return norm(a) == norm(b)

def row_matches(sample, raw):
    # Exact text match for categorical/time fields; tolerant numeric match
    # for coordinates and measurement values to account for CSV formatting.
    for field in ("location_id", "sensors_id", "location", "datetime",
                  "parameter", "units"):
        if norm(sample.get(field)) != norm(raw.get(field)):
            return False
    for field in ("lat", "lon", "value"):
        if not close_numeric(sample.get(field), raw.get(field)):
            return False
    return True

def main():
    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"Original dataset ZIP not found: {ZIP_PATH}")
    if not SAMPLE_PATH.exists():
        raise FileNotFoundError(f"Outlier sample CSV not found: {SAMPLE_PATH}")

    with SAMPLE_PATH.open("r", newline="", encoding="utf-8-sig") as f:
        samples = list(csv.DictReader(f))

    # Group sample rows by their source file so each compressed CSV is opened once.
    by_source = defaultdict(list)
    for idx, sample in enumerate(samples, start=1):
        sample["_sample_row"] = str(idx)
        by_source[norm(sample.get("source_file"))].append(sample)

    results = []
    source_counts = Counter()
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        archive_names = set(zf.namelist())

        for source_file, group in by_source.items():
            if not source_file:
                for sample in group:
                    results.append(make_result(sample, "MISSING_SOURCE_FILE", 0))
                continue

            # Match source paths exactly first; try removing a leading "./" as fallback.
            member = source_file if source_file in archive_names else source_file.removeprefix("./")
            if member not in archive_names:
                for sample in group:
                    results.append(make_result(sample, "SOURCE_FILE_NOT_IN_ZIP", 0))
                continue

            wanted = set(id(sample) for sample in group)
            found = Counter()
            with zf.open(member, "r") as compressed:
                with gzip.GzipFile(fileobj=compressed, mode="rb") as gz:
                    text_stream = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
                    reader = csv.DictReader(text_stream)
                    for raw in reader:
                        for sample in group:
                            if row_matches(sample, raw):
                                found[id(sample)] += 1

            for sample in group:
                count = found[id(sample)]
                status = "MATCHED_ONCE" if count == 1 else (
                    "MATCHED_MULTIPLE_TIMES" if count > 1 else "NO_MATCH_FOUND"
                )
                results.append(make_result(sample, status, count))
                source_counts[status] += 1

    fieldnames = [
        "_sample_row", "source_file", "location_id", "sensors_id", "datetime",
        "parameter", "units", "value", "outlier_type", "verification_status",
        "matching_source_rows"
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow({k: result.get(k, "") for k in fieldnames})

    lines = [
        "SOURCE ROW VERIFICATION SUMMARY",
        f"Sample rows checked: {len(samples)}",
        f"Distinct source files referenced: {len(by_source)}",
        f"ZIP archive: {ZIP_PATH}",
        "",
        "Verification outcomes:"
    ]
    for status in ("MATCHED_ONCE", "MATCHED_MULTIPLE_TIMES", "NO_MATCH_FOUND",
                   "SOURCE_FILE_NOT_IN_ZIP", "MISSING_SOURCE_FILE"):
        lines.append(f"  {status}: {source_counts.get(status, 0)}")
    lines += [
        "",
        "Interpretation:",
        "- MATCHED_ONCE means the sampled record was found once in its named source file.",
        "- MATCHED_MULTIPLE_TIMES means the matching record appeared more than once in that file.",
        "- NO_MATCH_FOUND means no matching record was found in the named file; inspect formatting or source mapping.",
        "- This verifies traceability only. It does not prove whether a value is scientifically valid or a sentinel code.",
        "- No source files or measurements were modified."
    ]
    OUT_SUMMARY.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nDetailed results: {OUT_CSV}")
    print(f"Summary: {OUT_SUMMARY}")

def make_result(sample, status, count):
    return {
        "_sample_row": sample.get("_sample_row", ""),
        "source_file": sample.get("source_file", ""),
        "location_id": sample.get("location_id", ""),
        "sensors_id": sample.get("sensors_id", ""),
        "datetime": sample.get("datetime", ""),
        "parameter": sample.get("parameter", ""),
        "units": sample.get("units", ""),
        "value": sample.get("value", ""),
        "outlier_type": sample.get("outlier_type", ""),
        "verification_status": status,
        "matching_source_rows": count,
    }

if __name__ == "__main__":
    main()
