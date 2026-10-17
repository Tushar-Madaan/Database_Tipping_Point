
"""Shared dataset utilities for the Database Tipping Point project."""

from pathlib import Path
import gzip
import zipfile
import pandas as pd

# 1. Define project paths in one place.
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATASET = PROJECT_DIR / "data_raw" / "data_gang_gang.zip"
RESULTS = PROJECT_DIR / "results"

# 2. Define the expected columns in every air-quality CSV file.
EXPECTED_COLUMNS = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value"
]


# 3. Check that the original ZIP file exists and is readable.
def check_dataset():
    if not DATASET.is_file():
        raise FileNotFoundError(f"Dataset not found: {DATASET}")

    with zipfile.ZipFile(DATASET) as archive:
        if archive.testzip() is not None:
            raise ValueError("The ZIP archive contains a corrupt file.")

    return True


# 4. Find all compressed CSV files in the ZIP archive.
def list_files():
    check_dataset()
    with zipfile.ZipFile(DATASET) as archive:
        return sorted(
            name for name in archive.namelist()
            if name.endswith(".csv.gz")
        )


# 5. Read one compressed CSV without extracting it.
def read_file(archive, filename, **options):
    with archive.open(filename) as member:
        with gzip.GzipFile(fileobj=member) as csv_file:
            return pd.read_csv(csv_file, **options)


# 6. Yield manageable batches from every CSV file.
def read_chunks(size=10_000):
    if size <= 0:
        raise ValueError("Chunk size must be positive.")

    with zipfile.ZipFile(DATASET) as archive:
        for filename in list_files():
            with archive.open(filename) as member:
                with gzip.GzipFile(fileobj=member) as csv_file:
                    for chunk in pd.read_csv(
                        csv_file, chunksize=size
                    ):
                        yield filename, chunk


# 7. Perform basic checks on a DataFrame.
def check_data(df):
    return {
        "rows": len(df),
        "missing_values": int(df.isna().sum().sum()),
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_columns": [
            col for col in EXPECTED_COLUMNS if col not in df.columns
        ],
    }


# 8. Create the results folder when needed.
def prepare_results():
    RESULTS.mkdir(parents=True, exist_ok=True)


# 9. Run a quick connection and dataset test.
if __name__ == "__main__":
    prepare_results()
    files = list_files()

    print("Dataset:", DATASET)
    print("Compressed CSV files:", len(files))

    with zipfile.ZipFile(DATASET) as archive:
        sample = read_file(archive, files[0], nrows=5)

    print("\nSample records:")
    print(sample.to_string(index=False))
    print("\nBasic checks:")
    print(check_data(sample))
