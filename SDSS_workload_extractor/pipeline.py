import argparse
import os
from modules.fetch import fetch_logs
from modules.parse import process_csv
from modules.clean import clean_queries
from modules.group import (
    load_cleaned_queries,
    group_queries_by_table,
    calculate_column_frequencies,
    save_grouped_queries,
    save_frequencies,
)

PIPELINE_DIR = "data"

def run_pipeline(
    csv_filename: str,
    fetch: bool = False,
    year: int = None,
    month: int = None,
    day: int = None,
    limit: int = 10000,
    modifiers: list = None,
    threshold: float = 0.01,
    jaccard: float = 0.8
):
    os.makedirs(PIPELINE_DIR, exist_ok=True)

    if fetch:
        if not year or not month:
            raise ValueError("Year and month are required when using --fetch.")
        print("Step 0: Fetching raw logs...")

        # This fetches to data/fetched_YYYY_MM_topLIMIT.csv
        fetch_logs(year=year, month=month, day=day, limit=limit)

        # Update path to match fetched file
        csv_filename = f"fetched_{year}_{str(month).zfill(2)}_top{limit}.csv"
        RAW_CSV = os.path.join("data", csv_filename)
    else:
        RAW_CSV = os.path.join(PIPELINE_DIR, csv_filename)

    PARSED_JSON = os.path.join(PIPELINE_DIR, "parsed.json")
    CLEANED_JSON = os.path.join(PIPELINE_DIR, "cleaned.json")
    GROUPED_JSON = os.path.join(PIPELINE_DIR, "grouped.json")
    FREQ_JSON = os.path.join(PIPELINE_DIR, "frequencies.json")

    print("Step 1: Parsing CSV...")
    process_csv(RAW_CSV, PARSED_JSON)

    print("Step 2: Cleaning parsed queries...")
    clean_queries(PARSED_JSON, CLEANED_JSON)

    print("Step 3: Grouping and frequency analysis...")
    queries = load_cleaned_queries(CLEANED_JSON)
    modifiers = modifiers or ["distinct", "groupby", "orderby", "top"]
    grouped = group_queries_by_table(queries, modifiers)
    frequencies = calculate_column_frequencies(grouped, threshold_ratio=threshold, jaccard_threshold=jaccard)

    print("Saving outputs...")
    save_grouped_queries(grouped, GROUPED_JSON, modifiers)
    save_frequencies(frequencies, FREQ_JSON)

    print("✅ Pipeline completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL workload pipeline with optional fetch step.")
    parser.add_argument("--csv", required=True, help="CSV file name to use or save")
    parser.add_argument("--fetch", action="store_true", help="Whether to fetch logs before processing.")
    parser.add_argument("--year", type=int, help="Year for fetch (required if --fetch is used)")
    parser.add_argument("--month", type=int, help="Month for fetch (required if --fetch is used)")
    parser.add_argument("--day", type=int, default=None, help="Optional day for fetch")
    parser.add_argument("--limit", type=int, default=10000, help="Number of logs to fetch")

    parser.add_argument("--modifiers", nargs="*", default=["distinct", "groupby", "orderby", "top"],
                        choices=["distinct", "groupby", "orderby", "top"],
                        help="Modifiers to use for grouping (default: all)")
    parser.add_argument("--threshold", type=float, default=0.01, help="Column frequency threshold (default: 0.01)")
    parser.add_argument("--jaccard", type=float, default=0.8, help="Jaccard similarity threshold (default: 0.8)")

    args = parser.parse_args()

    run_pipeline(
        csv_filename=args.csv,
        fetch=args.fetch,
        year=args.year,
        month=args.month,
        day=args.day,
        limit=args.limit,
        modifiers=args.modifiers,
        threshold=args.threshold,
        jaccard=args.jaccard
    )
