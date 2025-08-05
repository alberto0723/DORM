import argparse
import os
from modules.fetch import fetch_logs
from modules.count import get_total_logs
from modules.parse import process_csv
from modules.clean import clean_queries
from modules.group import (
    group_queries_by_table,
    calculate_column_frequencies,
    save_grouped_queries,
    load_cleaned_queries,
    save_frequencies  
)
# fastapi import FastAPI
#from api import workload


def main():
    #app = FastAPI()
    #app.include_router(workload.router, prefix="/api")

    parser = argparse.ArgumentParser(description="🛠️ Workload SQL Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Fetch mode
    fetch_parser = subparsers.add_parser("fetch", help="Fetch SQL logs from SkyServer for a given time period")
    fetch_parser.add_argument("--year", type=int, required=True)
    fetch_parser.add_argument("--month", type=int, required=True)
    fetch_parser.add_argument("--day", type=int, required=False)
    fetch_parser.add_argument("--limit", type=int, required=False)

    # Count mode
    count_parser = subparsers.add_parser("count", help="Count total SQL logs for a time period")
    count_parser.add_argument("--year", type=int, required=True)
    count_parser.add_argument("--month", type=int, required=False)
    count_parser.add_argument("--day", type=int, required=False)

    # Parse mode
    parse_parser = subparsers.add_parser("parse", help="Parse SQL statements and extract features")
    parse_parser.add_argument("--input", required=True)
    parse_parser.add_argument("--output", required=True)

    # Clean mode
    clean_parser = subparsers.add_parser("clean", help="Remove queries involving MyDB")
    clean_parser.add_argument("--input", required=True)
    clean_parser.add_argument("--output", required=True)

    # Group mode
    group_parser = subparsers.add_parser("group", help="Group queries and analyze column patterns")
    group_parser.add_argument("--input", required=True, help="Path to cleaned queries JSON")
    group_parser.add_argument("--output", required=True, help="Path to save grouped queries")
    group_parser.add_argument("--freq_output", default="column_frequencies.json", help="Path to save frequency file")
    group_parser.add_argument("--threshold", type=float, default=0.005, help="Frequency threshold (e.g., 0.005)")
    group_parser.add_argument("--jaccard", type=float, default=0.8, help="Jaccard similarity threshold")
    group_parser.add_argument("--modifiers", nargs="*", default=[], choices=["distinct", "groupby", "orderby", "top"],
                              help="Optional modifiers to include in grouping")
    group_parser.add_argument("--store_groups", action="store_true", help="Save grouped query data")
    args = parser.parse_args()

    # Auto-prepend "data/" if not already present
    if hasattr(args, "output") and not args.output.startswith("tmp/"):
        args.output = os.path.join("tmp", args.output)
    if hasattr(args, "input") and not args.input.startswith("tmp/"):
        args.input = os.path.join("tmp", args.input)
    if hasattr(args, "freq_output") and not args.freq_output.startswith("tmp/"):
        args.freq_output = os.path.join("tmp", args.freq_output)

    try:
        if args.command == "fetch":
            fetch_logs(year=args.year, month=args.month, day=getattr(args, "day", None), limit=getattr(args, "limit", None))
        elif args.command == "count":
            get_total_logs(year=args.year, month=getattr(args, "month", None), day=getattr(args, "day", None))
        elif args.command == "parse":
            process_csv(args.input, args.output)
        elif args.command == "clean":
            clean_queries(args.input, args.output)
        elif args.command == "group":
            queries = load_cleaned_queries(args.input)
            grouped = group_queries_by_table(queries, args.modifiers)
            frequencies = calculate_column_frequencies(grouped, args.threshold, args.jaccard)
            # Save frequencies correctly (stringify keys)
            save_frequencies(frequencies, args.freq_output)
            # Save full grouped queries only if requested
            if args.store_groups:
                save_grouped_queries(grouped, args.output, args.modifiers)

    except Exception as e:
        print(f"🚨 Error: {e}")


if __name__ == "__main__":
    main()
