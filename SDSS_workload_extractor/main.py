import traceback
import argparse
import os
from modules.fetch import fetch_logs
from modules.count import get_total_logs
from modules.parse import process_input
from modules.group import (
    group_queries_by_table,
    calculate_column_frequencies,
    save_grouped_queries,
    load_parsed_queries
)


if __name__ == "__main__":
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

    # Group mode
    group_parser = subparsers.add_parser("group", help="Group queries and analyze column patterns")
    group_parser.add_argument("--folder", required=True, help="Path where input is and output will be generated")
    group_parser.add_argument("--threshold", type=float, default=0.005, help="Frequency threshold (e.g., 0.005)")
    group_parser.add_argument("--jaccard", type=float, default=0.8, help="Jaccard similarity threshold")
    group_parser.add_argument("--modifiers", nargs="*", default=[], choices=["distinct", "groupby", "orderby", "top"],
                              help="Optional modifiers to include in grouping")
    args = parser.parse_args()

    # Auto-prepend "data/" if not already present
    if hasattr(args, "output") and not args.output.startswith("data/"):
        args.output = os.path.join("data", args.output)
    if hasattr(args, "input") and not args.input.startswith("data/"):
        args.input = os.path.join("data", args.input)
    if hasattr(args, "folder") and not args.folder.startswith("data/"):
        args.folder = os.path.join("data", args.folder)

    try:
        if args.command == "fetch":
            fetch_logs(year=args.year, month=args.month, day=getattr(args, "day", None), limit=getattr(args, "limit", None))
        elif args.command == "count":
            get_total_logs(year=args.year, month=getattr(args, "month", None), day=getattr(args, "day", None))
        elif args.command == "parse":
            process_input(args.input, args.output)
        elif args.command == "group":
            queries = load_parsed_queries(args.folder)
            grouped = group_queries_by_table(queries, args.modifiers)
            filtered_groups = calculate_column_frequencies(grouped, args.modifiers, args.threshold, args.jaccard)
            save_grouped_queries(filtered_groups, args.folder)

    except Exception as e:
        print(f"🚨 Error: {e}")
        traceback.print_exc()
