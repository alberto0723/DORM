import logging
import sys
import argparse
from pathlib import Path
import json
import csv
import catalog.tools as tools

import catalog.config as config
from catalog.first_normal_form import FirstNormalForm
from catalog.non_first_normal_form_json import NonFirstNormalFormJSON

if __name__ == "__main__":

    # Path definitions
    base_path = Path(__file__).parent

    # ---------------------------------------------------------------------------- #
    #                                configure argparse begin                      #
    # ---------------------------------------------------------------------------- #
    base_parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.HelpFormatter(prog, width=160),
        add_help=False,
        description="🔍 Execute queries over a pre-existing catalog"
    )
    base_parser.add_argument("--help", help="Shows this help message and exit", action="store_true")
    base_parser.add_argument("--logging", help="Enables logging", action="store_true")
    base_parser.add_argument("--show_sql", help="Prints the generated statements", action="store_true")
    base_parser.add_argument("--hide_progress", help="Silences progress bars and messages", action="store_true")
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--paradigm", type=str, choices=["1NF", "NF2_JSON"], required=True, help="Implementation paradigm for the design (either 1NF or NF2_JSON)", metavar="<prdgm>")
    base_parser.add_argument("--dbconf_file", type=str, help="Filename of the configuration file for DBMS connection", metavar="<conf>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--query_file", type=Path, help="Filename of the json file containing the queries", metavar="<path>")
    base_parser.add_argument("--print_rows", help="Prints the resulting rows", action="store_true")
    base_parser.add_argument("--print_counter", help="Prints the number of rows", action="store_true")
    base_parser.add_argument("--print_time", help="Prints the estimated time of each query (in milliseconds)", action="store_true")
    base_parser.add_argument("--print_cost", help="Prints the unitless cost estimation of each query", action="store_true")
    base_parser.add_argument("--save_cost", help="Saves the costs of the queries in a CSV file with the same name as that of the queries (just different extension)", action="store_true")

    # Manually check for help before full parsing
    if len(sys.argv) == 1 or '--help' in sys.argv or '-h' in sys.argv:
        base_parser.print_help()
        sys.exit(0)
    else:
        args = base_parser.parse_args()
        config.show_warnings = not args.hide_warnings
        config.show_progress = not args.hide_progress
        if args.logging:
            # Enable logging
            logging.basicConfig(level=logging.INFO)
        else:
            logging.disable()
        logging.info("BEGIN")
        assert args.paradigm in ["1NF", "NF2_JSON"], f"☠️ Only paradigms allowed are 1NF and NF2_JSON"
        if args.paradigm == "1NF":
            cat = FirstNormalForm(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema)
        else:
            cat = NonFirstNormalFormJSON(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema)
        logging.info("Executing batch queries")
        # Open and load the JSON file
        with open(args.query_file.with_suffix(".json"), 'r') as file:
            query_specs = json.load(file).get("queries")
        cost_per_query = [["Order", "Group ID", "Weight", "Cost"]]
        sum_cost = 0
        sum_frequencies = 0
        for i, spec in enumerate(query_specs):
            if True:
                print(f"\n-- Running query specification {i+1}")
                queries = cat.generate_query_statement(spec, explicit_schema=False)
                min_position = 0
                if args.print_cost or args.save_cost:
                    cost_vector = []
                    for q in queries:
                        cost_vector.append(cat.get_cost(queries[0]))
                    min_position = cost_vector.index(min(cost_vector))
                if args.show_sql:
                    print(r"--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
                    if len(queries) > 1:
                        print(f"Number of queries generated: {len(queries)}")
                        if args.print_cost or args.save_cost:
                            print("Best one is:")
                        else:
                            print("First one is:")
                    print(queries[min_position]+";")
                    print("--//////////////////////////////////////////")
                if args.print_cost or args.save_cost:
                    current_frec = spec.get("frequency", 1)
                    cost_per_query.append([i+1, spec.get("group_id", ""), current_frec, cost_vector[min_position]])
                    sum_frequencies += current_frec
                    sum_cost += cost_vector[min_position]*current_frec
                    if args.print_cost:
                        print("Vector of costs:", cost_vector)
                        print("Minimum position:", min_position)
                        print(f"Estimated cost: {cost_vector[min_position]:.2f}")
                        print(f"Weighted cost: {cost_vector[min_position]*current_frec:.2f} (for a weight of {current_frec:.2f})")
                if args.print_time:
                    print("Estimated time: ", cat.get_time(queries[min_position]))
                if args.print_rows or args.print_counter:
                    rows = cat.execute(queries[min_position])
                    if args.print_rows:
                        for row in rows:
                            print(row)
                    if args.print_counter:
                        print(f"Number of rows: {len(rows)}")
        if args.print_cost:
            print("=======================================")
            print(f"Average cost: {sum_cost/sum_frequencies:.2f}", )
        if args.save_cost:
            # Open and write to CSV
            with open(args.query_file.with_suffix(".csv"), mode="w", newline="") as result_file:
                writer = csv.writer(result_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerows(cost_per_query)
        logging.info("END")
