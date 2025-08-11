import logging
import sys
import argparse
from pathlib import Path
import json
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
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--paradigm", type=str, choices=["1NF", "NF2_JSON"], required=True, help="Implementation paradigm for the design (either 1NF or NF2_JSON)", metavar="<prdgm>")
    base_parser.add_argument("--dbconf_file", type=str, help="Filename of the configuration file for DBMS connection", metavar="<conf>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--query_file", type=Path, help="Filename of the json file containing the queries", metavar="<path>")
    base_parser.add_argument("--print_rows", help="Prints the resulting rows", action="store_true")
    base_parser.add_argument("--print_counter", help="Prints the number of rows", action="store_true")
    base_parser.add_argument("--print_cost", help="Prints the unitless cost estimation of each query", action="store_true")
    base_parser.add_argument("--print_time", help="Prints the estimated time of each query (in milliseconds)", action="store_true")

    # Manually check for help before full parsing
    if len(sys.argv) == 1 or '--help' in sys.argv or '-h' in sys.argv:
        base_parser.print_help()
        sys.exit(0)
    else:
        args = base_parser.parse_args()
        config.show_warnings = not args.hide_warnings
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
        with open(args.query_file, 'r') as file:
            query_specs = json.load(file).get("queries")
        for i, spec in enumerate(query_specs):
            if True:
                print(f"-- Running query specification {i}")
                queries = cat.generate_query_statement(spec, explicit_schema=False)
                if args.show_sql:
                    print(r"--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
                    if len(queries) > 1:
                        print(f"Number of queries generated: {len(queries)}")
                        print("First one is:")
                    print(queries[0]+";")
                    print("--//////////////////////////////////////////")
                if args.print_cost:
                    print("Estimated cost: ", cat.get_cost(queries[0]))
                if args.print_time:
                    print("Estimated time: ", cat.get_time(queries[0]))
                if args.print_rows or args.print_counter:
                    rows = cat.execute(queries[0])
                    if args.print_rows:
                        for row in rows:
                            print(row)
                    if args.print_counter:
                        print(f"Number of rows: {len(rows)}")
        logging.info("END")
