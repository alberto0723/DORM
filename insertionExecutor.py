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
        description="🔍 Execute insertions over a pre-existing catalog"
    )
    base_parser.add_argument("--help", help="Shows this help message and exit", action="store_true")
    base_parser.add_argument("--logging", help="Enables logging", action="store_true")
    base_parser.add_argument("--show_sql", help="Prints the generated statements", action="store_true")
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--paradigm", type=str, choices=["1NF", "NF2_JSON"], required=True, help="Implementation paradigm for the design (either 1NF or NF2_JSON)", metavar="<prdgm>")
    base_parser.add_argument("--dbconf_file", type=str, help="Filename of the configuration file for DBMS connection", metavar="<conf>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--insert_file", type=Path, help="Filename of the json file containing the queries", metavar="<path>")
    base_parser.add_argument("--print_result", help="Prints the resulting rows", action="store_true")

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
        logging.info("Executing batch insertions")
        # Open and load the JSON file
        with open(args.insert_file, 'r') as file:
            insert_specs = json.load(file).get("insertions")
        for i, spec in enumerate(insert_specs):
            if True:
                print(f"-- Running insert specification {i}")
                inserts = cat.generate_insert_statement(spec)
                if args.show_sql:
                    print(r"--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
                    if len(inserts) > 1:
                        print(f"Number of insertions generated: {len(inserts)}")
                        print("First one is:")
                    print(inserts[0]+";")
                    print("--//////////////////////////////////////////")
                result = cat.execute(inserts[0])
                if args.print_result:
                    print(result)
        logging.info("END")
