import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import first_normal_form

if __name__ == "__main__":

    # Path definitions
    base_path = Path(__file__).parent

    # ---------------------------------------------------------------------------- #
    #                                configure argparse begin                      #
    # ---------------------------------------------------------------------------- #
    base_parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.HelpFormatter(prog, width=100), add_help=True,
        description="Execute queries over a pre-existing catalog"
    )
    base_parser.add_argument("--logging", help="Enables logging", action="store_true")
    base_parser.add_argument("--show_sql", help="Prints the generated statements", action="store_true")
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--dbms", type=str, default="postgresql", help="Kind of DBMS to connect to", metavar="<dbms>")
    base_parser.add_argument("--ip", type=str, default="localhost", help="IP address for the database connection", metavar="<ip>")
    base_parser.add_argument("--port", type=str, default="5432", help="Port for the database connection", metavar="<port>")
    base_parser.add_argument("--user", type=str, help="Username for the database connection", metavar="<user>")
    base_parser.add_argument("--password", type=str, help="Password for the database connection", metavar="<psw>")
    base_parser.add_argument("--dbname", type=str, default="postgres", help="Database name", metavar="<dbname>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--query_file", type=Path, help="Filename of the json file containing the queries", metavar="<path>")
    base_parser.add_argument("--print_rows", help="Prints the resulting rows", action="store_true")
    base_parser.add_argument("--print_counter", help="Prints the number of rows", action="store_true")
    base_parser.add_argument("--print_estimation", help="Prints the cost estimation of each query", action="store_true")

    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        if args.logging:
            # Enable logging
            logging.basicConfig(level=logging.INFO)
        else:
            logging.disable()
        logging.info("BEGIN")
        cat = first_normal_form.FirstNormalForm(dbms=args.dbms, ip=args.ip, port=args.port, user=args.user,
                                         password=args.password, dbname=args.dbname, dbschema=args.dbschema)
        logging.info("Executing batch queries")
        # Open and load the JSON file
        with open(args.query_file, 'r') as file:
            query_specs = json.load(file).get("queries")
        for i, spec in enumerate(query_specs):
            print(f"-- Running query specification {i}")
            if True:
                queries = cat.generate_sql(spec, explicit_schema=False, show_warnings=not args.hide_warnings)
                if args.show_sql:
                    print(r"--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
                    if len(queries) > 1:
                        print(f"Number of queries generated: {len(queries)}")
                        print("First one is:")
                    print(queries[0]+";")
                    print("--//////////////////////////////////////////")
                if args.print_estimation:
                    print("Estimated cost: ", cat.estimate_cost(queries[0]))
                rows = cat.execute(queries[0])
                if args.print_rows:
                    for row in rows:
                        print(row)
                if args.print_counter:
                    print(f"Number of rows: {len(rows)}")
        logging.info("END")
