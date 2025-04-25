import logging
import sys
import argparse
from pathlib import Path
from catalog import relational, first_normal_form

if __name__ == "__main__":
    # Path definitions
    base_path = Path(__file__).parent
    default_hypergraphs_path = base_path.joinpath("files/hypergraphs")
    default_domains_path = base_path.joinpath("files/domains")
    default_designs_path = base_path.joinpath("files/designs")

    # ---------------------------------------------------------------------------- #
    #                                configure argparse begin                      #
    # ---------------------------------------------------------------------------- #
    base_parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.HelpFormatter(prog, width=100), add_help=True,
        description="Perform basic actions to create and visualize a catalog"
    )
    subparsers = base_parser.add_subparsers(help="Kind of catalog")
    base_parser.add_argument("--logging", help="Enables logging", action="store_true")
    base_parser.add_argument("--show_sql", help="Prints the generated statements", action="store_true")
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--create", help="Creates the catalog (otherwise it would be loaded from either a file or DBMS)", action="store_true")
    base_parser.add_argument("--supersede", help="Overwrites the existing catalog during creation", action="store_true")
    base_parser.add_argument("--hg_path", type=Path, default=default_hypergraphs_path, help="Path to hypergraphs folder", metavar="<path>")
    base_parser.add_argument("--hypergraph", type=str, default="input", help="File generated for the hypergraph with pickle", metavar="<hg>")
    base_parser.add_argument("--dbms", type=str, default="postgresql", help="Kind of DBMS to connect to", metavar="<dbms>")
    base_parser.add_argument("--ip", type=str, default="localhost", help="IP address for the database connection", metavar="<ip>")
    base_parser.add_argument("--port", type=str, default="5432", help="Port for the database connection", metavar="<port>")
    base_parser.add_argument("--user", type=str, help="Username for the database connection", metavar="<user>")
    base_parser.add_argument("--password", type=str, help="Password for the database connection", metavar="<psw>")
    base_parser.add_argument("--dbname", type=str, default="postgres", help="Database name", metavar="<dbname>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--check", help="Checks correctness of the catalog", action="store_true")
    base_parser.add_argument("--text", help="Shows the catalog in text format", action="store_true")
    base_parser.add_argument("--graph", help="Shows the catalog in graphical format", action="store_true")

    # ------------------------------------------------------------------------------ #
    #                                   Subparsers                                   #
    # ------------------------------------------------------------------------------ #
    domain_parser = subparsers.add_parser("domain", help="Uses a hypergraph with only atoms", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    design_parser = subparsers.add_parser("design", help="Uses a hypergraph with a full design", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # ---------------------------------------------------------------------- Schemas
    domain_parser.set_defaults(state="domain")  # This is the subfolder where hypergraphs are stored
    domain_parser.add_argument("--dom_path", type=Path, default=default_domains_path, help="Path to domains folder", metavar="<path>")
    domain_parser.add_argument("--dom_spec", type=str, default="default_specification", help="Specification of the domain (only atomic elements) in a JSON file", metavar="<domain>")
    # ---------------------------------------------------------------------- Designs
    design_parser.set_defaults(state="design")  # This is the subfolder where hypergraphs are stored
    design_parser.add_argument("--dsg_path", type=Path, default=default_designs_path, help="Path to designs folder", metavar="<path>")
    design_parser.add_argument("--dsg_spec", type=str, default="default_specification", help="Specification of the design in a JSON file", metavar="<design>")
    design_parser.add_argument("--translate", help="Translates the design into the database schema (e.g., create tables)", action="store_true")
    design_parser.add_argument("--datasource", type=str, help="Database schema to migrate the data from", metavar="<dbschema>")

    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        if args.logging:
            # Enable logging
            logging.basicConfig(level=logging.INFO)
        else:
            logging.disable()
        if args.create:
            consistent = False
            if args.state == "domain":
                cat = first_normal_form.FirstNormalForm(dbms=args.dbms, ip=args.ip, port=args.port, user=args.user,
                                            password=args.password, dbname=args.dbname, dbschema=args.dbschema,
                                            supersede=True)
                cat.load_domain(args.dom_path.joinpath(args.dom_spec + ".json"))
            elif args.state == "design":
                cat = first_normal_form.FirstNormalForm(dbms=args.dbms, ip=args.ip, port=args.port, user=args.user,
                                                 password=args.password, dbname=args.dbname, dbschema=args.dbschema,
                                                 supersede=args.supersede)
                cat.load_design(args.dsg_path.joinpath(args.dsg_spec + ".json"))
            else:
                raise Exception("Unknown catalog type to be created")
        else:
            consistent = True
            if args.user is None or args.password is None:
                cat = first_normal_form.FirstNormalForm(args.hg_path.joinpath(args.state).joinpath(args.hypergraph + ".HyperNetX"))
            else:
                cat = first_normal_form.FirstNormalForm(dbms=args.dbms, ip=args.ip, port=args.port, user=args.user,
                                                        password=args.password, dbname=args.dbname, dbschema=args.dbschema)

        if args.text:
            cat.show_textual()
        if args.check and (args.user is None or args.password is None):
            if cat.is_correct(design=(args.state == "design"), show_warnings=not args.hide_warnings):
                consistent = True
                print("The catalog is correct")
            else:
                consistent = False
                print("WARNING: The catalog is not consistent!!!")
        if consistent or (args.user is not None and args.password is not None):
            if args.state == "domain":
                if args.user is None or args.password is None:
                    cat.save(file_path=args.hg_path.joinpath(args.state).joinpath(args.dom_spec + ".HyperNetX"))
                else:
                    cat.save(show_sql=args.show_sql)
            elif args.state == "design":
                if args.user is None or args.password is None:
                    cat.save(file_path=args.hg_path.joinpath(args.state).joinpath(args.dsg_spec + ".HyperNetX"))
                    if args.translate:
                        cat.create_schema(show_sql=args.show_sql, show_warnings=not args.hide_warnings)
                else:
                    cat.save(migration_source=args.datasource, show_sql=args.show_sql, show_warnings=not args.hide_warnings)
            else:
                raise Exception("Unknown catalog type to be saved")
        else:
            print("The catalog is not consistent or its consistency was not checked (it was not saved)")
        if args.graph:
            cat.show_graphical()
