import logging
import warnings
import sys
import argparse
from pathlib import Path
import catalog.tools as tools

import catalog.config as config
from catalog.first_normal_form import FirstNormalForm
from catalog.non_first_normal_form_json import NonFirstNormalFormJSON

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
        formatter_class=lambda prog: argparse.HelpFormatter(prog, width=120), add_help=False,
        description="▶️ Perform basic actions to create and visualize a catalog"
    )
    subparsers = base_parser.add_subparsers(help="Kind of catalog", dest="command")
    base_parser.add_argument("--help", help="Shows this help message and exit", action="store_true")
    base_parser.add_argument("--logging", help="Enables logging", action="store_true")
    base_parser.add_argument("--show_sql", help="Prints the generated SQL statements", action="store_true")
    base_parser.add_argument("--hide_warnings", help="Silences warnings", action="store_true")
    base_parser.add_argument("--create", help="Creates the catalog (otherwise it would be loaded from either a file or DBMS)", action="store_true")
    base_parser.add_argument("--supersede", help="Overwrites the existing catalog during creation", action="store_true")
    base_parser.add_argument("--hg_path", type=Path, default=default_hypergraphs_path, help="Path to hypergraphs folder", metavar="<path>")
    base_parser.add_argument("--hypergraph", type=str, default="input", help="File generated for the hypergraph with pickle", metavar="<hg>")
    base_parser.add_argument("--dbconf_file", type=str, help="Filename of the configuration file for DBMS connection", metavar="<conf>")
    base_parser.add_argument("--dbschema", type=str, default="dorm_default", help="Database schema", metavar="<sch>")
    base_parser.add_argument("--check", help="Forces checking the consistency of the catalog when using files (when using a DBMS, the check is always performed)", action="store_true")
    base_parser.add_argument("--text", help="Shows the catalog in text format", action="store_true")
    base_parser.add_argument("--graph", help="Shows the catalog in graphical format", action="store_true")

    # ------------------------------------------------------------------------------ #
    #                                   Subparsers                                   #
    # ------------------------------------------------------------------------------ #
    domain_parser = subparsers.add_parser("domain", help="Uses a hypergraph with only atoms",
                                          formatter_class=lambda prog: argparse.HelpFormatter(prog, width=120), add_help=False,
                                          description="▶️ Acts on a catalog with only domain elements")
    design_parser = subparsers.add_parser("design", help="Uses a hypergraph with a full design",
                                          formatter_class=lambda prog: argparse.HelpFormatter(prog, width=120), add_help=False,
                                          description="▶️ Acts on a catalog with both domain and design elements")
    # ---------------------------------------------------------------------- Schemas
    domain_parser.set_defaults(state="domain")  # This is the subfolder where hypergraphs are stored
    domain_parser.add_argument("--dom_path", type=Path, default=default_domains_path, help="Path to domains folder", metavar="<path>")
    domain_parser.add_argument("--dom_fmt", type=str, choices=["JSON", "XML"], default="JSON", help="Format of the domain specification file (either JSON or XML)", metavar="<fmt>")
    domain_parser.add_argument("--dom_spec", type=str, default="default_spec", help="Filename containing the specification of the domain (only atomic elements)", metavar="<domain>")
    # ---------------------------------------------------------------------- Designs
    design_parser.set_defaults(state="design")  # This is the subfolder where hypergraphs are stored
    design_parser.add_argument("--paradigm", type=str, choices=["1NF", "NF2_JSON"], required=True, help="Implementation paradigm for the design (either 1NF or NF2_JSON)", metavar="<prdgm>")
    design_parser.add_argument("--dsg_path", type=Path, default=default_designs_path, help="Path to designs folder", metavar="<path>")
    design_parser.add_argument("--dsg_fmt", type=str, choices=["JSON", "XML"], default="JSON", help="Format of the design specification file (either JSON or XML)", metavar="<fmt>")
    design_parser.add_argument("--dsg_spec", type=str, default="default_spec", help="Specification of the design in a JSON file", metavar="<design>")
    design_parser.add_argument("--translate", help="Translates the design into the database schema (i.e., generates create tables) when files are used (when using a DBMS, the translation is always performed)", action="store_true")
    design_parser.add_argument("--src_sch", type=str, help="Database schema to migrate the data from", metavar="<sch>")
    design_parser.add_argument("--src_kind", type=str, choices=["1NF", "NF2_JSON"], help="Paradigm of the catalog to migrate the data from (either 1NF or NF2_JSON)", metavar="<prdgm>")

    # Manually check for help before full parsing
    if len(sys.argv) == 1 or '--help' in sys.argv or '-h' in sys.argv:
        base_parser.print_help()
        print("------------------------------------------------------------------------------------------")
        domain_parser.print_help()
        print("------------------------------------------------------------------------------------------")
        design_parser.print_help()
        sys.exit(0)
    else:
        args = base_parser.parse_args()
        config.show_warnings = not args.hide_warnings
        if args.logging:
            # Enable logging
            logging.basicConfig(level=logging.INFO)
        else:
            logging.disable()
        # Create a new catalog
        if args.create:
            consistent = False
            if args.state == "domain":
                # Any subclass can be used here (not Relational, because it is abstract and cannot be instantiated)
                cat = FirstNormalForm(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema, supersede=True)
                cat.load_domain(args.dom_path.joinpath(args.dom_spec + "." + args.dom_fmt.lower()), args.dom_fmt.upper())
            elif args.state == "design":
                assert args.paradigm in ["1NF", "NF2_JSON"], f"☠️ Only paradigms allowed are 1NF and NF2_JSON"
                if args.paradigm == "1NF":
                    cat = FirstNormalForm(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema, supersede=args.supersede)
                else:
                    cat = NonFirstNormalFormJSON(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema, supersede=args.supersede)
                cat.load_design(args.dsg_path.joinpath(args.dsg_spec + "." + args.dsg_fmt.lower()), args.dsg_fmt.upper())
            else:
                raise Exception("Unknown catalog type to be created")
        # Load pre-existing catalog
        else:
            consistent = True
            assert args.paradigm in ["1NF", "NF2_JSON"], f"☠️ Only paradigms allowed are 1NF and NF2_JSON"
            if args.dbconf_file is None:
                if args.paradigm == "1NF":
                    cat = FirstNormalForm(args.hg_path.joinpath(args.state).joinpath(args.hypergraph + ".HyperNetX"))
                else:
                    cat = NonFirstNormalFormJSON(args.hg_path.joinpath(args.state).joinpath(args.hypergraph + ".HyperNetX"))
            else:
                if args.paradigm == "1NF":
                    cat = FirstNormalForm(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema)
                else:
                    cat = NonFirstNormalFormJSON(dbconf=tools.read_db_conf(args.dbconf_file), dbschema=args.dbschema)

        if args.text:
            cat.show_textual()
        if args.check and (args.dbconf_file is None):
            if cat.is_consistent(design=(args.state == "design")):
                consistent = True
                print("The catalog is consistent!")
            else:
                consistent = False
                warnings.warn("⚠️ The catalog is not consistent!!!")
        if consistent or (args.dbconf_file is not None):
            if args.state == "domain":
                if args.dbconf_file is None:
                    cat.save(file_path=args.hg_path.joinpath(args.state).joinpath(args.dom_spec + ".HyperNetX"))
                else:
                    cat.save(show_sql=args.show_sql)
            elif args.state == "design":
                if args.dbconf_file is None:
                    cat.save(file_path=args.hg_path.joinpath(args.state).joinpath(args.dsg_spec + ".HyperNetX"))
                    if args.translate:
                        # Translating without showing the SQL sentences does not make much sense when using file (show_sql should always be True in this case)
                        cat.create_schema(show_sql=args.show_sql)
                else:
                    assert args.src_kind is None or args.src_kind in ["1NF", "NF2_JSON"], f"☠️ Only source catalog paradigms allowed are 1NF and NF2_JSON"
                    if args.src_kind == "1NF":
                        cat.save(migration_source_sch=args.src_sch, migration_source_kind=FirstNormalForm, show_sql=args.show_sql)
                    else:
                        cat.save(migration_source_sch=args.src_sch, migration_source_kind=NonFirstNormalFormJSON, show_sql=args.show_sql)
            else:
                raise Exception("Unknown catalog type to be saved")
        else:
            print("The catalog is not consistent or its consistency was not checked (it was not saved)")
        if args.graph:
            cat.show_graphical()
