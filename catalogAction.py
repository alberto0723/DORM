import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import catalog, relational, pureRelational

# Path definitions
base_path = Path(__file__).parent
default_hypergraphs_path = base_path.joinpath("files/hypergraphs")
default_schemas_path = base_path.joinpath("files/schemas")
default_designs_path = base_path.joinpath("files/designs")

# Enable logging
logging.basicConfig(level=logging.INFO)


def create(args):
    logging.info("Creating schema: "+args.sch_spec)
    # Open and load the JSON file
    with open(args.sch_path.joinpath(args.sch_spec + ".json"), 'r') as file:
        schema = json.load(file)
    # Create and fill the catalog
    cat = catalog.Catalog()
    for cl in schema.get("classes"):
        cat.add_class(cl.get("name"), cl.get("prop"), cl.get("attr"))
    for rel in schema.get("relationships"):
        cat.add_relationship(rel.get("name"), rel.get("ends"))
    return cat

def design(args):
    logging.info("Creating design: "+args.dsg_spec)
    # Open and load the JSON file
    with open(args.dsg_path.joinpath(args.dsg_spec + ".json"), 'r') as file:
        schema = json.load(file)
    # Create and fill the catalog
    cat = relational.Relational(args.hg_path.joinpath("schema").joinpath(schema.get("atoms") + ".HyperNetX"))
    for h in schema.get("hyperedges"):
        if h.get("kind") == "Struct":
            cat.add_struct(h.get("name"), h.get("anchor"), h.get("elements"))
        elif h.get("kind") == "Set":
            cat.add_set(h.get("name"), h.get("elements"))
        else:
            raise ValueError(f"Unknown kind of hyperedge '{h.get("kind")}'")
    return cat

def recover(args):
    logging.info("Recovering catalog: "+args.hypergraph)
    cat = pureRelational.PostgreSQL(args.hg_path.joinpath(args.state).joinpath(args.hypergraph + ".HyperNetX"))
    return cat

# ---------------------------------------------------------------------------- #
#                                configure argparse begin                      #
# ---------------------------------------------------------------------------- #
base_parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=True
)
subparsers = base_parser.add_subparsers(help="Kind of catalog")
base_parser.add_argument("--hg_path", type=Path, default=default_hypergraphs_path, help="Path to hypergraphs folder", metavar="<path>")
base_parser.add_argument("--hypergraph", type=str, default="input", help="File generated for the hypergraph with pickle", metavar="<hypergraph>")
base_parser.add_argument("--check", help="Checks correctness of the catalog", action="store_true")
base_parser.add_argument("--text", help="Shows the catalog in text format", action="store_true")
base_parser.add_argument("--graph", help="Shows the catalog in graphical format", action="store_true")
base_parser.add_argument("--create", help="Creates the schema", action="store_true")
base_parser.add_argument("--verbose", help="Prints the generated statements", action="store_true")
# ------------------------------------------------------------------------------ #
#                                   Subparsers                                   #
# ------------------------------------------------------------------------------ #
schema_parser = subparsers.add_parser("schema", help="Uses a hypergraph with only atoms", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
design_parser = subparsers.add_parser("design", help="Uses a hypergraph with a full design", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
# ---------------------------------------------------------------------- Schemas
schema_parser.set_defaults(state="schema")  # This is the subfolder where hypergraphs are stored
schema_parser.add_argument("--sch_path", type=Path, default=default_schemas_path, help="Path to schemas folder", metavar="<path>")
schema_parser.add_argument("--sch_spec", type=str, default="specification", help="Specification of the atomic schema in a JSON file", metavar="<schema>")
# ---------------------------------------------------------------------- Designs
design_parser.set_defaults(state="design")  # This is the subfolder where hypergraphs are stored
design_parser.add_argument("--dsg_path", type=Path, default=default_designs_path, help="Path to designs folder", metavar="<path>")
design_parser.add_argument("--dsg_spec", type=str, default="specification", help="Specification of the design in a JSON file", metavar="<design>")
design_parser.add_argument("--translate", help="Translates the design into the database (e.g., create tables)", action="store_true")


if __name__ == "__main__":
    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        if args.create:
            if args.state == "schema":
                c = create(args)
                c.save(file=args.hg_path.joinpath(args.state).joinpath(args.sch_spec + ".HyperNetX"))
            elif args.state == "design":
                c = design(args)
                c.save(file=args.hg_path.joinpath(args.state).joinpath(args.dsg_spec + ".HyperNetX"))
            else:
                raise Exception("Unknown catalog type to be created")
        else:
            c = recover(args)
        if args.text:
            c.show_textual()
        consistent = True
        if args.check:
            if c.is_correct(design=(args.state == "design")):
                print("The catalog is correct")
            else:
                consistent = False
                print("WARNING: The catalog is not consistent!!!")
        if args.state == "design" and consistent and args.translate:
            c.create_tables(verbose=args.verbose)
        if args.graph:
            c.show_graphical()
