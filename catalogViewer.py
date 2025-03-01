import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent
default_hypergraphs_path = base_path.joinpath("files/hypergraphs")

# Enable logging
logging.basicConfig(level=logging.INFO)


def recover(args):
    #self.config.input_path.joinpath(filename + ".HyperNetX")
    cat = catalog.Catalog(args.hg_path.joinpath(args.dir).joinpath(args.hypergraph + ".HyperNetX"))
    return cat

# ---------------------------------------------------------------------------- #
#                                configure argparse begin                      #
# ---------------------------------------------------------------------------- #
base_parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=True
)
subparsers = base_parser.add_subparsers(help="Kind of viewer")
base_parser.add_argument("--hg_path", type=Path, default=default_hypergraphs_path, help="Path to hypergraphs folder", metavar="<path>")
base_parser.add_argument("--hypergraph", type=str, default="input", help="File generated for the hypergraph with pickle", metavar="<hypergraph>")
base_parser.add_argument("--check", help="Checks correctness of the catalog", action="store_true")
base_parser.add_argument("--text", help="Enables textual output", action="store_true")
base_parser.add_argument("--graph", help="Enables graphical output", action="store_true")
# ------------------------------------------------------------------------------ #
#                                   Subparsers                                   #
# ------------------------------------------------------------------------------ #
design_parser = subparsers.add_parser("design", help="Visualizes a design", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
design_parser.set_defaults(dir="designs")
schema_parser = subparsers.add_parser("schema", help="Visualizes a hypergraph with only atoms", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
schema_parser.set_defaults(dir="schemas")

if __name__ == "__main__":
    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        c = recover(args)
        if args.text:
            c.show_textual()
        if args.check:
            if c.is_correct(design=(args.dir == "designs")):
                print("The catalog is correct")
            else:
                print("WARNING: The catalog is not correct!!!")
        if args.graph:
            c.show_graphical()

"""
import logging
import argparse
from pathlib import Path
import json
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent
default_schemas_path = base_path.joinpath("files/schemas")
default_hypergraphs_path = base_path.joinpath("files/hypergraphs/onlyAtoms")

# Enable logging
logging.basicConfig(level=logging.INFO)


def create(args):
    # Open and load the JSON file
    with open(args.sch_path.joinpath(args.schema + ".json"), 'r') as file:
        schema = json.load(file)
    # Create and fill the catalog
    cat = catalog.Catalog()
    for cl in schema.get("classes"):
        cat.add_class(cl.get("name"), cl.get("prop"), cl.get("attr"))
    for rel in schema.get("relationships"):
        cat.add_relationship(rel.get("name"), rel.get("ends"))
    return cat

# ---------------------------------------------------------------------------- #
#                                configure argparse begin                      #
# ---------------------------------------------------------------------------- #
base_parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=True
)
subparsers = base_parser.add_subparsers(help="Kind of loader")
base_parser.add_argument(
    "--hg_path",
    metavar="<path>",
    help="Path to hypergraphs folder",
    default=default_hypergraphs_path,
    type=Path,
)
base_parser.add_argument("--check", help="Checks correctness of the catalog", action="store_true")
base_parser.add_argument("--text", help="Enables textual output", action="store_true")
base_parser.add_argument("--graph", help="Enables graphical output", action="store_true")
# ---------------------------------------------------------------------------- #
#                            to recover argument parsing                       #
# ---------------------------------------------------------------------------- #
subparsers = base_parser.add_subparsers(help="Kind of loader")
recover_parser = subparsers.add_parser("recover", help="Creates a catalog using a previously pickled hypergraph", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
create_parser = subparsers.add_parser("create", help="Creates a catalog from a JSON file specification", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
base_parser.add_argument(
    "--sch_path",
    metavar="<path>",
    help="Path to schemas folder",
    default=default_schemas_path,
    type=Path,
)
create_parser.add_argument("--schema", help="Specification of the atomic schema in a JSON file", default="input", type=str)
#create_parser.add_argument("-o", "--output", help="Output file name to be generated for the hypergraph with pickle", metavar="<filename>", default="test", type=str)
create_parser.set_defaults(func=create)

if __name__ == "__main__":
    args = base_parser.parse_args()
    if not hasattr(args, "func"):
        base_parser.print_help()
    else:
        c = args.func(args)
        if args.text:
            c.show_textual()
        if args.check:
            if c.is_correct():
                print("The catalog is correct")
                if args.func == create:
                    c.save(file=args.hg_path.joinpath(args.schema + ".HyperNetX"))
            else:
                print("WARNING: The catalog is not correct!!!")
        else:
            if args.func == create:
                c.save(file=args.hg_path.joinpath(args.schema + ".HyperNetX"))
        if args.graph:
            c.show_graphical()
"""
