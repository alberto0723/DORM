import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent
default_schemas_path = base_path.joinpath("files/schemas")
default_hypergraphs_path = base_path.joinpath("files/hypergraphs")

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
base_parser.add_argument("--sch_path", type=Path, default=default_schemas_path, help="Path to schemas folder", metavar="<path>")
base_parser.add_argument("--schema", type=str, default="input", help="Specification of the atomic schema in a JSON file", metavar="<schema>")
base_parser.add_argument("--check", help="Checks correctness of the catalog", action="store_true")
base_parser.add_argument("--text", help="Enables textual output", action="store_true")
base_parser.add_argument("--graph", help="Enables graphical output", action="store_true")
base_parser.add_argument("--hg_path", type=Path, default=default_hypergraphs_path, help="Path to hypergraphs folder", metavar="<path>")
#create_parser.add_argument("-o", "--output", help="Output file name to be generated for the hypergraph with pickle", metavar="<filename>", default="test", type=str)

if __name__ == "__main__":
    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        c = create(args)
        if args.text:
            c.show_textual()
        if args.check:
            if c.is_correct(design=False):
                print("The catalog is correct")
                c.save(file=args.hg_path.joinpath("schemas").joinpath(args.schema + ".HyperNetX"))
            else:
                print("WARNING: The catalog is not correct!!!")
        else:
            c.save(file=args.hg_path.joinpath("schemas").joinpath(args.schema + ".HyperNetX"))
        if args.graph:
            c.show_graphical()
