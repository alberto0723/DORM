import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent
default_designs_path = base_path.joinpath("files/designs")
default_hypergraphs_path = base_path.joinpath("files/hypergraphs")

# Enable logging
logging.basicConfig(level=logging.INFO)

def design(args):
    # Open and load the JSON file
    with open(args.dsg_path.joinpath(args.design + ".json"), 'r') as file:
        schema = json.load(file)
    # Create and fill the catalog
    cat = catalog.Catalog(args.hg_path.joinpath("schemas").joinpath(schema.get("atoms") + ".HyperNetX"))
    for h in schema.get("hyperedges"):
        if h.get("kind") == "Struct":
            cat.add_struct(h.get("name"), h.get("elements"))
        elif h.get("kind") == "Set":
            cat.add_set(h.get("name"), h.get("elements"))
        else:
            raise ValueError(f"Unknown kind of hyperedge '{h.get("kind")}'")
    return cat


# ---------------------------------------------------------------------------- #
#                                configure argparse begin                      #
# ---------------------------------------------------------------------------- #
base_parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=True
)
base_parser.add_argument("--dsg_path", type=Path, default=default_designs_path, help="Path to designs folder", metavar="<path>")
base_parser.add_argument("--design", type=str, default="input", help="Specification of the design in a JSON file", metavar="<schema>")
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
        c = design(args)
        if args.text:
            c.show_textual()
        if args.check:
            if c.is_correct(design=True):
                print("The catalog is correct")
                c.save(file=args.hg_path.joinpath("designs").joinpath(args.design + ".HyperNetX"))
            else:
                print("WARNING: The catalog is not correct!!!")
        else:
            c.save(file=args.hg_path.joinpath("designs").joinpath(args.design + ".HyperNetX"))
        if args.graph:
            c.show_graphical()
