import logging
import sys
import argparse
from pathlib import Path
import json
from catalog import catalog, relational, normalized

# Path definitions
base_path = Path(__file__).parent
default_hypergraphs_path = base_path.joinpath("files/hypergraphs")
default_domains_path = base_path.joinpath("files/domains")
default_designs_path = base_path.joinpath("files/designs")

# Enable logging
logging.basicConfig(level=logging.INFO)


def create(args):
    logging.info("Creating domain: "+args.dom_spec)
    # Open and load the JSON file
    with open(args.dom_path.joinpath(args.dom_spec + ".json"), 'r') as file:
        domain = json.load(file)
    # Create and fill the catalog
    cat = catalog.Catalog()
    for cl in domain.get("classes"):
        cat.add_class(cl.get("name"), cl.get("prop"), cl.get("attr"))
    for ass in domain.get("associations", []):
        cat.add_association(ass.get("name"), ass.get("ends"))
    for gen in domain.get("generalizations", []):
        cat.add_generalization(gen.get("name"), gen.get("prop"), gen.get("superclass"), gen.get("subclasses"))
    return cat


def design(args):
    logging.info("Creating design: "+args.dsg_spec)
    # Open and load the JSON file
    with open(args.dsg_path.joinpath(args.dsg_spec + ".json"), 'r') as file:
        design = json.load(file)
    # Create and fill the catalog
    cat = normalized.Normalized(args.hg_path.joinpath("domain").joinpath(design.get("domain") + ".HyperNetX"))
    for h in design.get("hyperedges"):
        if h.get("kind") == "Struct":
            cat.add_struct(h.get("name"), h.get("anchor"), h.get("elements"))
        elif h.get("kind") == "Set":
            cat.add_set(h.get("name"), h.get("elements"))
        else:
            raise ValueError(f"Unknown kind of hyperedge '{h.get("kind")}'")
    return cat

def recover(args):
    logging.info("Recovering catalog: "+args.hypergraph)
    cat = normalized.Normalized(args.hg_path.joinpath(args.state).joinpath(args.hypergraph + ".HyperNetX"))
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
base_parser.add_argument("--create", help="Creates the catalog", action="store_true")
base_parser.add_argument("--verbose", help="Prints the generated statements", action="store_true")
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


if __name__ == "__main__":
    args = base_parser.parse_args()
    if len(sys.argv) == 1:
        base_parser.print_help()
    else:
        if args.create:
            consistent = False
            if args.state == "domain":
                c = create(args)
            elif args.state == "design":
                c = design(args)
            else:
                raise Exception("Unknown catalog type to be created")
        else:
            consistent = True
            c = recover(args)
        if args.text:
            c.show_textual()
        if args.check:
            if c.is_correct(design=(args.state == "design")):
                consistent = True
                print("The catalog is correct")
            else:
                consistent = False
                print("WARNING: The catalog is not consistent!!!")
        if consistent:
            if args.state == "domain":
                c.save(file=args.hg_path.joinpath(args.state).joinpath(args.dom_spec + ".HyperNetX"))
            elif args.state == "design":
                c.save(file=args.hg_path.joinpath(args.state).joinpath(args.dsg_spec + ".HyperNetX"))
                if args.translate:
                    c.create_schema(verbose=args.verbose)
            else:
                raise Exception("Unknown catalog type to be saved")
        else:
            print("The catalog is not consistent or its consistency was not checked (it was not saved)")
        if args.graph:
            c.show_graphical()
