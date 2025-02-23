import logging
from pathlib import Path
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.info("BEGIN")
    c = catalog.Catalog(base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    c.add_struct("Struct1", ["myClass1", "myClass2", "myAttribute1", "myAttribute3", "myRelationship1", "myAttribute2", "myAttribute4"])
    c.add_struct("Struct2", ["Struct1"])
    c.show_textual()
    #c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    if not c.is_correct():
        print("WARNING: The catalog is not correct!!!")
    c.show_graphical()
    logging.info("END")
