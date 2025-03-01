import logging
from pathlib import Path
from catalog import relational

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.info("BEGIN")
    c = relational.Relational(base_path.joinpath("files/hypergraphs/designs/book-authors_pureRelational.HyperNetX"))
    c.show_textual()
    #c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    if not c.is_correct(design=True):
        print("WARNING: The catalog is not correct!!!")
    # c.show_graphical()
    logging.info("END")
