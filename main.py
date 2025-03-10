import logging
from pathlib import Path
from catalog import pureRelational
import json

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.info("BEGIN")
    c = pureRelational.PostgreSQL(base_path.joinpath("files/hypergraphs/design/book-authors_pureRelational_test1.HyperNetX"))
    # c.show_textual()
    # #c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    # # if not c.is_correct(design=True):
    # #     print("WARNING: The catalog is not correct!!!")
    # # c.show_graphical()
    c.create_schema(verbose=True)
    logging.info("Executing batch queries")
    # Open and load the JSON file
    with open("files/queries/book-authors.json", 'r') as file:
        queries = json.load(file).get("queries")
    for query in queries:
        c.execute(query, verbose=True, onlyOneQuery=False)
    logging.info("END")
