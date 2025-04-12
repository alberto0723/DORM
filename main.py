import logging
from pathlib import Path
from catalog import normalized
import json

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.disable()
    logging.info("BEGIN")
    #c = normalized.Normalized(base_path.joinpath("files/hypergraphs/design/book-authors_normalized_test3.HyperNetX"))
    #c = normalized.Normalized(base_path.joinpath("files/hypergraphs/design/book-authors_partitioned.HyperNetX"))
    #c = normalized.Normalized(base_path.joinpath("files/hypergraphs/design/artist-record-track_normalized_test1.HyperNetX"))
    c = normalized.Normalized(base_path.joinpath("files/hypergraphs/design/students-workers_normalized_OneTablePerSubclass.HyperNetX"))
    c.show_textual()
    # #c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    # if not c.is_correct(design=True):
    #     print("WARNING: The catalog is not correct!!!")
    # # c.show_graphical()
    c.create_schema(verbose=True)
    logging.info("Executing batch queries")
    # Open and load the JSON file
    #with open("files/queries/book-authors.json", 'r') as file:
    #with open("files/queries/artist-record-track.json", 'r') as file:
    with open("files/queries/students-workers.json", 'r') as file:
        queries = json.load(file).get("queries")
    for i, query in enumerate(queries):
        print("--*********************************** ", i)
        if True:
            for q in c.generate_SQL(query, verbose=False):
                print("--//////////////////////////////////////////")
                print(q)
    logging.info("END")
