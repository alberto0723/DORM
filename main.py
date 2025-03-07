import logging
from pathlib import Path
from catalog import pureRelational
import sqlparse

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.info("BEGIN")
    c = pureRelational.PostgreSQL(base_path.joinpath("files/hypergraphs/designs/book-authors_pureRelational_test3.HyperNetX"))
    # c.show_textual()
    # #c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    # # if not c.is_correct(design=True):
    # #     print("WARNING: The catalog is not correct!!!")
    # # c.show_graphical()
    c.create_tables()
    c.execute()
    sentences = sqlparse.parse("WHERE a=1 and 2=b or c='23';")
    print("Whole predicate: "+str(sentences[0]))
    where_clause = sentences[0].tokens[0]
    print("WHERE clause: "+str(where_clause))
    for atom in where_clause.tokens:
        if atom.ttype is None:
            print("Comparison: "+str(atom))
            for token in atom.tokens:
                if token.ttype is None:
                    print("Attribute: "+token.value)
    logging.info("END")
