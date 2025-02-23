import logging
from pathlib import Path
from catalog import catalog

# Path definitions
base_path = Path(__file__).parent

if __name__ == '__main__':
    logging.info("BEGIN")
    c = catalog.Catalog()
    c.add_class('myClass1',
                1000,
                [
                    {'name': 'myAttribute1', 'prop': {'DataType': 'Integer', 'Size': 4, 'DistinctVals': 100}},
                    {'name': 'myAttribute2', 'prop': {'DataType': 'Float', 'Size': 8, 'DistinctVals': 200}}
                ]
                )
    c.add_class('myClass2',
                1000,
                [
                    {'name': 'myAttribute3', 'prop': {'DataType': 'Integer', 'Size': 4, 'DistinctVals': 100}},
                    {'name': 'myAttribute4', 'prop': {'DataType': 'Float', 'Size': 8, 'DistinctVals': 200}}
                ]
                )
    c.add_relationship('myRelationship1', [{'name': 'myClass1', 'multiplicity': 5}, {'name': 'myClass2', 'multiplicity': 10}])
    c.show_textual()
    c.save(file=base_path.joinpath("files/hypergraphs/test.HyperNetX"))
    if not c.is_correct():
        print("WARNING: The catalog is not correct!!!")
    #c.show_graphical()
    logging.info("END")
