# Wise Object-Relational Mapping (WORM)

This tool (based on [Modithas Hewasinghage](documents/Thesis-Moditha.pdf)'s PhD thesis) allows to generate database schemas and queries in a flexible way. 
Thus, queries are expressed in terms of fixed domain concepts, and generated automatically depending on the current design.

There are three different inputs:

- **Domain**: Concepts to be represented in the database in terms of *classes*, *attributes* and *relationships*. 
You can find a basic example about [Books and Authors](files/domains/book-authors.json).
- **Design**: Structure of the database expressed in terms of *structs* and *sets*.
You can find an [exemplary design](files/designs/book-authors_pureRelational.json) corresponding to a normalized relational database.
- **Queries**: Select-Project-Join expressions in terms of the domain concepts.
You can find some [query exemples](files/queries/book-authors.json) over the same domain.

## Setup

It is also assumed that Python 3 and library [HyperNetX](https://github.com/pnnl/HyperNetX) (among others) are installed. 
Tested with Python 3.12.1 and the packages listed in [requirements.txt](requirements.txt) (generated with ```pip freeze```).

## Running

There are some tools available to facilitate usage and testing.

### catalogAction
This is a flexible scripting tool that allows to manage the catalog, including creating, storing (as a serialized hypergraph), visualizing (both textual and graphically) and translating it into SQL.

```
usage: catalogAction.py [-h] [--hg_path <path>] [--hypergraph <hypergraph>]
                        [--check] [--text] [--graph] [--create] [--verbose]
                        {domain,design} ...

positional arguments:
  {domain,design}       Kind of catalog
    domain              Uses a hypergraph with only atoms
    design              Uses a hypergraph with a full design

options:
  -h, --help            show this help message and exit
  --hg_path <path>      Path to hypergraphs folder (default: C:\Users\alberto.
                        abello\Documents\PycharmProjects\WORM\files\hypergraph
                        s)
  --hypergraph <hypergraph>
                        File generated for the hypergraph with pickle
                        (default: input)
  --check               Checks correctness of the catalog (default: False)
  --text                Shows the catalog in text format (default: False)
  --graph               Shows the catalog in graphical format (default: False)
  --create              Creates the catalog (default: False)
  --verbose             Prints the generated statements (default: False)
  ```
