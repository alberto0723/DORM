# Dynamic Object-Relational Mapping (DORM)

This tool (based on [Modithas Hewasinghage](documents/Thesis-Moditha.pdf)'s PhD thesis) allows to generate database schemas and queries in a flexible way. 
Thus, queries are expressed in terms of fixed domain concepts, and generated automatically depending on the current design.
Hence, mappings dynamically change as database schema design evolves.

## Inputs

There are three different inputs:

### 1- Domain
Concepts to be represented in the database in terms of *classes*, *attributes* and *relationships*. 
You can find a basic example about [Books and Authors](files/domains/book-authors.json) and [Students and Workers]().

The contents of the domain files are as follows:
1. A list of ``classes`` is required:
   - Every class has a ``name``, some properties including a counter of instances and a list of attributes.
      - Every attribute has a name and some properties: ``DataType``, ``Size``, ``DistincVals``, and ``Identifier``.
1. An optional list of binary ``associations``:
   - Every association has a ``name`` and two ends.
      - Every end has a ``class`` and some properties: ``End_name`` and ``Multiplicity``.
1. An optional list of ``generalizations``:
   - Every generalization has a ``name``, some properties (i.e., ``Disjoint`` and ``Complete``), a ``superclass``, and a list of ``subclasses'':
      - Every subclass has a ``class`` and some properties: ``Constraint`` (which is a predicate over the attributes of the class).

#### Semantics and constraints
About classes:
- Classes must have an identifier (unless they are in a generalization hierarchy).
- Both class and attribute names must be unique.
About associations:
- Associations can only have two ends.
- Ends must have unique names.
About generalizations:
- Generalization names must be unique.
- A class can have at most a superclass.
- The top of every generalization hierarchy must have an identifier.
- Only the top of a generalization hierarchy can have identifier.

### 2- Design
Structure of the database expressed in terms of *structs* and *sets*.
You can find an [exemplary design](files/designs/book-authors_normalized.json) corresponding to a normalized relational database.

The contents of the design files are as follows:
1. 

#### Semantics and constraints

### 3- Queries
Select-Project-Join expressions in terms of the domain concepts.
You can find some [query exemples](files/queries/book-authors.json) over the same domain.

The content of the query files is just a list of SPJ queries, whose structure is as follows:
1. 

#### Semantics and constraints

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