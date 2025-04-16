# Dynamic Object-Relational Mapping (DORM)

This tool (based on [Moditha Hewasinghage](documents/Thesis-Moditha.pdf)'s PhD thesis) allows to generate database schemas and queries in a flexible way. 
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

#### Constraints
General:
- The domain cannot be empty
- All element names must be unique
- The domain must be connected

About classes:
- Classes must have an identifier (unless they are in a generalization hierarchy).
- Attributes belong only to one class.
- The number of different values of an attribute must be less or equal than the cardinality of its class.
- The number of different values of an identifier must coincide with the cardinality of its class.

About associations:
- Associations can only have two ends.
- Ends must have unique names.

About generalizations:
- Generalizations are acyclic.
- A class can have at most a superclass.
- The top of every generalization hierarchy must have an identifier.
- Only the top of a generalization hierarchy can have identifier.

### 2- Design
Structure of the database expressed in terms of *structs* and *sets*.
You can find an [exemplary design](files/designs/book-authors_normalized.json) corresponding to a normalized relational database.

The contents of the design files are as follows:
1. The name of the corresponding domain.
1. A list of hyperedges with a different name each, that can be of two kids
   1. ``Set``: Contains a list of elements (either classes or associations) contained in the set.
   1. ``Struct``: Contains a (potentially empty) list of elements (either classes or associations) contained in the struct, plus the ``anchor`` elements (also either classes or associations) which is the entry point (a.k.a. identifier) the struct.

#### Semantics
*Sets* are abstractions that represent tables, collections, arrays.

*Structs* are abstractions that contain different elements (a.k.a. classes, attributes and associations) from the domain, and represent kinds of entities, semantic types, etc.
A class contained in a struct means that at least its identifier belongs to it.

*Loose association ends* are those in the extremes of a chain of associations without a class.
They generate pointers (a.k.a. attributes) underneath, but do not indicate the whole class is contained in the struct.
All structs have an *anchor* that defines their identity.
The anchor of a struct inside a set generates an identifier composed of the identifiers of all classes in its anchor, together with the loose ends in there.

In the presence of a *generalization*, subclasses can directly access the attributes of the superclass.
Hence, the superclass does not need to be explicitly included in the struct of the subclass.

#### Constraints
General:
- All elements in the domain must be (potentially by transitivity) inside some set (except superclasses).
- All elements in the domain must be inside some struct (except superclasses).

About sets:
- Can contain structs inside, but not directly sets. 
- All structs in a set must share the same anchor attributes. 
However, some class must be different, and related by generalization.

About structs:
- Every struct must be ultimately contained either in a set or another struct.
- The anchor cannot be empty.
- Anchor and elements should be disjoint (actually all anchors are considered automatically elements of the struct).
- Elements and anchors in a struct can not contain two classes (directly or transitively) related by generalization.
- The anchor must be a connected subgraph of the domain.
- The elements and the anchor together must be a connected subgraph of the domain.
- There is only one path from every element in a struct to its anchor.
- Loose ends in the anchor must be loose ends in the struct.
  
### 3- Queries
Select-Project-Join expressions in terms of the domain concepts.
You can find some [query exemples](files/queries/book-authors.json) over the same domain.

The content of the query files is just a list of SPJ queries, whose structure is as follows:
1. ``project`` contains a list of attributes in the domain, which cannot be empty.
1. ``pattern`` contains a list of classes and associations in the domain, which cannot be empty.
1. ``filter`` contains a predicate (by now without parenthesis) in terms of the attributes of the domain.

#### Semantics
- The pattern may not contain any association.
- The pattern may not contain any class (meaning that only identifiers involved in an association are interesting to us).
- The use of generalizations is implicit.
In this case, subclasses are still identified for the corresponding queries and unions are generated accordingly.
Notice that, in case of many possible queries for every subclass, this could generate a combinatorial explosion.
- The attribute names in the projection are those of the attributes in the classes, except for the identifiers of classes not explicit in the pattern.
In this case, the corresponding association end should be used.
- Any element (class or association) present in the pattern entails that one table containing it will participate in the query, even if no attribute is used.

#### Constraints
- All elements in the three parts of a query must be connected (potentially by generalization).
- Generalizations cannot be explicit in the query.
- The pattern can not contain two classes (directly or transitively) related by generalization.

#### Known issues
Still, there are some queries that cannot be properly translated:
- If the result of the query would have twice the same table in the FROM clause, it will only appear once.
However, the links would still be generated twice.

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