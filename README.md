# Dynamic Object-Relational Mapping (DORM) 🛏️ 
This tool allows to generate database schemas and queries in a flexible and dynamic way. 
Thus, queries are expressed in terms of fixed domain concepts, and generated automatically depending on the current design.
Hence, mappings dynamically change as database schema design evolves.
Once the data are in the database powered by DORM, a new design also triggers the migration to the new schema.

## Project Information 📝
The motivation and need of evolving the database schema can be found in the paper:
[Enrico Gallinucci, Matteo Golfarelli, Wafaa Radwan, Gabriel Zarate, Alberto Abelló:
Impact Study of NoSQL Refactoring in SkyServer Database. DOLAP 2025: 1-11](https://ceur-ws.org/Vol-3931/paper1.pdf)
On the other hand, the overall idea of having a generic representation of the schema independent of the storage engine is based on [Moditha Hewasinghage](documents/Thesis-Moditha.pdf)'s PhD thesis.

## Getting Started 📘
This prototype of object-relational mapping implements conceptual schemas in databases.
Its dynamicity comes from the same domain information being potentially implementing in many different designs, without affecting the way user expresses the queries.
Thus, the system automatically translates those queries depending on the existing database schema underneath.
Moreover, the design can be modified and data is automatically migrated from one database schema to another.

For demonstration purposes, it has been implemented in PostgreSQL, but it could be generalized to any other relational DBMS or even non-relational document or key-value stores.
This is exemplified by offering two different implementation paradigms:
- *First Normal Form* (``1NF``): This option maps every domain attribute into a table attribute.
- *Non First Normal Form with JSON* (``NF2-JSON``): This option generates two attributes (namely an autoincrement integer *key* and a JSON *value*).
All attributes in the domain are then stored inside the *value*. 
By now, nested documents can be generated inside the *value*, but not sets.
Eventually they should be allowed.

Regarding the errors, the system can produce three different kinds:
- ⚠️ *Warning*: Some potential issue (e.g., a query can be translated in multiple ways), but the system is working fine (these can be disabled through the corresponding parameter ``hide_warnings``).
- 🚨 *Error*: A problem (most probably in the inputs) that prevents the system from working properly (e.g., giving rise to an inconsistent catalog).
- ☠️ *Assertion violation*: An internal error, just relevant for development (should hopefully never happen). 

There are three different inputs:

### 1- Domain 🌐
Concepts to be represented in the database in terms of *classes*, *attributes* and *relationships*. 
You can find a basic example about [Books and Authors](files/domains/book-authors-topic.json) and [Students and Workers](files/domains/students-workers.json).

The contents of the domain files are as follows:
1. A list of ``classes`` is required:
   - Every class has a ``name``, some properties including a counter of instances and a list of attributes.
      - Every attribute has a name and some properties: ``DataType``, ``Size``, ``DistincVals``, and ``Identifier``.
2. An optional list of binary ``associations``:
   - Every association has a ``name`` and two ends.
      - Every end has a ``class`` and some properties: ``End_name`` and ``Multiplicity``.
3. An optional list of ``generalizations``:
   - Every generalization has a ``name``, some properties (i.e., ``Disjoint`` and ``Complete``), a ``superclass``, and a list of ``subclasses'':
      - Every subclass has a ``class`` and some properties: ``Constraint`` (which is a predicate over the attributes of the class).

#### Constraints ⛓️
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

### 2- Design ✏️
Structure of the database expressed in terms of *structs* and *sets*.
You can find an [exemplary design](files/designs/1NF/book-authors.json) corresponding to a relational database in first normal form.

The contents of the design files are as follows:
1. The name of the corresponding domain.
2. A list of hyperedges with a different name each, that can be of two kids
   1. ``Set``: Contains a list of elements (either classes or associations) contained in the set.
   2. ``Struct``: Contains a (potentially empty) list of elements (either classes or associations) contained in the struct, plus the ``anchor`` elements (also either classes or associations) which is the entry point (a.k.a. identifier) the struct.

#### Semantics 🧠
*Sets* are abstractions that represent tables, collections, arrays.

*Structs* are abstractions that contain different elements (a.k.a. classes, attributes and associations) from the domain, and represent kinds of entities, semantic types, etc.
A class contained in a struct means that at least its identifier belongs to it.

*Loose association ends* are those in the extremes of a chain of associations without a class.
They generate pointers (a.k.a. attributes) underneath, but do not indicate the whole class is contained in the struct.
All structs have an *anchor* that defines their identity.
The anchor of a struct inside a set generates an identifier composed of the identifiers of all classes in its anchor, together with the loose ends in there.

In the presence of a *generalization*, subclasses can directly access the attributes of the superclass.
Hence, the superclass does not need to be explicitly included in the struct of the subclass.

#### Constraints ⛓️
General:
- All elements in the domain must be (potentially by transitivity) inside some set (except superclasses).
- All elements in the domain must be inside some struct (except superclasses).

About sets:
- Can contain one class or several structs inside, but not directly sets. 
- All structs in a set must share the same anchor attributes. 
However, some class must be different, and related by generalization.
- Sets cannot be nested due to not possible to nest 'jsonb_agg' in PostgreSQL (this is just an implementation issue). 
First level sets can contain other sets, but these cannot nest others.

About structs:
- Every struct must be ultimately contained either in a set or another struct.
- The anchor cannot be empty.
- Anchor and elements should be disjoint (actually all anchors are considered automatically elements of the struct).
- Elements and anchors in a struct can not contain two classes (directly or transitively) related by generalization.
- The anchor must be a connected subgraph of the domain.
- The elements and the anchor together must be a connected subgraph of the domain.
- Loose ends in the anchor must be loose ends in the struct.
- Discriminant attributes are mandatory if there are sibling classes by generalization.
- There is only one path from every element in a struct to its anchor.
- All elements in a struct are connected.
- All structs in a set must have the same attributes in the anchor.
- For all structs in a set, there must be a difference in a class in the anchor, which are related by generalization.
- If there are different structs in a set, and two of them differ in some sibling class in the anchor, the discriminant attribute must be provided in the struct.
- Any struct with a class with subclasses must contain the corresponding discriminants.
- All classes must appear linked to at least one anchor with minimum multiplicitity one. Such anchor must have minimum multiplicity one internally in the anchor, to guarantee that it does not miss any instance.
  
### 3- Queries 🔍
Select-Project-Join expressions in terms of the domain concepts.
You can find some [query examples](files/queries/book-authors.json) over the same domain.

The content of the query files is just a list of SPJ queries, whose structure is as follows:
1. ``project`` contains a list of attributes in the domain, which cannot be empty.
2. ``pattern`` contains a list of classes and associations in the domain, which cannot be empty.
3. ``filter`` contains a predicate (by now without parenthesis) in terms of the attributes of the domain.

#### Semantics 🧠
- The pattern may not contain any association.
- The pattern may not contain any class (meaning that only identifiers involved in an association are interesting to us).
- The use of generalizations is implicit.
In this case, subclasses are still identified for the corresponding queries and unions are generated accordingly.
Notice that, in case of many possible queries for every subclass, this could generate a combinatorial explosion.
- The attribute names in the projection are those of the attributes in the classes, except for the identifiers of classes not explicit in the pattern.
In this case, the corresponding association end should be used.
- Any element (class or association) present in the pattern entails that one table containing it will participate in the query, even if no attribute is used.

#### Constraints ⛓️
- All elements in the three parts of a query must be connected (potentially by generalization).
- Generalizations cannot be explicit in the query.
- The pattern can not contain two classes (directly or transitively) related by generalization.

#### Known issues ⚠️
Still, there are some queries that cannot be properly translated:
- If the result of the query would have twice the same table in the FROM clause, it will only appear once.
However, the links would still be generated twice.

## Setup ⚙️
It is assumed that Python 3 and library [HyperNetX](https://github.com/pnnl/HyperNetX) (among others) are installed. 
Tested with Python 3.12.1 and the packages listed in [requirements.txt](requirements.txt).

Some features can be tested with pure files, but full functionalities require a [PostgreSQL](https://www.postgresql.org) database connection.
We tested with version 14.

### Install Dependencies 🔗

With the virtual environment activated, install all required packages:

```bash
pip install -r requirements.txt
```

To update the list of dependencies later, run:

```bash
pip freeze > requirements.txt
```

There is an annoying bug in HyperNetX that constantly generates a warning. It can be avoided as explained in [BugFixForHyperNetX.txt](BugFixForHyperNetX.txt).

## Launching 🚀
There are two tools available to facilitate usage and testing.

### catalogAction ▶️
This is a flexible scripting tool that allows to manage the catalog, including creating, storing (either as a serialized hypergraph or in a DBMS), visualizing (both textual and graphically) and translating it into CREATE TABLE statements.
These can be directly executed in the DBMS.

```
usage: catalogAction.py [--help] [--logging] [--show_sql] [--hide_warnings] [--create] [--supersede] [--hg_path <path>]
                        [--hypergraph <hg>] [--dbconf_file <conf>] [--dbschema <sch>] [--check] [--text] [--graph]
                        {domain,design} ...

▶️ Perform basic actions to create and visualize a catalog

positional arguments:
  {domain,design}       Kind of catalog
    domain              Uses a hypergraph with only atoms
    design              Uses a hypergraph with a full design

options:
  --help                Shows this help message and exit
  --logging             Enables logging
  --show_sql            Prints the generated SQL statements
  --hide_warnings       Silences warnings
  --create              Creates the catalog (otherwise it would be loaded from either a file or DBMS)
  --supersede           Overwrites the existing catalog during creation
  --hg_path <path>      Path to hypergraphs folder
  --hypergraph <hg>     File generated for the hypergraph with pickle
  --dbconf_file <conf>  Filename of the configuration file for DBMS connection
  --dbschema <sch>      Database schema
  --check               Forces checking the consistency of the catalog when using files (when using a DBMS, the check is
                        always performed)
  --text                Shows the catalog in text format
  --graph               Shows the catalog in graphical format
------------------------------------------------------------------------------------------
usage: catalogAction.py domain [--dom_path <path>] [--dom_spec <domain>]

▶️ Acts on a catalog with only domain elements

options:
  --dom_path <path>    Path to domains folder
  --dom_spec <domain>  Specification of the domain (only atomic elements) in a JSON file
------------------------------------------------------------------------------------------
usage: catalogAction.py design --paradigm <prdgm> [--dsg_path <path>] [--dsg_spec <design>] [--translate]
                               [--src_sch <sch>] [--src_kind <prdgm>]

▶️ Acts on a catalog with both domain and design elements

options:
  --paradigm <prdgm>   Implementation paradigm for the design (either 1NF or NF2_JSON)
  --dsg_path <path>    Path to designs folder
  --dsg_spec <design>  Specification of the design in a JSON file
  --translate          Translates the design into the database schema (i.e., generates create tables) when files are
                       used (when using a DBMS, the translation is always performed)
  --src_sch <sch>      Database schema to migrate the data from
  --src_kind <prdgm>   Paradigm of the catalog to migrate the data from (either 1NF or NF2_JSON)
```

Its [automatically generated](https://diagram-generator.com) flow chart is in [CatalogAction.pdf](documents/Diagrams/CatalogAction.pdf).

### queryExecutor 🔍
This is a flexible scripting tool that allows to generate queries and execute them in a DBMS.

```
usage: queryExecutor.py [--help] [--logging] [--show_sql] [--hide_warnings] --paradigm <prdgm> [--dbconf_file <conf>] [--dbschema <sch>] [--query_file <path>]
                        [--print_rows] [--print_counter] [--print_cost] [--print_time]

🔍 Execute queries over a pre-existing catalog

options:
  --help                Shows this help message and exit
  --logging             Enables logging
  --show_sql            Prints the generated statements
  --hide_warnings       Silences warnings
  --paradigm <prdgm>    Implementation paradigm for the design (either 1NF or NF2_JSON)
  --dbconf_file <conf>  Filename of the configuration file for DBMS connection
  --dbschema <sch>      Database schema
  --query_file <path>   Filename of the json file containing the queries
  --print_rows          Prints the resulting rows
  --print_counter       Prints the number of rows
  --print_cost          Prints the unitless cost estimation of each query
  --print_time          Prints the estimated time of each query (in milliseconds)
```

Its [automatically generated](https://diagram-generator.com) flow chart is in [QueryExecutor.pdf](documents/Diagrams/QueryExecutor.pdf).

## Demo 💻

Understanding the contribution of this project is tricky, since it must be seen in the DBMS itself (everything automatically happens behind scenes).
To grasp the idea of what the prototype is actually doing, you can follow the next steps:

1. Create a database in [PostgreSQL](https://www.postgresql.org) on your own and include in a configuration file (e.g., `db_conf.txt`) all the connection fields (namely `dbms`, `ip`, `port`, `user`, `password`, and `dbname`).
You can find an example at [db_conf.example.txt](db_conf.example.txt)
2. Create a schema in the database to contain some data (these will be migrated later to other versions of this schema).
```bash
python catalogAction.py --db_conf db_conf.txt --dbschema <sourcesch> --show_sql --supersede --create design --paradigm 1NF --dsg_spec 1NF/book-authors_test2
```
3. Insert some testing data.
```SQL
INSERT INTO <sourcesch>.books_table VALUES (1, 'The Lord of the Rings', 'HarperCollins', 101, 'J.R.R. Tolkien', 133, 'M', 'U.K.');
INSERT INTO <sourcesch>.books_table VALUES (2, 'The Goods Themselves', 'Galaxy', 102, 'Isaac Asimov', 105, 'M', 'New York City, U.S.A.');
```
4. Indicate that the schema contains data by annotating it.
```SQL
DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = '<sourcesch>';

    EXECUTE format('COMMENT ON SCHEMA <sourcesch> IS %L', metadata || '{"data_migrated": true}');
END $$
```
5. Query the source schema.
```bash
python queryExecutor.py --db_conf db_conf.txt --dbschema <sourcesch> --paradigm 1NF --show_sql --print_rows --query_file files/queries/book-authors.json
```
6. Create a new schema containing a different design and migrate there the data contained in the source you just created before.
```bash
python catalogAction.py --db_conf db_conf.txt --dbschema <newsch> --show_sql --supersede --create design --paradigm 1NF --dsg_spec 1NF/book-authors_test1 --src_sch <sourcesch> --src_kind 1NF
```
7. Check the tables and contents of the new schema and compare against the source ones.
8. Query the new schema.
```bash
python queryExecutor.py --db_conf db_conf.txt --dbschema <newsch> --paradigm 1NF --show_sql --print_rows --query_file files/queries/book-authors.json
```

Notice that despite the source and the new schema being different, the query specification file we use is exactly the same, and the resulting tuples we get also coincide.
Nevertheless, the SQL queries being generated are different.

Steps 6 to 8 can be repeated for any design of domain [book-authors_1-1](files/domains/book-authors_1-1.json): 
- [1NF/book-authors_test](files/designs/1NF/book-authors.json)
- [1NF/book-authors_test1](files/designs/1NF/book-authors_test1.json) 
- [1NF/book-authors_test2](files/designs/1NF/book-authors_test2.json)
- [1NF/book-authors_test3](files/designs/1NF/book-authors_test3.json)

Moreover, the four designs can also be instantiated using `NF2_JSON` as paradigm (still keeping the source paradigm for data migration as `1NF`).
Actually, once created and having migrated data to it, any design can be used as source for the automatic migration of data to any other design, as soon as both share the same domain.
Other designs of the same domain violating 1NF are also available:
- [NF2/book-authors_test1](files/designs/NF2/book-authors_test1.json) 
- [NF2/book-authors_test2](files/designs/NF2/book-authors_test2.json)
- [NF2/book-authors_test3](files/designs/NF2/book-authors_test3.json)

### Experiments 🔬

Also, there are two batch files that run several combinations of designs and queries under folder `files`.
```bash
test_all_1NF.bat
test_all_NF2.bat
```

Notice that, in order to migrate data, some of the tests in those batch files require the creation before-hand of the source schema in the demo above with name `source`.
Also, a second source schema called `source2` should be created following the same steps as above, but the design [1NF/book-authors-topic](files/designs/1NF/book-authors-topic.json) and the corresponding data in [book-authors-topic](files/data/book-authors-topic.sql)