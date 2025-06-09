from abc import ABC, abstractmethod
import logging
import warnings
import itertools
from IPython.display import display
import pandas as pd
import sqlalchemy
import hypernetx as hnx
import json
import re
from typing import Type, TypeVar

RelationalType = TypeVar('RelationalType', bound='Relational')

from .tools import custom_warning, drop_duplicates
from .catalog import Catalog

# Libraries initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("Relational")
warnings.showwarning = custom_warning


class Relational(Catalog, ABC):
    """
    This is a subclass of Catalog that implements the constraints specific for relational databases,
    as well as the management of the connection to the DBMS and corresponding interaction.
    It uses SQLAlchemy (https://www.sqlalchemy.org)
    """
    # This contains the connection to the database to store the catalog
    dbconf = None
    dbschema = None
    engine = None
    TABLE_NODES = '__dorm_catalog_nodes'
    TABLE_EDGES = '__dorm_catalog_edges'
    TABLE_INCIDENCES = '__dorm_catalog_incidences'
    TABLE_GUARDS = '__dorm_catalog_guards'

    def __init__(self, paradigm_name=None, file_path=None, dbconf=None, dbschema=None, supersede=False):
        # This print is just to avoid silly mistakes while testing, can eventually be removed
        print(f"*********************** {paradigm_name} ***********************")

        if dbconf is None:
            super().__init__(file_path=file_path)
            self.metadata["paradigm"] = paradigm_name
        else:
            if not ("dbms" in dbconf and "ip" in dbconf and "port" in dbconf and "user" in dbconf and "password" in dbconf and "dbname" in dbconf):
                raise ValueError(f"🚨 Missing required parameters to create the connection of the catalog in the DBMS (namely dbms, ip, port, user, password, and dbname) in {self.dbconf}")
            if dbschema is None:
                raise ValueError(f"🚨 Missing schema for the creation of the catalog in the DBMS")
            self.dbconf = dbconf
            self.dbschema = dbschema
            url = f"{self.dbconf['dbms']}://{self.dbconf['user']}:{self.dbconf['password']}@{self.dbconf['ip']}:{self.dbconf['port']}/{self.dbconf['dbname']}"
            logger.info(f"Creating database connection to '{self.dbschema}' at '{url}'")
            self.engine = sqlalchemy.create_engine(url, connect_args={"options": f"-csearch_path={self.dbschema}"})
            with self.engine.connect() as conn:
                if supersede:
                    logger.info(f"Creating schema '{dbschema}'")
                    conn.execute(sqlalchemy.text(f"DROP SCHEMA IF EXISTS {dbschema} CASCADE;"))
                    conn.execute(sqlalchemy.text(f"CREATE SCHEMA {dbschema};"))
                    conn.execute(sqlalchemy.text(f"COMMENT ON SCHEMA {dbschema} IS '"+"{}';"))
                    conn.commit()
                    # This creates either an empty hypergraph or reads it from a file
                    super().__init__(file_path=file_path)
                    self.metadata["paradigm"] = paradigm_name
                else:
                    catalog_tables = [self.TABLE_NODES, self.TABLE_EDGES, self.TABLE_INCIDENCES, self.TABLE_GUARDS]
                    if any(table not in sqlalchemy.inspect(self.engine).get_table_names() for table in catalog_tables):
                        ValueError(f"🚨 Missing required tables '{catalog_tables}' in the database with tables {sqlalchemy.inspect(self.engine).get_table_names()} in schema '{self.dbschema}' at '{self.dbconf}' (probably not a DORM schema)")
                    logger.info(f"Loading the catalog from the database connection")
                    df_nodes = pd.read_sql_table(self.TABLE_NODES, con=self.engine)
                    df_edges = pd.read_sql_table(self.TABLE_EDGES, con=self.engine)
                    df_incidences = pd.read_sql_table(self.TABLE_INCIDENCES, con=self.engine)
                    # There is a bug in the library, and the name of the property column for both nodes and edges is taken from "misc_properties_col"
                    H = hnx.Hypergraph(df_incidences, edge_col="edges", node_col="nodes", cell_weight_col="weight", misc_cell_properties_col="misc_properties",
                                       node_properties=df_nodes, node_weight_prop_col="weight", misc_properties_col="misc_properties",
                                       edge_properties=df_edges, edge_weight_prop_col="weight")
                    super().__init__(hypergraph=H)
                    self.guards = pd.read_sql_table(self.TABLE_GUARDS, con=self.engine)
                    # Get domain and design
                    result = conn.execute(sqlalchemy.text("SELECT n.nspname AS schema_name, d.description AS comment FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid WHERE n.nspname=:schema;"), {"schema": dbschema})
                    row = result.fetchone()
                    if not row:
                        ValueError(f"🚨 No metadata (in the form of a comment) found in the schema of the database (necessary to check domain and design origin), probably not a DORM schema")
                    self.metadata = json.loads(row.comment)
                    if "paradigm" in self.metadata:
                        if self.metadata["paradigm"] != paradigm_name:
                            raise ValueError(f"🚨 Expected paradigm in the current design of the catalog is {paradigm_name}, '{self.metadata['paradigm']}' found instead in '{self.dbschema}' at '{self.dbconf}'")
                    else:
                        self.metadata["paradigm"] = paradigm_name

    def save(self, file_path=None, migration_source_sch=None, migration_source_kind=None, show_sql=False) -> None:
        if file_path is not None:
            super().save(file_path)
        elif self.engine is not None:
            logger.info("Checking the catalog before saving it in the database")
            if self.is_consistent(design="design" in self.metadata):
                logger.info("Saving the catalog in the database")
                df_nodes = self.H.nodes.dataframe.copy()
                df_nodes['misc_properties'] = df_nodes['misc_properties'].apply(json.dumps)
                df_nodes.to_sql(self.TABLE_NODES, self.engine, if_exists='replace', index=True)
                df_edges = self.H.edges.dataframe.copy()
                df_edges['misc_properties'] = df_edges['misc_properties'].apply(json.dumps)
                df_edges.to_sql(self.TABLE_EDGES, self.engine, if_exists='replace', index=True)
                df_incidences = self.H.incidences.dataframe.copy()
                df_incidences['misc_properties'] = df_incidences['misc_properties'].apply(json.dumps)
                df_incidences.to_sql(self.TABLE_INCIDENCES, self.engine, if_exists='replace', index=True)
                self.guards.to_sql(self.TABLE_GUARDS, self.engine, if_exists='replace', index=True)
                self.create_schema(migration_source_sch=migration_source_sch, migration_source_kind=migration_source_kind, show_sql=show_sql)
                self.metadata["tables_created"] = "design" in self.metadata
                if migration_source_sch is not None and migration_source_kind is not None:
                    self.metadata["has_data"] = True
                with (self.engine.connect() as conn):
                    statement = f"COMMENT ON SCHEMA {self.dbschema} IS '{json.dumps(self.metadata)}';"
                    conn.execute(sqlalchemy.text(statement))
                    conn.commit()
            else:
                raise ValueError("🚨 An inconsistent catalog cannot be saved in the DBMS")
        else:
           raise ValueError("🚨 No connection to the database or file provided")

    def contains_set_including_transitivity_by_edge_name(self, edge_name, visited: list[str] = None) -> bool:
        if visited is None:
            visited = [edge_name]
        else:
            visited.append(edge_name)
        for node_name in self.get_outbounds().query('edges == "' + edge_name + '"').index.get_level_values("nodes"):
            if self.is_phantom(node_name):
                next_edge = self.get_edge_by_phantom_name(node_name)
                assert next_edge not in visited, f"☠️ Cycle of edges detected: {visited}"
                if self.is_set(next_edge):
                    return True
                elif self.is_struct(next_edge):
                    return self.contains_set_including_transitivity_by_edge_name(next_edge, visited)
        return False

    def is_consistent(self, design=False) -> bool:
        consistent = super().is_consistent(design)
        # Only needs to run further checks if the basic one succeeded
        if consistent:
            # --------------------------------------------------------------------- ICs about being a relational catalog in PostgreSQL

            # IC-Relational1:
            logger.info("Checking IC-Relational1")
            matches6_1 = self.get_inbound_firstLevel().index.get_level_values("edges")
            violations6_1 = self.get_sets()[self.get_sets().apply(lambda row: not row.name in matches6_1 and self.contains_set_including_transitivity_by_edge_name(row.name), axis=1)]
            if not violations6_1.empty:
                consistent = False
                print(f"🚨 IC-Relational1 violation: Sets cannot be nested due to not possible to nest 'jsonb_agg' in PostgreSQL")
                display(violations6_1)

        return consistent

    def create_schema(self, migration_source_sch=None, migration_source_kind=None, show_sql=False) -> None:
        """
        Creates the tables according to the design.
        :param migration_source_sch: Name of the database schema to migrate the data from.
        :param migration_source_kind: paradigm used in the database to migrate the data from (either 1NF or NF2_JSON).
        :param show_sql: Whether to print SQL statements or not.
        """
        logger.info("Creating schema")
        statements = self.generate_create_table_statements()
        if migration_source_sch is not None and migration_source_kind is not None:
            statements.extend(self.generate_migration_statements(migration_source_sch, migration_source_kind))
        statements.extend(self.generate_add_pk_statements())
        statements.extend(self.generate_add_fk_statements())
        if self.engine is not None:
            with self.engine.connect() as conn:
                for statement in statements:
                    if show_sql:
                        print(statement)
                    conn.execute(sqlalchemy.text(statement))
                conn.commit()

    @abstractmethod
    def generate_create_table_statements(self) -> list[str]:
        """
        Table creation depends on the concrete implementation strategy.
        :return: List of statements generated (one per table)
        """
        pass

    @abstractmethod
    def generate_migration_insert_statement(self, table_name: str, project: list[str], pattern: list[str], source: Type[RelationalType]) -> str:
        '''
        Generates insert statements to migrate data from a database to another.
        :param table_name: The table to be loaded.
        :param project: List of attributes to be loaded in that table.
        :param pattern: List of domain elements that determine the content of the table.
        :param source: The source catalog to get the data from.
        :return: The SQL statement that moves the data from one schema to another.
        '''
        pass

    def generate_migration_statements(self, migration_source_sch, migration_source_kind) -> list[str]:
        """
        Generates insertions to migrate data from one schema to another one.
        Both must be in the same database for it to work.
        :param migration_source_sch: Database schema to migrate the data from.
        :param migration_source_kind: paradigm used in the database to migrate the data from (either 1NF or NF2_JSON).
        :return: List of statements generated to migrate the data (one per struct)
        """
        source = migration_source_kind(dbconf=self.dbconf, dbschema=migration_source_sch)
        # Basic consistency checks between both source and target catalogs
        if source.metadata.get("domain", "") != self.metadata["domain"]:
            raise ValueError(
                f"🚨 Domain mismatch between source and target migration catalogs: {source.metadata.get('domain', '')} vs {self.metadata['domain']}")
        if source.metadata.get("design", "") == self.metadata["design"] and source.metadata.get("paradigm", "") == self.metadata["paradigm"]:
            warnings.warn("⚠️ Useless action (design and paradigm of source and target coincide in the migration)")
        if not source.metadata.get("tables_created", False):
            raise ValueError(f"🚨 The source {migration_source_sch} does not have tables to migrate (according to its metadata)")
        if not source.metadata.get("has_data", False):
            warnings.warn(f"⚠️ The source {migration_source_sch} does not have data to migrate (according to its metadata)")
        statements = []
        # For each table
        for table_name in self.get_inbound_firstLevel().index.get_level_values("edges"):
            logger.info(f"-- Generating data migration for table {table_name}")
            # For each struct in the table, we have to create a different extraction query
            for struct_name in self.get_struct_names_inside_set_name(table_name):
                project = [attr for attr, _ in self.get_struct_attributes(struct_name)]
                pattern = []
                node_list = self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes").to_list()
                # The node_list is extended inside the loop itself (kind of a recursive call)
                for node_name in node_list:
                    if self.is_class_phantom(node_name) or self.is_association_phantom(node_name):
                        pattern.append(self.get_edge_by_phantom_name(node_name))
                    if self.is_struct_phantom(node_name):
                        node_list.extend(self.get_outbound_struct_by_name(self.get_edge_by_phantom_name(node_name)).index.get_level_values("nodes").to_list())
                    if self.is_set_phantom(node_name):
                        node_list.extend(self.get_outbound_set_by_name(self.get_edge_by_phantom_name(node_name)).index.get_level_values("nodes").to_list())
                sentence = self.generate_migration_insert_statement(table_name, project, pattern, source)
                statements.append(sentence)
        return statements

    @abstractmethod
    def generate_add_pk_statements(self) -> list[str]:
        """
        PK generation depends on the concrete implementation strategy.
        :return: List of statements generated (one per table)
        """
        pass

    @abstractmethod
    def generate_add_fk_statements(self) -> list[str]:
        """
        FK generation depends on the concrete implementation strategy.
        :return: List of statements generated (one per FK)
        """
        pass

    def generate_joins(self, tables, query_classes, query_associations, alias_table, join_attr, schema_name: str = "", visited: dict[str, str] = None) -> str:
        """
        Find the connections between tables, according to the required classes and associations
        end generate the corresponding join clause
        Consider that the pattern of associations is acyclic, which means that we can add joins incrementally one by one
        There are four cases of potential joins
        1- a class in common between the current table and a visited one
        2- a class in the current table corresponding to a loose end in a visited one
        3- a loose end in the current table corresponding to a class in a visited one
        4- a loose end in the current table corresponding to another loose end in a visited one, and the corresponding class is not in the query
        :param tables: List of tables
        :param query_classes: List of classes to be provided by the query (can be empty)
        :param query_associations: List of associations to be provided by the query (can be empty)
        :param alias_table: Dictionary with the alias of every table in the query
        :param join_attr: Dictionary indicating where the domain attribute can be found in the table
        :param visited: Dictionary with all visited classes and from which table they are taken
        :param schema_name: Schema name to be concatenated in front of every table in the FROM clause
        :return: String containing the join clause of the tables received as parameter
        """
        # TODO: Consider that there could be more than one connected component (provided by the query) in the table
        #   (associations should be used to choose the right one)
        if visited is None:
            first_table = True
            visited = dict()
        else:
            first_table = False
        unjoinable = []
        associations = self.get_outbound_associations()[self.get_outbound_associations().index.get_level_values("edges").isin(query_associations)]
        query_superclasses = query_classes.copy()
        for class_name in query_classes:
            query_superclasses.extend(self.get_superclasses_by_class_name(class_name))
        query_superclasses = drop_duplicates(query_superclasses)
        while tables:
            # Take any table and find all its potentially connection points
            current_table = tables.pop(0)
            # Get potential attributes to plug the current table
            plugs = []  # This will contain pairs of attribute names that can be plugged (first belongs to the current table)
            # For every struct in the table
            struct_name_list = self.get_struct_names_inside_set_name(current_table)
            for struct_name in struct_name_list:
                node_name_list = self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes").to_list()
                for node_name in node_name_list:
                    if self.is_struct_phantom(node_name):
                        struct_name_list.append(self.get_edge_by_phantom_name(node_name))
                    elif self.is_set_phantom(node_name):
                        for hop_node_name in self.get_outbound_set_by_name(self.get_edge_by_phantom_name(node_name)).index.get_level_values("nodes").to_list():
                            node_name_list.append(hop_node_name)
                    elif self.is_class_phantom(node_name):
                        class_name = self.get_edge_by_phantom_name(node_name)
                        if class_name in query_superclasses:
                            # Any class in the query is a potential connection point per se
                            plugs.append((self.get_class_id_by_name(class_name), self.get_class_id_by_name(class_name)))
                            # Also, it can connect to a loose end if it participates in an association
                            for ass in associations.itertuples():
                                if self.get_edge_by_phantom_name(ass.Index[1]) in [class_name]+self.get_superclasses_by_class_name(class_name):
                                    plugs.append((self.get_class_id_by_name(class_name), ass.misc_properties["End_name"]))
                for end_name in self.get_loose_association_end_names_by_struct_name(struct_name):
                    for ass in associations.itertuples():
                        if end_name == ass.misc_properties["End_name"]:
                            # Loose end can connect to a class id
                            plugs.append((end_name, self.get_class_id_by_name(self.get_edge_by_phantom_name(ass.Index[1]))))
                            # A loose end in the current table can correspond to another loose end in a visited one, as soon as the corresponding class is not in the query
                            if self.get_edge_by_phantom_name(ass.Index[1]) not in query_classes:
                                for ass2 in associations.itertuples():
                                    if ass.Index[1] == ass2.Index[1]:
                                        plugs.append((end_name, ass2.misc_properties["End_name"]))
            # Check if the other ends of any of the connection points has been visited before
            joins = []
            laterals = ""
            for plug in plugs:
                if plug[1] in visited:
                    # TODO: Two identical lateral joins will be created if the same content of a set is used twice in joins
                    if 'jsonb_array_elements' in join_attr[plug[1]+"@"+visited[plug[1]]]:
                        # The split is assuming that there is a single parenthesis
                        laterals += "  JOIN LATERAL " + join_attr[plug[1]+"@"+visited[plug[1]]].replace("value", alias_table[visited[plug[1]]] + ".value").split(")")[0] + ") AS " + alias_table[visited[plug[1]]] + "_" + plug[1] + " ON TRUE\n"
                        lhs = alias_table[visited[plug[1]]] + "_" + plug[1] + join_attr[plug[1]+"@"+visited[plug[1]]].split(")")[1]
                    else:
                        lhs = alias_table[visited[plug[1]]]+"."+join_attr[plug[1]+"@"+visited[plug[1]]]
                    if 'jsonb_array_elements' in join_attr[plug[0]+"@"+current_table]:
                        # The split is assuming that there is a single parenthesis
                        laterals += "  JOIN LATERAL " + join_attr[plug[1]+"@"+current_table].replace("value", alias_table[current_table] + ".value").split(")")[0] + ") AS " + alias_table[current_table] + "_" + plug[1] + " ON TRUE\n"
                        rhs = alias_table[current_table] + "_" + plug[1] + join_attr[plug[1]+"@"+current_table].split(")")[1]
                    else:
                        rhs = alias_table[current_table]+"."+join_attr[plug[0]+"@"+current_table]
                    joins.append(lhs + "=" + rhs)
            if not first_table and not joins:
                unjoinable.append(current_table)
            else:
                tables += unjoinable
                unjoinable = []
                break
        # Duplication removal should not be necessary, but they appear because of multiple structs in a table
        joins = drop_duplicates(joins)
        # Get all the connection point in the table and mark them as visited
        for plug in plugs:
            visited[plug[0]] = current_table
        # Create the join clause
        join_clause = schema_name + current_table + " " + alias_table[current_table]
        if not first_table:
            if unjoinable:
                raise ValueError(f"🚨 Tables '{unjoinable}' are not joinable in the query")
            join_clause = laterals + "  JOIN "+join_clause+" ON "+" AND ".join(joins)
        if not tables:
            return join_clause
        else:
            return join_clause+'\n'+self.generate_joins(tables, query_classes, query_associations, alias_table, join_attr, schema_name, visited)

    def generate_query_statement(self, spec, explicit_schema=False) -> list[str]:
        """
        Generates SQL statements corresponding to the given query.
        It uses the bucket algorithm of query rewriting using views to generate all possible combinations of tables to
        retrieve the required classes and associations.
        :param spec: A JSON containing the select-project-join information.
        :param explicit_schema: Adds the dbschema to every table in the FROM clause.
        :return: A list with all possible SQL statements ascendantly sorted by the number of tables.
        """
        logger.info("Resolving query")
        if not self.metadata.get("tables_created", False):
            warnings.warn(f"⚠️ There are no tables to be queried in the schema '{self.dbschema}'")
        project_attributes, filter_attributes, pattern_edges, required_attributes, filter_clause = self.parse_query(spec)
        if explicit_schema:
            schema_name = self.dbschema + "."
        else:
            schema_name = ""
        # For each combination of tables, generate an SQL query
        sentences = []
        # Check if all classes in the pattern are in some struct
        # Some classes may be stored implicitly in their subclasses
        classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("edges").isin(pattern_edges)]
        implicit_classes = classes[~classes.index.get_level_values("nodes").isin(self.get_outbound_structs().index.get_level_values("nodes"))]
        # If all classes in the pattern are in some struct (i.e., no classes being implicitly stored in subclasses)
        if implicit_classes.empty:
            query_alternatives, class_names, association_names = self.create_bucket_combinations(pattern_edges, required_attributes)
            if len(query_alternatives) > 1:
                warnings.warn(f"⚠️ The query may be ambiguous, since it can be solved by using different combinations of tables: {query_alternatives}")
                # TODO: Can we check here if two combinations differ in only one table whose difference is by generalization? Then, we can prioritize taking first the query using the table with the subclass.
                #       In general, this can be complex to check, because of the exponential number of mappings between classes in the two queries and
                query_alternatives = sorted(query_alternatives, key=len)
            for tables_combination in query_alternatives:
                alias_table, proj_attr, join_attr, location_attr = self.get_aliases(tables_combination)
                conditions = [filter_clause] + self.get_discriminants(tables_combination, class_names)
                # We need to generate a subquery if there are filter unwinding jsons, because PostgreSQL does not allow this in the where clause
                # Thus, the internal query unwinds everything, and the external check the conditions on these attributes
                conditions_internal = []
                conditions_external = []
                for condition in conditions:
                    condition_attributes = self.parse_predicate(condition)
                    if any('jsonb_array_elements' in proj_attr[a] for a in condition_attributes):
                        conditions_external.append(condition)
                    else:
                        conditions_internal.append(condition)
                filter_attributes_external = drop_duplicates(self.parse_predicate(" AND ".join(conditions_external)))
                # Simple case of only one table required by the query
                if len(tables_combination) == 1:
                    # Build the FROM clause
                    from_clause = "\nFROM " + schema_name + tables_combination[0]
                # Case with several tables that require joins
                else:
                    # Build the FROM clause
                    from_clause = "\nFROM "+self.generate_joins(tables_combination, class_names, association_names, alias_table, join_attr, schema_name)
                    # Add the alias to all attributes, since there is more than one table now
                    for dom_attr_name, attr_proj in proj_attr.items():
                        if 'jsonb_array_elements' in attr_proj:
                            proj_attr[dom_attr_name] = attr_proj.replace("value", location_attr[dom_attr_name] + ".value")
                        else:
                            proj_attr[dom_attr_name] = location_attr[dom_attr_name] + "." + attr_proj
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join([proj_attr[a] + " AS " + a for a in project_attributes + filter_attributes_external]) + from_clause
                # Add the WHERE clause
                if conditions_internal != [] and conditions_internal != ["TRUE"]:
                    # Replace the domain name by the name in the table in the WHERE clause
                    for dom_attr_name, attr_proj in proj_attr.items():
                        conditions_internal = [s.replace(dom_attr_name, attr_proj) for s in conditions_internal]
                    sentence += "\nWHERE " + " AND ".join(f"({cond})" for cond in conditions_internal)
                if conditions_external:
                    sentence_with_filter = "SELECT " + ", ".join(project_attributes)
                    sentence_with_filter += "\nFROM (\n" + sentence + "\n) _"
                    sentence_with_filter += "\nWHERE " + " AND ".join(f"({cond})" for cond in conditions_external)
                else:
                    sentence_with_filter = sentence
                sentences.append(sentence_with_filter)
        # If some classes are implicitly stored in the current design (i.e. stored only in their subclasses)
        else:
            # We need to recursively do it one by one, so we only take the first implicit superclass
            superclass_name = implicit_classes.index[0][0]
            superclass_phantom_name = implicit_classes.index[0][1]
            # This deals with multiple generalizations at once. Most probably, it should deal one by one
            generalization = self.get_outbound_generalization_superclasses().reset_index(level="edges", drop=False).loc[superclass_phantom_name]
            subclasses = self.get_outbound_generalization_subclasses().loc[generalization.edges]
            subqueries = []
            for subclass_phantom_name in subclasses.index:
                new_query = spec.copy()
                # Replace the superclass by one of its subclasses in the query pattern
                new_query["pattern"] = [self.get_edge_by_phantom_name(subclass_phantom_name) if elem == superclass_name else elem for elem in new_query["pattern"]]
                subqueries.append(self.generate_query_statement(new_query, explicit_schema))
            # We need to combine it, because a query may be solved in many different ways
            for combination in list(itertools.product(*drop_duplicates(subqueries))):
                sentences.append("\nUNION\n".join(combination))
        return sentences

    @abstractmethod
    def generate_values_clause(self, table_name: str, data_values: dict[str, str]) -> str:
        """
        Values generation depends on the concrete implementation strategy.
        :param table_name: Name of the table
        :param data_values: Dictionary with pairs attribute name and value
        :return: String representation of the values to be inserted
        """
        pass

    def generate_insert_statement(self, spec) -> list[str]:
        """
        Generates SQL statements corresponding to the given insertion.
        :param spec: A JSON containing the pattern and data to be inserted.
        :return: A list with all possible SQL statements.
        """
        logger.info("Resolving insert")
        if not self.metadata.get("tables_created", False):
            # This is just a warning, because we are just generating the statement, not trying to execute it
            warnings.warn(f"⚠️ There are no tables in the schema '{self.dbschema}' to receive the insertions")
        data_values, pattern_edges = self.parse_insert(spec)
        # For each combination of tables, generate an SQL query
        sentences = []
        # Check if all classes in the pattern are in some struct
        # Some classes may be stored implicitly in their subclasses
        classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("edges").isin(pattern_edges)]
        implicit_classes = classes[~classes.index.get_level_values("nodes").isin(self.get_outbound_structs().index.get_level_values("nodes"))]
        # If all classes in the pattern are in some struct (i.e., no classes being implicitly stored in subclasses)
        for table_name in self.get_insertion_alternatives(pattern_edges, list(data_values.keys())):
            sentences.append("INSERT INTO " + self.generate_values_clause(table_name, data_values))
        # if implicit_classes.empty:
        #     for table_name in self.get_insertion_alternatives(pattern_edges, list(data_values.keys())):
        #         sentences.append("INSERT INTO " + self.generate_values_clause(table_name, data_values))
        # else:
        #     # TODO: Implement insertions in the presence of implicit classes
        #     assert False, "☠️ Insertions in implicit classes has not been implemented"
        return sentences

    def check_execution(self) -> None:
        if self.engine is None:
            raise ValueError("🚨 Queries cannot be executed without a connection to the DBMS")
        if not self.metadata.get("tables_created", False):
            print(f"🚨 There are no tables to be queried in the schema '{self.dbschema}'")

    def execute(self, statement) -> sqlalchemy.Sequence[sqlalchemy.Row] | int:
        """
        Executes a statement in the engine associated to the catalog.
        :param statement: SQL statement to be executed (has to be either INSERT or UPDATE).
        :return: Set of rows resulting from the query execution or 1 (if it was an insertion).
        """
        self.check_execution()
        with self.engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(statement))
            if statement.startswith("INSERT "):
                if not self.metadata.get("has_data", False):
                    self.metadata["has_data"] = True
                    statement = f"COMMENT ON SCHEMA {self.dbschema} IS '{json.dumps(self.metadata)}';"
                    conn.execute(sqlalchemy.text(statement))
                conn.commit()
                return 1
            elif statement.startswith("SELECT "):
                return result.fetchall()
            else:
                assert False, f"☠️ Unknown kind of statement (neither SELECT nor INSERT) to be executed: '{statement}'"

    def get_cost(self, query) -> float:
        """
        Estimates the cost of a query in the engine associated to the catalog.
        :param query: SQL query to be executed.
        :return: Unitless estimated cost.
        """
        self.check_execution()
        with self.engine.connect() as conn:
            first_row = conn.execute(sqlalchemy.text("EXPLAIN " + query)).fetchone()
        assert first_row is not None, "☠️ Empty access plan"
        # Extract the float (e.g., from "Execution Time: 0.456 ms")
        match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', str(first_row[0]))
        assert match is not None, f"☠️ Cost not found in the access plan of the query '{first_row}'"
        try:
            # match.group(1) gives you just the number (whatever in the parenthesis in the pattern)
            return float(match.group(1))
        except ValueError:
            raise ValueError(f"🚨 Cost parsing failed in the access plan of the query '{first_row}'")

    def get_time(self, query) -> float:
        """
        Estimates the execution time of a query in the engine associated to the catalog (requires true execution!!!).
        :param query: SQL query to be executed.
        :return: Estimated time in milliseconds.
        """
        self.check_execution()
        with self.engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"EXPLAIN (ANALYZE TRUE, SUMMARY TRUE) " + query)).fetchall()
        assert len(result) > 0, "☠️ Empty access plan"
        last_row = result[-1]
        # Extract the float (e.g., from "Execution Time: 0.456 ms")
        match = re.search(r'([\d.]+)\s*ms', str(last_row[0]))
        assert match is not None, f"☠️ Time not found in the access plan of the query '{last_row}'"
        try:
            # match.group(1) gives you just the number, without the " ms" suffix  (whatever in the parenthesis in the pattern)
            return float(match.group(1))
        except ValueError:
            raise ValueError(f"🚨 Cost parsing failed in the access plan of the query '{last_row}'")
