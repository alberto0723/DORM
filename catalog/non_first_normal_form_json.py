import logging
import warnings
import pandas as pd
from IPython.display import display
import networkx as nx

from .relational import Relational
from .tools import drop_duplicates

# Library initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("NonFirstNormalFormJSON")


class NonFirstNormalFormJSON(Relational):
    """
    This is a subclass of Relational that implements the code generation as normalized in 1NF.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.info("Using a non-first normal form (NF2) implementation of the schema using JSON")
        # This print is just to avoid silly mistakes while testing, can eventually be removed
        print("*********************** NonFirstNormalFormJSON ***********************")

    def is_correct(self, design=False, show_warnings=True) -> bool:
        correct = super().is_correct(design, show_warnings=show_warnings)
        # Not worth to check anything if the more basic stuff is already not correct
        if correct:
            pass
        return correct

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        The same table is generated irrespectively of the attributes: one numerical key and one JSON value that will contain all the data.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info("-- Creating table " + table.Index[0])
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table.Index[0] + " (\n  key SERIAL PRIMARY KEY,\n  value JSONB\n  );"
            statements.append(sentence)
        return statements

    def generate_migration_statements(self, migration_source, show_warnings=True) -> list[str]:
        """
        Generates insertions to migrate data from one schema to another one.
        Both must be in the same database for it to work.
        :param migration_source: Database schema to migrate the data from.
        :param show_warnings: Whether to print warnings statements or not.
        :return: List of statements generated to migrate the data (one per struct)
        """
        statements = []
        source = NonFirstNormalFormJSON(dbms=self.dbms, ip=self.ip, port=self.port, user=self.user, password=self.password, dbname=self.dbname, dbschema=migration_source)
        self.check_migration(source, migration_source, show_warnings)
        firstlevels = self.get_inbound_firstLevel()
        # TODO
        # For each table
        # for table in firstlevels.itertuples():
        #     logger.info(f"-- Generating data migration for table {table.Index[0]}")
        #     # For each struct in the table, we have to create a different extraction query
        #     for struct_name in self.get_struct_names_inside_set_name(table.Index[0]):
        #         project = list(self.get_struct_attributes(struct_name).keys())
        #         pattern = []
        #         for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
        #             if self.is_class_phantom(incidence.Index[1]) or self.is_association_phantom(incidence.Index[1]):
        #                 pattern.append(self.get_edge_by_phantom_name(incidence.Index[1]))
        #         sentence = f"INSERT INTO {table.Index[0]}({", ".join(project)})\n" + source.generate_sql({"project": project, "pattern": pattern}, explicit_schema=True, show_warnings=show_warnings)[0] + ";"
        #         statements.append(sentence)
        return statements

    def generate_add_pk_statements(self) -> list[str]:
        """
        Generated the DDL to add PKs to the tables.
        Actually, in the case of NF2, it just adds uniqueness to the corresponding attributes inside the JSON, for the
        IDs of the classes in the anchor of the struct, plus the loose ends of the associations in the anchor.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info(f"-- Altering table {table.Index[0]} to add the PK (actually just uniqueness)")
            sentence = "CREATE UNIQUE INDEX pk_" + table.Index[0] + " ON " + table.Index[0]
            # Create the PK
            # All structs in a set must share the anchor attributes (IC-Design4), so we can take any of them
            struct_name = self.get_struct_names_inside_set_name(table.Index[0])[0]
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class_phantom(key):
                    key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                # If it is not a class, it is a loose end
                else:
                    key_list.append(key)
            assert key_list, f"☠️ Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined"
            # This is not considering that an anchor of a struct can be in a nested struct (only at first level)
            sentence += "((" + "), (".join(["value->>'" + k + "'" for k in key_list]) + "));\n"
            statements.append(sentence)
        return statements

    def generate_add_fk_statements(self, show_warnings=True) -> list[str]:
        """
        FKs cannot be generated over JSONB attributes in PostgreSQL.
        :param show_warnings: Whether to print warnings statements or not.
        :return: List of statements generated (one per table)
        """
        if show_warnings:
            warnings.warn("⚠️ Foreign keys cannot be defined over PostgreSQL JSONB attributes (hence, not implemented in NonFirstNormalFormJSON class)")
        return []

    def generate_joins(self, tables, query_classes, query_associations, alias_table, alias_attr, visited, schema_name="") -> str:
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
        :param alias_attr: Dictionary indicating from which table each attribute must be taken
        :param visited: Dictionary with all visited classes and from which table they are taken
        :param schema_name: Schema name to be concatenated in front of every table in the FROM clause
        :return: String containing the join clause of the tables received as parameter
        """
        # TODO: Consider that there could be more than one connected component (provided by the query) in the table
        #   (associations should be used to choose the right one)
        first_table = (visited == {})
        unjoinable = []
        associations = self.get_outbound_associations()[self.get_outbound_associations().index.get_level_values("edges").isin(query_associations)]
        query_superclasses = query_classes.copy()
        for class_name in query_classes:
            query_superclasses.extend(self.get_superclasses_by_class_name(class_name))
        query_superclasses = drop_duplicates(query_superclasses)
        # TODO
        # while tables:
        #     # Take any table and find all its potentially connection points
        #     current_table = tables.pop(0)
        #     # Get potential attributes to plug the current table
        #     plugs = []  # This will contain pairs of attribute names that can be plugged (first belongs to the current table)
        #     # For every struct in the table
        #     for struct_name in self.get_struct_names_inside_set_name(current_table):
        #         for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
        #             if self.is_class_phantom(incidence.Index[1]):
        #                 class_name = self.get_edge_by_phantom_name(incidence.Index[1])
        #                 if class_name in query_superclasses:
        #                     # Any class in the query is a potential connection point per se
        #                     plugs.append((self.get_class_id_by_name(class_name), self.get_class_id_by_name(class_name)))
        #                     # Also, it can connect to a loose end if it participates in an association
        #                     for ass in associations.itertuples():
        #                         if self.get_edge_by_phantom_name(ass.Index[1]) in [class_name]+self.get_superclasses_by_class_name(class_name):
        #                             plugs.append((self.get_class_id_by_name(class_name), ass.misc_properties["End_name"]))
        #         for end_name in self.get_loose_association_end_names_by_struct_name(struct_name):
        #             for ass in associations.itertuples():
        #                 if end_name == ass.misc_properties["End_name"]:
        #                     # Loose end can connect to a class id
        #                     plugs.append((end_name, self.get_class_id_by_name(self.get_edge_by_phantom_name(ass.Index[1]))))
        #                     # A loose end in the current table can correspond to another loose end in a visited one, as soon as the corresponding class is not in the query
        #                     if self.get_edge_by_phantom_name(ass.Index[1]) not in query_classes:
        #                         for ass2 in associations.itertuples():
        #                             if ass.Index[1] == ass2.Index[1]:
        #                                 plugs.append((end_name, ass2.misc_properties["End_name"]))
        #     # Check if the other ends of any of the connection points has been visited before
        #     joins = []
        #     for plug in plugs:
        #         if plug[1] in visited:
        #             joins.append(alias_table[visited[plug[1]]]+"."+plug[1]+"="+alias_table[current_table]+"."+plug[0])
        #     if not first_table and not joins:
        #         unjoinable.append(current_table)
        #     else:
        #         tables += unjoinable
        #         unjoinable = []
        #         break
        # # Duplication removal should not be necessary, but they appear because of multiple structs in a table
        # joins = drop_duplicates(joins)
        # # Get all the connection point in the table and mark them as visited
        # for plug in plugs:
        #     visited[plug[0]] = current_table
        # # Create the join clause
        # join_clause = schema_name + current_table + " " + alias_table[current_table]
        # if not first_table:
        #     if unjoinable:
        #         raise ValueError(f"🚨 Tables '{unjoinable}' are not joinable in the query")
        #     join_clause = "  JOIN "+join_clause+" ON "+" AND ".join(joins)
        # if not tables:
        #     return join_clause
        # else:
        #     return join_clause+'\n '+self.generate_joins(tables, query_classes, query_associations, alias_table, alias_attr, visited, schema_name)

    def generate_query_statement(self, spec, explicit_schema=False, show_warnings=True) -> list[str]:
        """
        Generates SQL statements corresponding to the given query.
        It uses the bucket algorithm of query rewriting using views to generate all possible combinations of tables to
        retrieve the required classes and associations
        :param spec: A JSON containing the select-project-join information
        :param explicit_schema: Adds the dbschema to every table in the FROM clause
        :param show_warnings: Whether to print warnings or not
        :return: A list with all possible SQL statements ascendantly sorted by the number of tables
        """
        logger.info("Resolving query")
        if show_warnings and not self.metadata.get("tables_created", False):
            warnings.warn("⚠️ There are no tables to be queried in the schema '{self.dbschema}'")
        project_attributes, filter_attributes, pattern_edges, required_attributes, filter_clause = self.parse_query(spec)
        if explicit_schema:
            schema_name = self.dbschema + "."
        else:
            schema_name = ""
        # For each combination of tables, generate an SQL query
        sentences = []
        # Check if all classes in the pattern are in some struct
        # Some classes may be stored implicitly by their subclasses
        # TODO
        # classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("edges").isin(pattern_edges)]
        # implicit_classes = classes[~classes.index.get_level_values("nodes").isin(self.get_outbound_structs().index.get_level_values("nodes"))]
        # # If all classes in the pattern are in some struct
        # if implicit_classes.empty:
        #     query_alternatives, class_names, association_names = self.create_bucket_combinations(pattern_edges, required_attributes)
        #     if len(query_alternatives) > 1:
        #         if show_warnings: warnings.warn(f"⚠️ The query may be ambiguous, since it can be solved by using different combinations of tables: {query_alternatives}")
        #         # TODO: Can we check here if two combinations differ in only one table whose difference is by generaliazation? Then, we can prioritize taking first the query using the table with the subclass.
        #         #       In general, this can be complex to check, because of the exponencial number of mappings between classes in the two queries and
        #         query_alternatives = sorted(query_alternatives, key=len)
        #     for tables_combination in query_alternatives:
        #         alias_table, alias_attr, location_attr = self.get_aliases(tables_combination)
        #         modified_filter_clauses = [filter_clause]+self.get_discriminants(tables_combination, class_names)
        #         # Simple case of only one table required by the query
        #         if len(tables_combination) == 1:
        #             # Build the SELECT clause
        #             sentence = "SELECT " + ", ".join([alias_attr[a] for a in project_attributes])
        #             # Build the FROM clause
        #             sentence += "\nFROM " + schema_name + tables_combination[0]
        #         # Case with several tables that require joins
        #         else:
        #             # Build the SELECT clause
        #             sentence = "SELECT " + ", ".join([location_attr[a]+"."+alias_attr[a] for a in project_attributes])
        #             # Build the FROM clause
        #             sentence += "\nFROM "+self.generate_joins(tables_combination, class_names, association_names, alias_table, location_attr,{}, schema_name)
        #             # Add alias to the WHERE clause if there is more than one table
        #             for attr in location_attr.items():
        #                 modified_filter_clauses = [s.replace(attr[0], attr[1]+"."+alias_attr[attr[0]]) for s in modified_filter_clauses]
        #         # Build the WHERE clause
        #         sentence += "\nWHERE " + " AND ".join(modified_filter_clauses)
        #         sentences.append(sentence)
        # # If some classes are implicitly stored in the current design (i.e. stored only in their subclasses)
        # else:
        #     # We need to recursively do it one by one, so we only take the first implicit superclass
        #     superclass_name = implicit_classes.index[0][0]
        #     superclass_phantom_name = implicit_classes.index[0][1]
        #     # This deals with multiple generalizations at once. Most probably, it should deal one by one
        #     generalization = self.get_outbound_generalization_superclasses().reset_index(level="edges", drop=False).loc[superclass_phantom_name]
        #     subclasses = self.get_outbound_generalization_subclasses().loc[generalization.edges]
        #     subqueries = []
        #     for subclass_phantom in subclasses.itertuples():
        #         new_query = spec.copy()
        #         # Replace the superclass by one of its subclasses in the query pattern
        #         new_query["pattern"] = [self.get_edge_by_phantom_name(subclass_phantom.Index) if elem == superclass_name else elem for elem in new_query["pattern"]]
        #         subqueries.append(self.generate_sql(new_query, explicit_schema, show_warnings=show_warnings))
        #     # We need to combine it, because a query may be solved in many different ways
        #     for combination in list(itertools.product(*drop_duplicates(subqueries))):
        #         sentences.append("\nUNION\n".join(combination))
        return sentences
