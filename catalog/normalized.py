import logging
import itertools
import pandas as pd
from IPython.display import display
import networkx as nx
import sqlalchemy

from .relational import Relational
from .tools import combine_tables, drop_duplicates

# Library initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("Normalized")

class Normalized(Relational):
    """
    This is a subclass of Relational that implements the code generation as normalized in 1NF
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check_to_one(self, path):
        correct = True
        for i, current in enumerate(path):
            if self.is_association(current):
                if len(path) > i+1:
                    properties = self.H.get_cell_properties(current, path[i+1])
                    if "Multiplicity" in properties:
                        correct = correct and (properties.get("Multiplicity") <= 1)
                    else:
                        raise ValueError(f"Checking multiplicity: Multiplicity not provided for association '{current}-{path[i+1]}'")
        return correct

    def is_correct(self, design=False, verbose=True):
        correct = super().is_correct(design, verbose)
        if correct:
            # ---------------------------------------------------------------- ICs about being a normalized catalog
            # IC-Normalized1: All associations from the anchor of a struct must be to one (or less)
            logger.info("Checking IC-Normalized1")
            firstlevels = self.get_inbound_firstLevel()
            # For each table
            for table in firstlevels.itertuples():
                for struct in self.get_outbound_set_by_name(table.Index[0]).itertuples():
                    struct_name = self.get_edge_by_phantom_name(struct.Index[1])
                    members = self.get_outbound_struct_by_name(struct_name).index.get_level_values(1).tolist()
                    anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                    dont_cross = self.get_anchor_associations_by_struct_name(struct_name)
                    restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                    bipartite = restricted_struct.H.remove_edges(dont_cross).bipartite()
                    for anchor in anchor_points:
                        for member in set(members)-set(anchor_points):
                            if self.is_class_phantom(member):
                                paths = list(nx.all_simple_paths(bipartite, source=anchor, target=member))
                                if len(paths) == 1:
                                    if not self.check_to_one(paths[0]):
                                        correct = False
                                        print(f"IC-PureRelational1 violation: A struct '{struct_name}' has an unacceptable path (not to one) '{paths[0]}'")
                                elif len(paths) > 1:
                                    raise ValueError(f"IC-PureRelational1: Something went wrong in '{struct_name}' on finding more than one path '{paths}' between '{anchor}' and '{member}'")
        return correct

    def get_struct_attributes(self, struct_name):
        """
        This generates the correspondence between attribute names in a table and their corresponding attribute.
        It is necessary to do it to consider foreign keys.
        :param struct_name:
        :return: A dictionary with the pairs "intable_name" and "domain_name" in the hypergraph attribute
        """
        elements = self.get_outbound_struct_by_name(struct_name)
        loose_ends = self.get_loose_association_end_names_by_struct_name(struct_name)
        # For each element in the table
        attribute_dicc = {}
        for elem in elements.itertuples():
            if self.is_attribute(elem.Index[1]):
                attribute_dicc[elem.Index[1]] = elem.Index[1]
            elif self.is_class_phantom(elem.Index[1]):
                attribute_dicc[
                    self.get_class_id_by_name(self.get_edge_by_phantom_name(elem.Index[1]))] = (
                    self.get_class_id_by_name(self.get_edge_by_phantom_name(elem.Index[1])))
            elif self.is_association_phantom(elem.Index[1]):
                ends = self.get_outbound_association_by_name(self.get_edge_by_phantom_name(elem.Index[1]))
                for end in ends.itertuples():
                    if end.misc_properties["End_name"] in loose_ends:
                        attribute_dicc[end.misc_properties['End_name']] = (
                            self.get_class_id_by_name(self.get_edge_by_phantom_name(end.Index[1])))
            elif self.is_generalization_phantom(elem.Index[1]):
                pass
            else:
                raise ValueError(f"Some element in struct '{struct_name}' is not expected: '{elem.Index[1]}'")
        return attribute_dicc

    def generate_create_table_statements(self, verbose=False):
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        One or more structs are expected inside the set, but all of them should generate the same attributes.
        Inside each table, there are all the attributes in the struct, plus the IDs of the classes, plus the loose ends
        of the associations.
        The primary key of the table is composed by the IDs of the classes in the anchor of the struct, plus the loose
        ends of the associations in the anchor.
        :param verbose: Indicates if the DDL should be printed
        :return: list of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info("-- Creating table " + table.Index[0])
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table.Index[0] + " (\n"
            # Get all the attributes in all the structs
            attribute_dicc = {}
            for struct_name in self.get_struct_names_inside_set_name(table.Index[0]):
                attribute_dicc.update(self.get_struct_attributes(struct_name))
            # Add all the attributes to the create table sentence
            for attr_alias, attr_name in attribute_dicc.items():
                attribute = self.get_attribute_by_name(attr_name)
                sentence += "  " + attr_alias
                if attribute["misc_properties"].get("DataType") == "String":
                    sentence += " VarChar(" + str(attribute["misc_properties"].get("Size")) + "),\n"
                else:
                    sentence += " " + attribute["misc_properties"].get("DataType") + ",\n"
            # Create the PK
            # All structs in a set must share the anchor attributes (IC-Design4), so we can take any of them
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class_phantom(key):
                    key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                # If it is not a class, it is a loose end
                else:
                    key_list.append(key)
            if not key_list:
                raise ValueError(
                    f"Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined")
            clause_pk = "  PRIMARY KEY (" + ",".join(key_list) + ")\n"
            sentence += clause_pk + "  );"
            if verbose:
                print(sentence)
            statements.append(sentence)
        return statements

    def create_schema(self, verbose=False):
        '''
        Creates the tables in the design.
        :param verbose: Indicates if the DDL should be printed
        '''
        logger.info("Creating tables")
        statements = self.generate_create_table_statements()
        if self.engine is not None:
            with self.engine.connect() as conn:
                for statement in statements:
                    conn.execute(sqlalchemy.text(statement))
                conn.commit()

    def create_bucket_combinations(self, pattern, required_attributes):
        """
        For each required domain elements, create a bucket with all the tables where it can come from.
        Then, combine all these buckets to cover all elements
        :param pattern: List of classes and associations in the query
        :param required_attributes: List of attributes used in the query
        :return: List of combinations of tables covering all the required elements
        :return: List of classes required
        :return: List of associations required
        """
        tables = []
        classes = []
        associations = []
        for elem in pattern:
            # Find the tables (aka fist level elements) where the element belongs
            hierarchy = [elem]+self.get_superclasses_by_class_name(elem, [])
            hierarchy_phantoms = [self.get_phantom_of_edge_by_name(c) for c in hierarchy]
            second_levels = self.get_outbound_structs()[self.get_outbound_structs().index.get_level_values('nodes').isin(hierarchy_phantoms)]
            inbounds = self.get_inbound_structs()
            inbounds["nodes"] = inbounds.index.get_level_values('nodes')
            second_level_phantoms = pd.merge(second_levels, inbounds, on="edges", how="inner")["nodes"]
            # No need to check if they are at first level, because sets always are (no nested structures are allowed)
            first_levels = self.get_outbound_sets()[self.get_outbound_sets().index.get_level_values('nodes').isin(second_level_phantoms)].index.get_level_values("edges").tolist()
            # Sorting the list of tables is important to drop duplicates later
            first_levels.sort()
            # Split join edges into classes and associations
            if self.is_association(elem):
                associations.append(elem)
                # If the element is an association, any table containing it is an option
                tables.append(first_levels)
            if self.is_class(elem):
                classes.append(elem)
                # If it is a class, the id always belongs to the table, hence we add it even if not required
                current_attributes = []
                # Take the required attributes in the class that are in the current table
                for class_name in hierarchy:
                    current_attributes.extend(self.get_outbound_class_by_name(class_name)[self.get_outbound_class_by_name(class_name).index.get_level_values('nodes').isin(required_attributes)].index.get_level_values('nodes').tolist())
                if self.get_class_id_by_name(elem) not in current_attributes:
                    current_attributes.append(self.get_class_id_by_name(elem))
                # If it is a class, it may be vertically partitioned
                # We need to generate joins of these tables that cover all required attributes one by one
                # Get the tables independently for every attribute in the class
                for attr in current_attributes:
                    if not self.is_id(attr) or len(current_attributes) == 1:
                        attr_tables = []
                        for table in first_levels:
                            kind = self.H.get_cell_properties(table, attr, "Kind")
                            if kind is not None:
                                attr_tables.append(table)
                        if attr_tables:
                            tables.append(attr_tables)
        # Generate combinations of the buckets of each element to get the combinations that cover all of them
        return combine_tables(drop_duplicates(tables)), classes, associations

    def get_aliases(self, tables_combination):
        alias_table = {}
        alias_attr = {}
        location_attr = {}
        # The list of tables is reversed, so that the first appearance of an attribute prevails (seems more logical)
        for index, table in enumerate(reversed(tables_combination)):
            # -- Determine the aliases of tables and required attributes
            alias_table[table] = self.config.prepend_table_alias + str(len(tables_combination) - index)
            for struct_name in self.get_struct_names_inside_set_name(table):
                for intable_name, domain_name in self.get_struct_attributes(struct_name).items():
                    location_attr[intable_name] = alias_table[table]
                    alias_attr[intable_name] = intable_name
                associations = self.get_inbound_associations()[
                    self.get_inbound_associations().index.get_level_values("nodes").isin(
                        pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_inbound_associations(),
                                 on="nodes", how="inner").index)]
                classes = self.get_inbound_classes()[
                    self.get_inbound_classes().index.get_level_values("nodes").isin(
                        pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_inbound_classes(),
                                 on="nodes", how="inner").index)]
                association_ends = self.get_outbound_associations()[
                    (self.get_outbound_associations().index.get_level_values("edges").isin(
                        associations.index.get_level_values("edges"))) & (
                        self.get_outbound_associations().index.get_level_values("nodes").isin(
                            classes.index.get_level_values("nodes")))]
                # Set the location of all association ends that have a class in the struct (i.e., non-loose ends)
                for end in association_ends.itertuples():
                    location_attr[end.misc_properties["End_name"]] = alias_table[table]
                    alias_attr[end.misc_properties["End_name"]] = self.get_class_id_by_name(
                        self.get_edge_by_phantom_name(end.Index[1]))
        return alias_table, alias_attr, location_attr

    def get_discriminants(self, tables_combination, pattern_class_names):
        discriminants = []
        # For every class in the pattern
        for pattern_class_name in pattern_class_names:
            pattern_superclasses = self.get_superclasses_by_class_name(pattern_class_name, [])
            if pattern_superclasses:
                # For every table in the query
                for table in tables_combination:
                    for struct_name in self.get_struct_names_inside_set_name(table):
                        # For all classes in the current struct of the current table
                        table_classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("nodes").isin(pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_inbound_classes(), on="nodes", how="inner").index)]
                        # For all classes in the table
                        for table_class_name in table_classes.index.get_level_values("edges"):
                            table_hierarchy = [table_class_name]+self.get_superclasses_by_class_name(table_class_name, [])
                            # Check if they are siblings
                            if pattern_class_name != table_class_name and [s for s in pattern_superclasses if s in table_hierarchy]:
                                # TODO: We need to check if the discriminant is available in the table, which should happen if the generalization is overlapping
                                # Add the corresponding discriminant (this works because we have single inheritance)
                                discriminants.append(
                                    self.get_outbound_generalization_subclasses().reset_index(level="edges", drop=True).loc[
                                        self.get_phantom_of_edge_by_name(pattern_class_name)].misc_properties["Constraint"])
        # It should not be necessary to remove duplicates if dessing and query are sound (some extra check may be needed)
        # Right now, the same discriminant twice is useless, because attribute alias can come from only one table
        return drop_duplicates(discriminants)

    def generate_joins(self, tables, query_classes, query_associations, alias_table, alias_attr, visited, explicit_schema=False):
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
        :return: String containing the join clause of the tables received as parameter
        """
        # TODO: Consider that there could be more than one connected component (provided by the query) in the table
        #   (associations should be used to choose the right one)
        if explicit_schema:
            schema = self.dbschema+"."
        else:
            schema = ""
        first_table = (visited == {})
        unjoinable = []
        associations = self.get_outbound_associations()[self.get_outbound_associations().index.get_level_values("edges").isin(query_associations)]
        query_superclasses = query_classes.copy()
        for class_name in query_classes:
            query_superclasses.extend(self.get_superclasses_by_class_name(class_name, []))
        query_superclasses = drop_duplicates(query_superclasses)
        while tables:
            # Take any table and find all its potentially connection points
            current_table = tables.pop(0)
            # Get potential attributes to plug the current table
            plugs = []  # This will contain pairs of attribute names that can be plugged (first belongs to the current table)
            # For every struct in the table
            for struct_name in self.get_struct_names_inside_set_name(current_table):
                for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
                    if self.is_class_phantom(incidence.Index[1]):
                        class_name = self.get_edge_by_phantom_name(incidence.Index[1])
                        if class_name in query_superclasses:
                            # Any class in the query is a potential connection point per se
                            plugs.append((self.get_class_id_by_name(class_name), self.get_class_id_by_name(class_name)))
                            # Also, it can connect to a loose end if it participates in an association
                            for ass in associations.itertuples():
                                if self.get_edge_by_phantom_name(ass.Index[1]) in [class_name]+self.get_superclasses_by_class_name(class_name, []):
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
            for plug in plugs:
                if plug[1] in visited:
                    joins.append(alias_table[visited[plug[1]]]+"."+plug[1]+"="+alias_table[current_table]+"."+plug[0])
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
        join_clause = schema + current_table + " " + alias_table[current_table]
        if not first_table:
            if unjoinable:
                raise ValueError(f"Tables '{unjoinable}' are not joinable in the query")
            join_clause = "  JOIN "+join_clause+" ON "+" AND ".join(joins)
        if not tables:
            return join_clause
        else:
            return join_clause+'\n '+self.generate_joins(tables, query_classes, query_associations, alias_table, alias_attr, visited, explicit_schema)

    def generate_sql(self, spec, explicit_schema=False, verbose=False):
        '''
        Generates SQL statements corresponding to the given query.
        It uses the bucket algorithm of query rewriting using views to generate all possible combinations of tables to
        retrieve the required classes and associations
        :param spec: A JSON containing the select-project-join information
        :return: A list with all possible SQL statements ascendantly sorted by the number of tables
        '''
        logger.info("Resolving query")
        project_attributes, filter_attributes, pattern_edges, required_attributes, filter_clause = self.parse_query(spec)
        # For each combination of tables, generate an SQL query
        sentences = []
        # Check if all classes in the pattern are in some struct
        # Some classes may be stored implicitly by their subclasses
        classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("edges").isin(pattern_edges)]
        implicit_classes = classes[~classes.index.get_level_values("nodes").isin(self.get_outbound_structs().index.get_level_values("nodes"))]
        # If all classes in the pattern are in some struct
        if implicit_classes.empty:
            query_alternatives, class_names, association_names = self.create_bucket_combinations(pattern_edges, required_attributes)
            if len(query_alternatives) > 1:
                if verbose: print(f"WARNING: The query may be ambiguous, since it can be solved by using different combinations of tables: {query_alternatives}")
                query_alternatives = sorted(query_alternatives, key=len)
            for tables_combination in query_alternatives:
                alias_table, alias_attr, location_attr = self.get_aliases(tables_combination)
                modified_filter_clauses = [filter_clause]+self.get_discriminants(tables_combination, class_names)
                # Simple case of only one table required by the query
                if len(tables_combination) == 1:
                    # Build the SELECT clause
                    sentence = "SELECT " + ", ".join([alias_attr[a] for a in project_attributes])
                    # Build the FROM clause
                    sentence += "\nFROM " + tables_combination[0]
                # Case with several tables that require joins
                else:
                    # Build the SELECT clause
                    sentence = "SELECT " + ", ".join([location_attr[a]+"."+alias_attr[a] for a in project_attributes])
                    # Build the FROM clause
                    sentence += "\nFROM "+self.generate_joins(tables_combination, class_names, association_names, alias_table, location_attr,{}, explicit_schema)
                    # Add alias to the WHERE clause if there is more than one table
                    for attr in location_attr.items():
                        modified_filter_clauses = [s.replace(attr[0], attr[1]+"."+alias_attr[attr[0]]) for s in modified_filter_clauses]
                # Build the WHERE clause
                sentence += "\nWHERE " + " AND ".join(modified_filter_clauses)
                sentences.append(sentence)
        # If some classes are implicitly stored in the current design (i.e. stored only in their subclasses)
        else:
            # We need to recursively do it one by one, so we only take the first implicit superclass
            superclass_name = implicit_classes.index[0][0]
            superclass_phantom_name = implicit_classes.index[0][1]
            # This deals with multiple generalizations at once. Most probably, it should deal one by one
            generalization = self.get_outbound_generalization_superclasses().reset_index(level="edges", drop=False).loc[superclass_phantom_name]
            subclasses = self.get_outbound_generalization_subclasses().loc[generalization.edges]
            subqueries = []
            for subclass_phantom in subclasses.itertuples():
                new_query = spec.copy()
                # Replace the superclass by one of its subclasses in the query pattern
                new_query["pattern"] = [self.get_edge_by_phantom_name(subclass_phantom.Index) if elem == superclass_name else elem for elem in new_query["pattern"]]
                subqueries.append(self.generate_sql(new_query, explicit_schema, verbose))
            # We need to combine it, because a query may be solved in many different ways
            for combination in list(itertools.product(*drop_duplicates(subqueries))):
                sentences.append("\nUNION\n".join(combination))
        return sentences
