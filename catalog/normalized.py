import logging
from IPython.display import display
import pandas as pd
from matplotlib import table
import networkx as nx

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

from .relational import Relational
from .tools import df_difference, combine_tables, drop_duplicates

logger = logging.getLogger("Normalized")

class Normalized(Relational):
    """This is a subclass of Relational that implements the code generation as normalized in 1NF
    """
    def __init__(self, file=None):
        super().__init__(file)

    def check_toOne(self, path):
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

    def is_correct(self, design=False):
        correct = super().is_correct(design)
        if correct:
            # ---------------------------------------------------------------- ICs about being a normalized catalog
            # IC-Normalized1: All associations from the anchor of a struct must be to one (or less)
            logger.info("Checking IC-Normalized1")
            firstlevels = self.get_inbound_firstLevel()
            # For each table
            for table in firstlevels.itertuples():
                for struct in self.get_outbound_sets().query('edges == "'+table.Index[0]+'"').itertuples():
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
                                    if not self.check_toOne(paths[0]):
                                        correct = False
                                        print(f"IC-PureRelational1 violation: A struct '{struct_name}' has an unacceptable path (not to one) '{paths[0]}'")
                                elif len(paths) > 1:
                                    raise ValueError(f"IC-PureRelational1: Something went wrong in '{struct_name}' on finding more than one path '{paths}' between '{anchor}' and '{member}'")

        return correct

    def get_struct_attributes(self, struct_name):
        '''
        This generates the correspondence between attribute names in a table and their corresponding attribute.
        It is necessary to do it to consider foreign keys
        :param struct_name:
        :return: A dictionary with the pairs "intable_name" and "domain_name" in the hypergraph attribute
        '''
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

    def create_schema(self, verbose=False):
        '''
        Creates the tables in the design. One table is created for every set in the first level (i.e., without parent).
        One or more structs are expected inside the set, but all of them should generate the same attributes.
        Inside each table, there are all the attributes in the struct, plus the IDs of the classes, plus the loose ends
        of the associations.
        The primary key of the table is composed by the IDs of the classes in the anchor of the struct, plus the loose
        ends of the associations in the anchor.
        :param verbose: Indicates if the DDL should be printed
        :return: ???
        '''
        # TODO: Connect to the DB and create the table there (better to create all at once to be sure they are all correct)
        logger.info("Creating tables")
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info("-- Creating table " + table.Index[0])
            sentence = "DROP TABLE IF EXISTS " + table.Index[0]+" CASCADE;\nCREATE TABLE " + table.Index[0] + " (\n"
            struct_phantoms = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
            # TODO: Consider multiple structs in a set (corresponding to horizontal partitioning)
            struct_name = self.get_edge_by_phantom_name(struct_phantoms.index[0][1])
            attribute_dicc = self.get_struct_attributes(struct_name)
            for attr_alias, attr_name in attribute_dicc.items():
                attribute = self.get_attributes().query('nodes == "'+attr_name+'"')
                sentence += "  " + attr_alias
                if attribute.iloc[0]["misc_properties"].get("DataType") == "String":
                    sentence += " VarChar(" + str(attribute.iloc[0]["misc_properties"].get("Size")) + "),\n"
                else:
                    sentence += " " + attribute.iloc[0]["misc_properties"].get("DataType") + ",\n"
            # If the anchor is a class, its ID is the PK
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class_phantom(key):
                    key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                else:
                    key_list.append(key)
            if not key_list:
                raise ValueError(f"Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined")
            clause_PK = "  PRIMARY KEY ("+",".join(key_list)+")\n"
            sentence += clause_PK + "  );"
            if verbose:
                print(sentence)

    def create_bucket_combinations(self, pattern, required_attributes):
        '''
        For each required domain elements, create a bucket with all the tables where it can come from.
        Then, combine all these buckets to cover all elements
        :param join_edges: List of classes and associations in the query
        :param required_attributes: List of attributes used in the query
        :return: List of combinations of tables covering all the required elements
        :return: List of classes required
        :return: List of associations required
        '''
        tables = []
        classes = []
        associations = []
        for elem in pattern:
            # Find the tables (aka fist level elements) where the element belongs
            node_name = self.get_phantom_of_edge_by_name(elem)
            second_levels = self.get_outbound_structs()[self.get_outbound_structs().index.get_level_values('nodes') == node_name]
            inbounds = self.get_inbound_structs()
            inbounds["nodes"] = inbounds.index.get_level_values('nodes')
            second_level_phantoms = pd.merge(second_levels, inbounds, on="edges", how="inner")["nodes"]
            # No need to check if they are at first level, because sets always are (no nested structures are allowed)
            #first_levels = self.get_outbound_sets()[(self.get_outbound_sets().index.get_level_values('nodes').isin(second_level_phantoms)) & (self.get_outbound_sets().index.get_level_values('edges').isin(self.get_edges_firstlevel()["edges"]))].reset_index(drop=False)["edges"].drop_duplicates().values.tolist()
            first_levels = self.get_outbound_sets()[self.get_outbound_sets().index.get_level_values('nodes').isin(second_level_phantoms)].reset_index(drop=False)["edges"].drop_duplicates().values.tolist()
            first_levels.sort()
            # Split join edges into classes and associations
            if self.is_association(elem):
                associations.append(elem)
                # If the element is an association, any table containing it is an option
                tables.append(first_levels)
            if self.is_class(elem):
                classes.append(elem)
                current_attributes = self.get_outbound_class_by_name(elem)[self.get_outbound_class_by_name(elem).index.get_level_values('nodes').isin(required_attributes)].index.get_level_values('nodes').values.tolist()
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

    def generate_joins(self, tables, query_classes, query_associations, alias_table, alias_attr, visited):
        '''
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
        '''
        # TODO: Consider that there could be more than one connected component (provided by the query) in the table
        #   (associations should be used to choose the right one)
        first_table = (visited == {})
        unjoinable = []
        associations = self.get_outbound_associations()[self.get_outbound_associations().index.get_level_values("edges").isin(query_associations)]
        while tables:
            # Take any table and find all its potentially connection points
            current_table = tables.pop(0)
            struct_name = self.get_edge_by_phantom_name(self.get_outbound_set_by_name(current_table).index[0][1])
            # Get potential attributes to plug the current table
            plugs = [] # This will contain pairs of attribute names that can be plugged (first belongs to the current table)
            for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
                if self.is_class_phantom(incidence.Index[1]):
                    class_name = self.get_edge_by_phantom_name(incidence.Index[1])
                    if class_name in query_classes:
                        # Any class in the query is a potential connection point per se
                        plugs.append((self.get_class_id_by_name(class_name), self.get_class_id_by_name(class_name)))
                        # Also, it can connect to a loose end if it participates in an association
                        for ass in associations.itertuples():
                            if class_name == self.get_edge_by_phantom_name(ass.Index[1]):
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
        # Get all the connection point in the table and mark them as visited
        # TODO: Consider multiple structs inside a set (corresponding to horizontal partitioning)
        for plug in plugs:
            visited[plug[0]] = current_table
        # Create the join clause
        join_clause = current_table + " " + alias_table[current_table]
        if not first_table:
            if unjoinable:
                raise ValueError(f"Tables '{unjoinable}' are not joinable in the query")
            join_clause = "  JOIN "+join_clause+" ON "+" AND ".join(joins)
        if not tables:
            return join_clause
        else:
            return join_clause+'\n '+self.generate_joins(tables, query_classes, query_associations, alias_table, alias_attr, visited)

    def generate_SQL(self, query, verbose=True):
        '''
        Generates SQL statements corresponding to the given query.
        It uses the bucket algorithm of query rewriting using views to generate all possible combinations of tables to
        retrieve the required classes and associations
        :param query: A JSON containing the select-project-join information
        :param verbose: Whether to print the SQL statements
        :return: A list with all possible SQL statements ascendantly sorted by the number of tables
        '''
        logger.info("Executing query")
        project_attributes, filter_attributes, pattern_edges, required_attributes, filter_clause = self.parse_query(query)

        # Check if all classes are in some struct
        classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("edges").isin(pattern_edges)]
        implicit_classes = classes[~classes.isin(self.get_outbound_structs().index.get_level_values("nodes"))]
        for class_edge in implicit_classes.itertuples():
            # TODO: This is assuming that the missing class participates in only one generalization
            generalization = self.get_outbound_generalization_superclasses().reset_index(level="edges", drop=False).loc[class_edge.Index[1]]
            subclasses = self.get_outbound_generalization_subclasses().loc[generalization.edges]
            for subclass_phantom in subclasses.itertuples():
                print(self.get_edge_by_phantom_name(subclass_phantom.Index))

        query_options, class_names, association_names = self.create_bucket_combinations(pattern_edges, required_attributes)
        if len(query_options) > 1:
            if verbose: print(f"WARNING: The query may be ambiguous, since it can be solved by using different combinations of tables: {query_options}")
            query_options = sorted(query_options, key=len)

        # For each combination of tables, generate an SQL query
        sentences = []
        for tables_combination in query_options:
            modified_filter_clauses = [filter_clause]
            alias_table = {}
            alias_attr = {}
            location_attr = {}
            # The list of tables is reversed, so that the first appearance of an attribute prevails (seems more logical)
            for index, table in enumerate(reversed(tables_combination)):
                # -- Determine the aliases of tables and required attributes
                alias_table[table] = self.config.prepend_table_alias + str(len(tables_combination) - index)
                # TODO: There could be more than one struct
                struct_name = self.get_edge_by_phantom_name(
                    self.get_outbound_set_by_name(table).index.get_level_values("nodes")[0])
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
                        associations.index.get_level_values("edges"))) & (self.get_outbound_associations().index.get_level_values("nodes").isin(classes.index.get_level_values("nodes")))]
                # Set the location of all association ends that have a class in the struct (i.e., non-loose ends)
                for end in association_ends.itertuples():
                    location_attr[end.misc_properties["End_name"]] = alias_table[table]
                    alias_attr[end.misc_properties["End_name"]] = self.get_class_id_by_name(
                        self.get_edge_by_phantom_name(end.Index[1]))
                # -- Find required discriminants
                # Foll all classes in the current table
                for class_name1 in classes.index.get_level_values("edges"):
                    # If the current one is in the query
                    if class_name1 in class_names:
                        superclasses1 = self.get_superclasses_by_class_name(class_name1, [])
                        # If it has superclasses
                        if superclasses1:
                            # Check all other classes in the table
                            for class_name2 in classes.index.get_level_values("edges"):
                                if class_name1 != class_name2:
                                    # Get their superclasses
                                    superclasses2 = self.get_superclasses_by_class_name(class_name2, [])
                                    # Check if they are siblings
                                    if [s for s in superclasses1 if s in superclasses2]:
                                        # Add the corresponding discriminant (this works because we have single inheritance)
                                        modified_filter_clauses.append(self.get_outbound_generalization_subclasses().reset_index(level="edges",
                                                                                                  drop=True).loc[self.get_phantom_of_edge_by_name(class_name1)].misc_properties["Constraint"])
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
                sentence += "\nFROM "+self.generate_joins(tables_combination, class_names, association_names, alias_table, location_attr,{})
                # Add alias to the WHERE clause if there is more than one table
                for attr in location_attr.items():
                    modified_filter_clauses = [s.replace(attr[0], attr[1]+"."+alias_attr[attr[0]]) for s in modified_filter_clauses]
            # Build the WHERE clause
            sentence += "\nWHERE " + " AND ".join(modified_filter_clauses) + ";"
            sentences.append(sentence)
        return sentences
