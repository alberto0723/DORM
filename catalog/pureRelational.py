import logging
from IPython.display import display
import pandas as pd
from matplotlib import table
import networkx as nx

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

from .relational import Relational
from .tools import df_difference, show_textual_hypergraph, show_graphical_hypergraph, combine_tables, drop_duplicates

class PostgreSQL(Relational):
    """This is a subclass of Relational that implements the code generation in PostgreSQL
    """
    def __init__(self, file=None):
        super().__init__(file)

    def check_toOne(self, path):
        correct = True
        print("Path:", path)
        for i, current in enumerate(path):
            if self.is_relationship(current):
                print("Current relationship:", current)
                if len(path) > i+1:
                    print(path[i+1])
                    properties = self.H.get_cell_properties(current, path[i+1])
                    print(properties)
                    if "Multiplicity" in properties:
                        correct = correct and (properties.get("Multiplicity") <= 1)
                    else:
                        raise ValueError(f"Checking multiplicity: Multiplicity not provided for relationship '{current}-{path[i+1]}'")
        return correct

    def is_correct(self, design=False):
        correct = super().is_correct(design)
        if correct:
            # ---------------------------------------------------------------- ICs about being a pure relational catalog
            # IC-PureRelational1: All relationships from the anchor of a struct must be to one (or less)
            logging.info("Checking IC-PureRelational1")
            firstlevels = self.get_inbound_firstLevel()
            # For each table
            for table in firstlevels.itertuples():
                for struct in self.get_outbound_sets().query('edges == "'+table.Index[0]+'"').itertuples():
                    struct_name = self.get_edge_by_phantom_name(struct.Index[1])
                    print("---------------------Struct_name:", struct_name)
                    members = self.get_outbound_struct_by_name(struct_name).index.get_level_values(1).tolist()
                    anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                    dont_cross = self.get_anchor_relationships_by_struct_name(struct_name)
                    restricted_struct = self.get_restricted_struct_hypergraph(struct_name).remove_edges(dont_cross)
                    bipartite = restricted_struct.bipartite()
                    print("Anchor_points:", anchor_points)
                    print("Members:", members)
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

    def create_schema(self, verbose=False):
        logging.info("Creating tables")
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            clause_PK = None
            logging.info("-- Creating table " + table.Index[0])
            sentence = "CREATE TABLE IF NOT EXISTS " + table.Index[0] + " (\n"
            struct_phantoms = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
            # TODO: Consider multiple structs in a set (corresponding to horizontal partitioning)
            struct_name = self.get_edge_by_phantom_name(struct_phantoms.index[0][1])
            elements = self.get_outbound_struct_by_name(struct_name)
            # For each element in the table
            attribute_list = []
            for elem in elements.itertuples():
                if self.is_attribute(elem.Index[1]):
                    attribute_list.append(elem.Index[1])
                elif self.is_class_phantom(elem.Index[1]):
                    attribute_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(elem.Index[1])))
                elif self.is_relationship_phantom(elem.Index[1]):
                    ends = self.get_outbound_relationship_by_name(self.get_edge_by_phantom_name(elem.Index[1]))
                    for end in ends.itertuples():
                        attribute_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(end.Index[1])))
                else:
                    raise ValueError(f"Some element in a struct is not expected: '{elem.Index[1]}'")
            attribute_list = list(set(attribute_list))
            for attr_name in attribute_list:
                attribute = self.get_attributes().query('nodes == "'+attr_name+'"')
                sentence += "  " + attr_name
                if attribute.iloc[0]["misc_properties"].get("DataType") == "String":
                    sentence += " VarChar(" + str(attribute.iloc[0]["misc_properties"].get("Size")) + "),\n"
                else:
                    sentence += " " + attribute.iloc[0]["misc_properties"].get("DataType") + ",\n"
            # If the anchor is a class, its ID is the PK
            key_list = []
            for key in self.get_anchor_points_by_struct_name(struct_name):
                key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
            if len(key_list) > 0:
                clause_PK = "  PRIMARY KEY ("+",".join(key_list)+")\n"
            else:
                raise ValueError(f"Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined")
            sentence += clause_PK + "  );"
            if verbose:
                print(sentence)

    def generate_joins(self, tables, query_classes, query_relationships, visited, alias_table, alias_attr):
        first_table = (visited == {})
        unjoinable = []
        while tables:
            current_table = tables.pop(0)
            # TODO: Consider that there could be more than one connected component (provided by the query) in the table
            #  (relationships should be used to choose the right one)
            # Generate joins for classes already in visited
            struct_name = self.get_edge_by_phantom_name(self.get_outbound_set_by_name(current_table).index[0][1])
            current_classes = []
            # If all relationships in the anchor are in the query anchor points are considered for join
            if all(rel in query_relationships for rel in self.get_anchor_relationships_by_struct_name(struct_name)):
                for phantom_name in self.get_anchor_points_by_struct_name(struct_name):
                    current_classes.append(self.get_edge_by_phantom_name(phantom_name))
            # If classes are in the query, they are considered for join
            for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
                if self.is_class_phantom(incidence.Index[1]):
                    class_name = self.get_edge_by_phantom_name(incidence.Index[1])
                    if class_name in query_classes:
                        current_classes.append(class_name)
            joins = []
            for c in current_classes:
                if c in visited:
                    identifier = self.get_class_id_by_name(c)
                    joins.append(alias_table[visited[c]]+"."+identifier+"="+alias_table[current_table]+"."+identifier)
            if not first_table and not joins:
                unjoinable.append(current_table)
            else:
                tables += unjoinable
                unjoinable = []
                break
        # Get all the classes in the table and mark them as visited
        # TODO: Consider multiple structs inside a set (corresponding to horizontal partitioning)
        for c in current_classes:
            visited[c] = current_table
        # Create the join clause
        join_clause = current_table + " " + alias_table[current_table]
        if not first_table:
            if unjoinable:
                raise ValueError(f"Tables '{unjoinable}' are not joinable in the query")
            join_clause = "  JOIN "+join_clause+" ON "+" AND ".join(joins)
        if not tables:
            return join_clause
        else:
            return join_clause+'\n '+self.generate_joins(tables, query_classes, query_relationships, visited, alias_table, alias_attr)

    def generate_SQL(self, query, verbose=True):
        logging.info("Executing query")
        project_attributes, filter_attributes, join_edges, required_attributes, filter_clause = self.parse_query(query)

        # Get the tables where each required domain elements is found
        tables = []
        classes = []
        relationships = []
        for elem in join_edges:
            # Split join edges into classes and relationships
            if self.is_class(elem):
                classes.append(elem)
            if self.is_relationship(elem):
                relationships.append(elem)
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
            tables.append(first_levels)
        # Generate combinations of the tables of each element to get the combinations that cover all of them
        query_options = combine_tables(drop_duplicates(tables))
        if len(query_options) > 1:
            if verbose: print(f"WARNING: The query may be ambiguous, since it can be solved by using different combinations of tables: {query_options}")
            query_options = sorted(query_options, key=len)
        # For each option, generate an SQL query
        sentences = []
        for option in query_options:
            modified_filter_clause = filter_clause
            # Simple case of only one table required by the query
            if len(option) == 1:
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join(project_attributes)
                # Build the FROM clause
                sentence += "\nFROM " + option[0]
            # Case with several tables that require joins
            else:
                # Determine the aliases of tables and required attributes
                alias_table = {}
                alias_attr = {}
                # The list of tables is reversed, so that the first appearance of an attribute prevails (seems more logical)
                for index, table in enumerate(reversed(option)):
                    alias_table[table] = self.config.prepend_table_alias+str(len(option)-index)
                    # TODO: There could be more than one struct
                    struct_name = self.get_edge_by_phantom_name(self.get_outbound_set_by_name(table).index[0][1])
                    implicit_classes = [self.get_edge_by_phantom_name(p) for p in self.get_anchor_points_by_struct_name(struct_name)]
                    implicit_ids = self.get_outbound_classes()[(self.get_outbound_classes().index.get_level_values("edges").isin(implicit_classes)) & (self.get_outbound_classes().index.get_level_values("nodes").isin(self.get_ids()["name"]))]
                    for id in implicit_ids.itertuples():
                        alias_attr[id.Index[1]] = alias_table[table]
                    contained_attributes = self.get_transitives_by_edge_name(table)[self.get_transitives_by_edge_name(table).index.get_level_values("nodes").isin(required_attributes)]
                    for attr in contained_attributes.itertuples():
                        alias_attr[attr.Index[1]] = alias_table[table]
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join([alias_attr[a]+"."+a for a in project_attributes])
                # Build the FROM clause
                sentence += "\nFROM "+self.generate_joins(option, classes, relationships,{}, alias_table, alias_attr)
                # Add alias to the WHERE clause if there is more than one table
                for attr in alias_attr.items():
                    modified_filter_clause = modified_filter_clause.replace(attr[0], attr[1]+"."+attr[0])
            # Build the WHERE clause
            sentence += "\n WHERE " + modified_filter_clause + ";"
            sentences.append(sentence)
        return sentences
