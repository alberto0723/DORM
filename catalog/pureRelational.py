import logging
from IPython.display import display
import pandas as pd
from matplotlib import table

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
import sqlparse

from .relational import Relational
from .tools import df_difference, show_textual_hypergraph, show_graphical_hypergraph, combine_tables, drop_duplicates

class PostgreSQL(Relational):
    """This is a subclass of Relational that implements the code generation in PostgreSQL
    """
    def __init__(self, file=None):
        super().__init__(file)

    def check_toOne(self, current, members):
        correct = True
        allRelationships = self.get_outbound_relationships().reset_index(drop=False)
        currentRelationships = allRelationships[allRelationships['nodes'] == current]
        for r in allRelationships[
                (allRelationships['edges'].isin(currentRelationships['edges'])) & (allRelationships['nodes'] != current)
                ].itertuples():
            if self.get_phantom_of_edge_by_name(r.edges) in members:
                if r.misc_properties.get("Multiplicity") <= 1:
                    correct = correct and self.check_toOne(r.nodes, [x for x in members if x != self.get_phantom_of_edge_by_name(r.edges)])
                else:
                    correct = False
                    print(f"IC-PureRelational1 violation: Multiplicity of '{r.edges}' towards '{self.get_edge_by_phantom_name(r.nodes)}' should be less or equal than 1 (it is '{r.misc_properties.get("Multiplicity")}')")
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
                if correct:
                    struct_phantoms = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
                    # TODO: There could be more than one struct in a set (horizontal partitioning)
                    elements = self.get_outbound_struct_by_name(struct_phantoms.index[0][0])
                    members = elements.index.get_level_values(1).tolist()
                    anchor = elements[elements["misc_properties"].apply(lambda x: x.get('Anchor', False))]
                    if self.is_class_phantom(anchor.index[0][1]):
                        correct = self.check_toOne(anchor.index[0][1], members)
                    elif self.is_relationship_phantom(anchor.index[0][1]):
                        relationship = self.get_inbound_relationships().query('nodes == "'+anchor.index[0][1]+'"')
                        legs = self.get_outbound_relationships().query('edges == "'+relationship.index[0][0]+'"')
                        for leg in legs.itertuples():
                            correct = correct and self.check_toOne(leg.Index[1], [x for x in members if x != anchor.index[0][1]])
                    else:
                        correct = False
                        print(f"IC-PureRelational1 violation: A struct '{table.Index[0]}' has an unacceptable anchor '{anchor.index[0][1]}'")
                        display(elements)
        return correct

    def create_schema(self, verbose=False):
        logging.info("Creating tables")
        show_textual_hypergraph(self.H)
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            clause_PK = None
            logging.info("-- Creating table " + table.Index[0])
            sentence = "CREATE TABLE IF NOT EXISTS " + table.Index[0] + " (\n"
            struct_phantoms = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
            # TODO: Consider multiple structs in a set (corresponding to horizontal partitioning)
            elements = self.get_outbound_struct_by_name(self.get_edge_by_phantom_name(struct_phantoms.index[0][1]))
            # For each element in the table
            attribute_list = []
            for elem in elements.itertuples():
                if elem.misc_properties.get("Anchor"):
                    anchor_name = elem.Index[1]
                else:
                    # If it is an attribute
                    if self.is_attribute(elem.Index[1]):
                        attribute_list.append(elem.Index[1])
            attribute_list = list(set(attribute_list))
            for attr_name in attribute_list:
                attribute = self.get_attributes().query('nodes == "'+attr_name+'"')
                sentence += "  " + attr_name
                if attribute.iloc[0]["misc_properties"].get("DataType") == "String":
                    sentence += " VarChar(" + str(attribute.iloc[0]["misc_properties"].get("Size")) + "),\n"
                else:
                    sentence += " " + attribute.iloc[0]["misc_properties"].get("DataType") + ",\n"
            # If the anchor is a class, its ID is the PK
            if self.is_class(self.get_edge_by_phantom_name(anchor_name)):
                anchor_id = self.get_class_id_by_name(self.get_edge_by_phantom_name(anchor_name))
                if elements[elements.index.get_level_values("nodes") == anchor_id].shape[0] == 0:
                    raise ValueError(f"'{table.Index[0]}' must contain its anchor ID {anchor_id} as an element")
                else:
                    clause_PK = "  PRIMARY KEY ("+anchor_id+")\n"
            # If it is a relationship, we get a compound PK
            # TODO: There could be chained relationships as anchor (i.e., we would take the extremes as PK)
            elif self.is_relationship(self.get_edge_by_phantom_name(anchor_name)):
                legs = self.get_outbound_relationships().query('edges == "'+self.get_edge_by_phantom_name(anchor_name)+'"')
                leg_names = []
                for leg in legs.itertuples():
                    leg_id = self.get_class_id_by_name(self.get_edge_by_phantom_name(leg.Index[1]))
                    if elements[elements.index.get_level_values("nodes") == leg_id].shape[0] == 0:
                        raise ValueError(f"'{table.Index[0]}' must contain its anchor ID {leg_id} as an element")
                    leg_names.append(leg_id)
                clause_PK = "  PRIMARY KEY ("+",".join(leg_names)+")\n"
            else:
                raise ValueError(f"Anchor of '{table.Index[0]}' (i.e. {anchor_name}) is neither a class nor a relationship")
            if clause_PK is None:
                raise ValueError(f"Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined")
            sentence += clause_PK + "  );"
            if verbose:
                print(sentence)

    def generate_joins(self, tables, classes, relationships, visited, alias_table, alias_attr):
        first_table = (visited == {})
        unjoinable = []
        while tables:
            current_table = tables.pop(0)
            # TODO: Consider that there could be more than one connected component (provided by the query) in the table
            # TODO: Consider multiple structs inside a set (corresponding to horizontal partitioning)
            # Get all the edges in the table
            struct_name = self.get_outbound_set_by_name(current_table).index[0][0]
            current_classes = []
            current_relationships = []
            for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
                edge_phantom = incidence.Index[1]
                if self.is_class_phantom(edge_phantom):
                    if self.get_edge_by_phantom_name(edge_phantom) in classes:
                        current_classes.append(self.get_edge_by_phantom_name(edge_phantom))
                    if self.get_edge_by_phantom_name(edge_phantom) in relationships:
                        current_relationships.append(self.get_edge_by_phantom_name(edge_phantom))
            # Generate joins for classes already in visited
            joins = []
            for c in classes:
                if c in visited:
                    identifier = self.get_class_id_by_name(c)
                    joins.append(alias_table[visited[c]]+"."+identifier+"="+alias_table[current_table]+"."+identifier)
                else:
                    visited[c] = current_table
            if not first_table and not joins:
                unjoinable.append(current_table)
            else:
                tables += unjoinable
                unjoinable = []
                break
        join_clause = current_table + " " + alias_table[current_table]
        if not first_table:
            if unjoinable:
                raise ValueError(f"Tables '{unjoinable}' are not joinable in the query")
            join_clause = "  JOIN "+join_clause+" ON "+" AND ".join(joins)+','
        if not tables:
            return join_clause
        else:
            return join_clause+'\n '+self.generate_joins(tables, classes, list(set(relationships)-set(current_relationships)), visited, alias_table, alias_attr)

    def generate_SQL(self, query):
        logging.info("Executing query")
        sentences = []
        # Get the query and parse it
        project_attributes = query.get("project")
        join_edges = query.get("join")
        filter_attributes = []
        if "filter" in query:
            where_clause = "WHERE "+query.get("filter")
            where_parsed = sqlparse.parse(where_clause)[0].tokens[0]

            # This extracts the attribute names
            # TODO: Parenthesis are not considered. It will require some kind of recursion
            for atom in where_parsed.tokens:
                if atom.ttype is None:  # This is a clause in the predicate
                    for token in atom.tokens:
                        if token.ttype is None:  # This is an attribute in the predicate
                            filter_attributes.append(token.value)
        else:
            where_clause = ""
        required_attributes = list(set(project_attributes + filter_attributes))
        # Check if the hypergraph contains all the projected attributes
        non_existing_attributes = df_difference(pd.DataFrame(project_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the projection does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the filter attributes
        non_existing_attributes = df_difference(pd.DataFrame(filter_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the filter does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the join hyperedges
        non_existing_relationships = df_difference(pd.DataFrame(join_edges), pd.concat([self.get_classes(), self.get_relationships()])["name"].reset_index(drop=True))
        if non_existing_relationships.shape[0] > 0:
            raise ValueError(f"Some class or relationship in the join does not belong to the catalog: {non_existing_relationships.values.tolist()[0]}")

        restricted_domain = self.H.restrict_to_edges(join_edges)
        # Check if the restricted domain is connected
        if not restricted_domain.is_connected(s=1):
            raise ValueError(f"Some query elements (i.e., attributes, classes and relationships) are not connected")

        # Check if the restricted domain contains all the required attributes
        missing_attributes = df_difference(pd.DataFrame(required_attributes), restricted_domain.nodes.dataframe.reset_index(drop=False)["uid"])
        if missing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the query is not covered by the joined elements: {missing_attributes.values.tolist()[0]}")

        # Get the tables where every required domain elements are found
        tables = []
        classes = []
        relationships = []
        for elem in join_edges+required_attributes:
            # Split join edges into classes and relationships
            if self.is_class(elem):
                classes.append(elem)
                node_name = self.get_phantom_of_edge_by_name(elem)
            elif self.is_relationship(elem):
                relationships.append(elem)
                node_name = self.get_phantom_of_edge_by_name(elem)
            elif self.is_attribute(elem):
                node_name = elem
            else:
                raise ValueError(f"A join edge was neither a class nor a relationship nor an attribute")
            first_levels = self.get_transitives()[(self.get_transitives().index.get_level_values('nodes') == node_name) & (self.get_transitives().index.get_level_values('edges').isin(self.get_edges_firstlevel()["edges"]))].reset_index(drop=False)["edges"].drop_duplicates().values.tolist()
            first_levels.sort()
            tables.append(first_levels)

        query_options = combine_tables(drop_duplicates(tables))
        if len(query_options) > 1:
            print(f"WARNING: The query may be ambiguous, since it can be solved by using different combinations of tables: {query_options}")
            query_options = sorted(query_options, key=len)
        for option in query_options:
            if len(option) == 1:
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join(project_attributes)
                # Build the FROM clause
                sentence += "\nFROM " + option[0]
            else:
                # Determine the aliases of tables and required attributes
                alias_table = {}
                alias_attr = {}
                # The list of tables is reversed, so that the first appearance of an attribute prevails (seems more logical)
                for index, table in enumerate(reversed(option)):
                    alias_table[table] = self.config.prepend_table_alias+str(len(option)-index)
                    contained_attributes = self.get_transitives_by_edge_name(table)[self.get_transitives_by_edge_name(table).index.get_level_values("nodes").isin(required_attributes)]
                    for attr in contained_attributes.itertuples():
                        alias_attr[attr.Index[1]] = alias_table[table]
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join([alias_attr[a]+"."+a for a in project_attributes])
                # Build the FROM clause
                sentence += "\nFROM "+self.generate_joins(option, classes, relationships, {}, alias_table, alias_attr)
                # Add alias to the where clause if there is more than one table
                for attr in alias_attr.items():
                    where_clause = where_clause.replace(attr[0], attr[1]+"."+attr[0])
            # Build the WHERE clause
            sentence += "\n" + where_clause + ";"
            sentences.append(sentence)
        return sentences
