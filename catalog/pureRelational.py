import logging
from IPython.display import display
import pandas as pd
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
                    struct_phantom = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
                    elements = self.get_outbound_struct_by_phantom_name(struct_phantom.index[0][1])
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

    def create_tables(self, verbose=False):
        logging.info("Creating tables")
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            clause_PK = None
            logging.info("-- Creating table " + table.Index[0])
            sentence = "CREATE TABLE IF NOT EXISTS " + table.Index[0] + " (\n"
            struct_phantom = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
            elements = self.get_outbound_struct_by_phantom_name(struct_phantom.index[0][1])
            # For each element in the table
            for elem in elements.itertuples():
                # If it is an attribute
                if self.is_attribute(elem.Index[1]):
                    attribute = self.get_attributes().query('nodes == "'+elem.Index[1]+'"')
                    sentence += "  " + elem.Index[1]
                    if attribute.iloc[0]["misc_properties"].get("DataType") == "String":
                        sentence += " VarChar(" + str(attribute.iloc[0]["misc_properties"].get("Size")) + "),\n"
                    else:
                        sentence += " " + attribute.iloc[0]["misc_properties"].get("DataType") + ",\n"
                else:
                    if elem.misc_properties.get("Anchor"):
                        anchor_name = elem.Index[1]
            # If the anchor is a class, its ID is the PK
            if self.is_class(self.get_edge_by_phantom_name(anchor_name)):
                anchor_id = self.get_class_id_by_phantom_name(anchor_name).index[0]
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
                    leg_id = self.get_class_id_by_phantom_name(leg.Index[1]).index[0]
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

    def execute(self, query, verbose=False):
        logging.info("Executing query")
        print("--------- Query parsed begin")
        # Get the query
        project_attributes = query.get("project")
        join_relationships = query.get("join")
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
        print("Project: "+str(project_attributes))
        print("Join: "+str(join_relationships))
        print("Filter: "+str(filter_attributes))
        print("Required attributes: " + str(required_attributes))
        print("--------- Query parsed end")
        # Check if the hypergraph contains all the projected attributes
        non_existing_attributes = df_difference(pd.DataFrame(project_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the projection does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the filter attributes
        non_existing_attributes = df_difference(pd.DataFrame(filter_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the filter does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the join hyperedges
        non_existing_relationships = df_difference(pd.DataFrame(join_relationships), pd.concat([self.get_classes(), self.get_relationships()])["name"].reset_index(drop=True))
        if non_existing_relationships.shape[0] > 0:
            raise ValueError(f"Some class or relationship in the join does not belong to the catalog: {non_existing_relationships.values.tolist()[0]}")

        restricted_schema = self.H.restrict_to_edges(join_relationships)
        # Check if the restricted schema is connected
        if not restricted_schema.is_connected(s=1):
            raise ValueError(f"Some query elements (i.e., attributes, classes and relationships) are not connected")

        # Check if the restricted schema contains all the required attributes
        missing_attributes = df_difference(pd.DataFrame(required_attributes), restricted_schema.nodes.dataframe.reset_index(drop=False)["uid"])
        if missing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the query is not covered by the joined elements: {missing_attributes.values.tolist()[0]}")

        # Get the tables where every required relationship is found
        tables = []
        for rel in join_relationships:
            first_levels = self.get_transitives()[(self.get_transitives().index.get_level_values('nodes') == self.get_phantom_of_edge_by_name(rel)) & (self.get_transitives().index.get_level_values('edges').isin(self.get_edges_firstlevel()["edges"]))].reset_index(drop=False)["edges"].drop_duplicates().values.tolist()
            first_levels.sort()
            print(rel+"\t"+str(first_levels))
            tables.append(first_levels)
        query_options = combine_tables(drop_duplicates(tables))
        if len(query_options) > 1:
            print(f"WARNING: The query may be ambiguous, since it can be solved by using different combinations of tables: {query_options}")
        for option in query_options:
            if len(option) == 1:
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join(project_attributes)
                # Build the FROM clause
                sentence += "\nFROM " + option[0]
            else:
                print("Involved tables: "+str(option))
                # Check if the combination is connected by the given relationships, and find the join attributes
                table_links = self.get_incidences()[(self.get_incidences().index.get_level_values('edges').isin(option))]
                print("-------------------------Table links: ")
                display(table_links)
                relationship_links = self.get_incidences()[(self.get_incidences().index.get_level_values('edges').isin(join_relationships))]
                print("----------------------Relationship links: ")
                display(relationship_links)
                # Disambiguate required attributes
                # Build the SELECT clause
                sentence = "SELECT " + ", ".join(project_attributes)
                # Build the FROM clause
                sentence += "\nFROM " + ",".join(option)
            # Build the WHERE clause
            sentence += "\n" + where_clause + ";"
            if verbose:
                print(sentence)
