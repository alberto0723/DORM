import logging
import hypernetx as hnx
import pickle
from IPython.display import display
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')  # This sets the backend to plot (default TkAgg does not work)

from . import config
from .tools import df_difference


class Catalog:
    """This class manages the catalog of a database using hypergraphs
    It uses HyperNetX (https://github.com/pnnl/HyperNetX)
    """
    def __init__(self, file=None):
        self.config = config.Config()
        if file is None:
            self.H = hnx.Hypergraph([])
        else:
            logging.info("Loading hypergraph from " + str(file))
            with open(file, "rb") as f:
                self.H = pickle.load(f)

    def save(self, file):
        logging.info("Saving hypergraph in " + str(file))
        # Save the hypergraph to a pickle file
        with open(file, "wb") as f:
            pickle.dump(self.H, f)

    def get_nodes(self):
        nodes = self.H.nodes.dataframe.rename_axis("nodes")
        nodes["name"] = nodes.index
        return nodes

    def get_edges(self):
        edges = self.H.edges.dataframe.rename_axis("edges")
        edges["name"] = edges.index
        return edges

    def get_incidences(self):
        incidences = self.H.incidences.dataframe
        return incidences

    def get_ids(self):
        nodes = self.get_nodes()
        ids = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Identifier')]
        return ids

    def get_attributes(self):
        nodes = self.get_nodes()
        attributes = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Attribute')]
        return attributes

    def get_phantoms(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom')]
        return phantoms

    def get_phantom_relationships(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and x['Subkind'] == 'Relationship')]
        return phantoms

    def get_edge_by_phantom_name(self, phantom_name):
        return self.get_inbounds()[self.get_inbounds().index.get_level_values('nodes') == phantom_name].index[0][0]

    def get_phantom_of_edge_by_name(self, edge_name):
        return self.get_inbounds().loc[edge_name].index[0]

    def get_classes(self):
        edges = self.get_edges()
        classes = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Class')]
        return classes

    def get_relationships(self):
        edges = self.get_edges()
        relationships = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Relationship')]
        return relationships

    def get_structs(self):
        edges = self.get_edges()
        structs = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Struct')]
        return structs

    def get_sets(self):
        edges = self.get_edges()
        sets = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Set')]
        return sets

    def get_inbounds(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return inbounds

    def get_inbound_relationships(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'RelationshipIncidence')]
        return inbounds

    def get_inbound_structs(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'StructIncidence')]
        return inbounds

    def get_inbound_sets(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'SetIncidence')]
        return inbounds

    def get_outbounds(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound')]
        return outbounds

    def get_outbound_relationships(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'RelationshipIncidence')]
        return outbounds

    def get_outbound_structs(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'StructIncidence')]
        return outbounds

    def get_outbound_struct_by_phantom_name(self, phantom):
        struct = self.get_inbound_structs().query('nodes == "' + phantom + '"')
        elements = self.get_outbound_structs().query('edges == "' + struct.index[0][0] + '"')
        # elements = pd.concat([
        #                 self.get_outbound_structs().query('edges == "' + struct.index[0][0] + '"'),
        #                 self.get_transitives().query('edges == "' + struct.index[0][0] + '"'),
        #                 ])
        return elements

    def get_outbound_sets(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'SetIncidence')]
        return outbounds

    def get_transitives(self):
        incidences = self.get_incidences()
        transitives = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Transitive')]
        return transitives

    def get_inbound_firstLevel(self):
        firstLevelPhantoms = df_difference(pd.concat([self.get_inbound_structs(), self.get_inbound_sets()], ignore_index=False).reset_index()[["nodes"]],
                                    self.get_outbounds().reset_index()[["nodes"]])
        firstLevelIncidences = self.get_inbounds().join(firstLevelPhantoms.set_index("nodes"), on="nodes", how='inner')
        return firstLevelIncidences

    def is_attribute(self, name):
        return name in self.get_attributes().index

    def is_id(self, name):
        return name in self.get_ids().index

    def is_class(self, name):
        return name in self.get_classes().index

    def is_relationship_phantom(self, name):
        return name in self.get_phantom_relationships().index

    def is_hyperedge(self, name):
        return name in self.get_edges()["name"]

    def is_relationship(self, name):
        return name in self.get_relationships().index

    def is_struct(self, name):
        return name in self.get_structs().index

    def is_set(self, name):
        return name in self.get_sets().index

    def add_class(self, class_name, properties, att_list):
        """Besides the class name and the number of instances of the class, this method requires
        a list of attributes, where each attribute is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that can contain any key, but at least it should contain
        DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logging.info("Adding class "+class_name)
        if self.is_hyperedge(class_name):
            raise ValueError(f"Some hyperedge called '{class_name}' already exists")
        # First element in the pair is the name and the second its properties
        properties["Kind"] = 'Class'
        edges = [(class_name, properties)]
        # This adds a special attribute to identify instances in the class
        # First element in the pair is the node name and the second its properties
        nodes = [(class_name+'_ID', {'Kind': 'Identifier', 'DataType': 'Integer', 'Size': self.config.size_IDs, 'DistinctVals': properties["Count"]})]
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(class_name, class_name+'_ID', {'Kind': 'ClassIncidence', 'Direction': 'Inbound'})]
        for att in att_list:
            if att['name'] in self.get_nodes()["name"]:
                raise ValueError(f"Some node called '{att['name']}' already exists")
            att['prop']['Kind'] = 'Attribute'
            nodes.append((att['name'], att['prop']))
            incidences.append((class_name, att['name'], {'Kind': 'ClassIncidence', 'Direction': 'Outbound'}))
        self.H.add_nodes_from(nodes)
        self.H.add_edges_from(edges)
        self.H.add_incidences_from(incidences)

    def add_relationship(self, relationship_name, ends_list):
        """Besides the association name, this method requires
        a list of ends (usually should be only two), where each end is a dictionary with the keys 'name' and 'multiplicity'.
        The latter is another dictionary that contains
        DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logging.info("Adding relationship "+relationship_name)
        if self.is_hyperedge(relationship_name):
            raise ValueError(f"The hyperedge '{relationship_name}' already exists")
        if len(ends_list) != 2:
            raise ValueError(f"The relationship '{relationship_name}' should have exactly two ends, but has {len(ends_list)}")
        self.H.add_edge(relationship_name, Kind='Relationship')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node( 'Phantom_'+relationship_name, Kind='Phantom', Subkind='Relationship')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(relationship_name, 'Phantom_'+relationship_name, {'Kind': 'RelationshipIncidence', 'Direction': 'Inbound'})]
        for end in ends_list:
            end['prop']['Kind'] = 'RelationshipIncidence'
            end['prop']['Direction'] = 'Outbound'
            incidences.append((relationship_name, end['name']+'_ID', end['prop']))
        self.H.add_incidences_from(incidences)

    def add_struct(self, struct_name, root, elements):
        logging.info("Adding struct "+struct_name)
        if self.is_hyperedge(struct_name):
            raise ValueError(f"The hyperedge '{struct_name}' already exists")
        self.H.add_edge(struct_name, Kind='Struct')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node('Phantom_'+struct_name, Kind='Phantom', Subkind="Struct")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(struct_name, 'Phantom_'+struct_name, {'Kind': 'StructIncidence', 'Direction': 'Inbound'})]
        for elem in [root]+elements:
            if self.is_attribute(elem):
                incidences.append((struct_name, elem, {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Root': (elem == root)}))
            elif self.is_class(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Root': elem == root}))
            elif self.is_relationship(elem) or self.is_struct(elem) or self.is_set(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Root': elem == root}))
                for outbound_elem in self.get_outbounds().loc[elem].index:
                    if outbound_elem not in [self.get_phantom_of_edge_by_name(root)] + elements:
                        incidences.append((struct_name, outbound_elem, {'Kind': 'StructIncidence', 'Direction': 'Transitive'}))
                try:
                    for transitive_elem in self.get_transitives().loc[elem].index:
                        if transitive_elem not in [self.get_phantom_of_edge_by_name(root)] + elements:
                            incidences.append((struct_name, transitive_elem, {'Kind': 'StructIncidence', 'Direction': 'Transitive'}))
                except KeyError:
                    pass
            else:
                raise ValueError(f"Creating struct '{struct_name}' could not find '{elem}' to place it inside")
        self.H.add_incidences_from(incidences)

    def add_set(self, set_name, elements):
        logging.info("Adding set "+set_name)
        if set_name in self.get_edges()["name"]:
            raise ValueError(f"The hyperedge '{set_name}' already exists")
        if len(elements) == 0:
            raise ValueError(f"The set '{set_name}' should have some elements, but has {len(elements)}")
        self.H.add_edge(set_name, Kind='Set')
        # This adds a special phantom node required to represent different cases of inclusion in sets
        self.H.add_node('Phantom_'+set_name, Kind='Phantom', Subkind="Set")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(set_name, 'Phantom_'+set_name, {'Kind': 'SetIncidence', 'Direction': 'Inbound'})]
        for elem in elements:
            if self.is_attribute(elem):
                incidences.append((set_name, elem, {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_relationship(elem) or self.is_struct(elem):
                incidences.append((set_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
                for outbound_elem in self.get_outbounds().loc[elem].index:
                    if outbound_elem not in elements:
                        incidences.append((set_name, outbound_elem, {'Kind': 'SetIncidence', 'Direction': 'Transitive'}))
                try:
                    for transitive_elem in self.get_transitives().loc[elem].index:
                        if transitive_elem not in elements:
                            incidences.append((set_name, transitive_elem, {'Kind': 'SetIncidence', 'Direction': 'Transitive'}))
                except KeyError:
                    pass
            elif self.is_class(elem):
                raise ValueError(f"Sets cannot contain classes (adding '{elem}' into '{set_name}')")
            elif self.is_set(elem):
                raise ValueError(f"Sets cannot contain sets (adding '{elem}' into '{set_name}')")
            else:
                raise ValueError(f"Creating set '{set_name}' could not find '{elem}' to place it inside")
        self.H.add_incidences_from(incidences)

    def show_graphical(self):
        # Customize node graphical display
        node_colors = []
        node_labels = {}
        for i in self.H.nodes.dataframe['misc_properties'].items():
            node_labels[i[0]] = i[0]
            if i[1].get('Kind') == 'Identifier':
                node_colors.append('blue')
            elif i[1].get('Kind') == 'Attribute':
                node_colors.append('green')
            elif i[1].get('Kind') == 'Phantom':
                if self.config.phantom:
                    node_colors.append('yellow')
                else:
                    node_colors.append('white')
                    node_labels[i[0]] = ''
            else:
                raise ValueError(f"Undefined representation for node '{i[0]}' of kind '{i[1].get('Kind')}'")
        # Customize edge graphical display
        edge_lines = []
        for i in self.H.edges.dataframe['misc_properties'].items():
            if i[1].get('Kind') == 'Class':
                edge_lines.append('dotted')
            elif i[1].get('Kind') == 'Relationship':
                edge_lines.append('dashed')
            elif i[1].get('Kind') == 'Struct':
                edge_lines.append('dashdot')
            elif i[1].get('Kind') == 'Set':
                edge_lines.append('solid')
            else:
                raise ValueError(f"Wrong kind of edge {i[1].get('Kind')} for {i[0]}")

        # Graphical display
        fig = plt.figure(figsize=(4, 4))
        hnx.drawing.draw(self.H,
                         edge_labels_on_edge=True,
                         layout_kwargs={'seed': 666},
                         node_labels=node_labels,
                         nodes_kwargs={'facecolors': node_colors},
                         edges_kwargs={'linestyles': edge_lines, 'edgecolor': 'black'},
                         #'facecolors': edge_colors}, # This fills the edges, but they are not transparent
                         # edge_labels_kwargs={'color': 'black'} # This does not work
                         )
        plt.show()

    def show_textual(self):
        # Textual display
        print("-----------------------------------------------Nodes: ")
        display(self.H.nodes.dataframe)
        print("-----------------------------------------------Edges: ")
        display(self.H.edges.dataframe)
        print("------------------------------------------Incidences: ")
        display(self.H.incidences.dataframe)

    def is_correct(self, design=False):
        """This method checks all the integrity constrains of the catalog
        It can be expensive, so just do it at the end, not for each operation
        """
        correct = True
        edges = self.get_edges()
        incidences = self.get_incidences()
        ids = self.get_ids()
        phantoms = self.get_phantoms()
        attributes = self.get_attributes()
        classes = self.get_classes()
        relationships = self.get_relationships()
        structs = self.get_structs()
        sets = self.get_sets()
        inbounds = self.get_inbounds()
        structInbounds = self.get_inbound_structs()
        outbounds = self.get_outbounds()
        structOutbounds = self.get_outbound_structs()

        # -------------------------------------------------------------------------------------------------- Generic ICs
        # Pre-check emptiness
        logging.info("Checking emptiness")
        if self.get_nodes().shape[0] == 0 or self.get_edges().shape[0] == 0 or self.get_incidences().shape[0] == 0:
            print(f"This is a degenerated hypergraph: {self.get_nodes().shape[0]} nodes, {self.get_edges().shape[0]} edges, and {self.get_incidences().shape[0]} incidences")
            return False

        # IC-Generic1: Names must be unique
        logging.info("Checking IC-Generic1")
        union1_1 = pd.concat([self.get_nodes()["name"], self.get_edges()["name"]], ignore_index=True)
        violations1_1 = union1_1.groupby(union1_1).size()
        if violations1_1[violations1_1 > 1].shape[0] > 0:
            correct = False
            print("IC-Generic1 violation: Some names are not unique")
            display(violations1_1[violations1_1 > 1])

        # IC-Generic2: The catalog must be connected
        logging.info("Checking IC-Generic2")
        if not self.H.is_connected(s=1):
            correct = False
            print("IC-Generic2 violation: The catalog is not connected")

        # IC-Generic3: Every phantom belongs to one edge
        logging.info("Checking IC-Generic3")
        matches1_3 = inbounds.join(edges, on='edges', rsuffix='_edges', how='inner')
        violations1_3 = phantoms[~phantoms["name"].isin((matches1_3.reset_index(drop=False))["nodes"])]
        if violations1_3.shape[0] > 0:
            correct = False
            print("IC-Generic3 violation: There are phantoms without an edge")
            display(violations1_3)

        # IC-Generic4: Every edge has at least inbound
        logging.info("Checking IC-Generic4")
        matches1_4 = inbounds.join(edges, on='edges', rsuffix='_edges', how='inner')
        violations1_4 = edges[~edges["name"].isin((matches1_4.reset_index(drop=False))["edges"])]
        if violations1_4.shape[0] > 0:
            correct = False
            print("IC-Generic4 violation: There are edges without inbound")
            display(violations1_4)

        # IC-Generic5: Every edge has at least one outbound
        logging.info("Checking IC-Generic5")
        matches1_5 = outbounds.join(edges, on='edges', rsuffix='_edges', how='inner')
        violations1_5 = edges[~edges["name"].isin((matches1_5.reset_index(drop=False))["edges"])]
        if violations1_5.shape[0] > 0:
            correct = False
            print("IC-Generic4 violation: There are edges without outbound")
            display(violations1_5)

        # IC-Generic6: An edge cannot have more than one inbound
        logging.info("Checking IC-Generic6")
        violations1_6 = inbounds.groupby(inbounds.index.get_level_values('edges')).size()
        if violations1_6[violations1_6 > 1].shape[0] > 0:
            correct = False
            print("IC-Generic6 violation: There are edges with more than one inbound")
            display(violations1_6[violations1_6 > 1])

        # IC-Generic7: An edge cannot be cyclic
        logging.info("Checking IC-Generic7->To be implemented")
        # violations1_7 = inbounds.groupby(inbounds.index.get_level_values('edges')).size()
        # if violations1_7[violations1_7 > 1].shape[0] > 0:
        #     correct = False
        #     print("IC-Generic6 violation: There are cyclic edges")
        #     display(violations1_7[violations1_7 > 1])

        # ------------------------------------------------------------------------------------------------- ICs on atoms
        # IC-Atoms1: Every class has one ID which is inbound
        logging.info("Checking IC-Atoms1")
        matches2_1 = inbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        violations2_1 = classes[~classes["name"].isin((matches2_1.reset_index(drop=False))["edges"])]
        if violations2_1.shape[0] > 0:
            correct = False
            print("IC-Atoms1 violation: There are classes without identifier")
            display(violations2_1)

        # IC-Atoms2: Every ID belongs to one class which is inbound
        logging.info("Checking IC-Atoms2")
        matches2_2 = inbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_2 = ids[~ids["name"].isin((matches2_2.reset_index(drop=False))["nodes"])]
        if violations2_2.shape[0] > 0:
            correct = False
            print("IC-Atoms2 violation: There are IDs without a class")
            display(violations2_2)

        # IC-Atoms3: Every attribute must belong at least one class which is outbound
        logging.info("Checking IC-Atoms3")
        matches2_3 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_3 = attributes[~attributes["name"].isin((matches2_3.reset_index(drop=False))["nodes"])]
        if violations2_3.shape[0] > 0:
            correct = False
            print("IC-Atoms3 violation: There are attributes without a class")
            display(violations2_3)

        # IC-Atoms4: An attribute cannot belong to more than one class
        logging.info("Checking IC-Atoms4")
        matches2_4 = incidences.join(classes, on='edges', rsuffix='_edges', how='inner').join(attributes, on='nodes', rsuffix='_nodes', how='inner')
        violations2_4 = matches2_4.groupby(matches2_4.index.get_level_values('nodes')).size()
        if violations2_4[violations2_4 > 1].shape[0] > 0:
            correct = False
            print("IC-Atoms4 violation: There are attributes with more than one class")
            display(violations2_4[violations2_4 > 1])

        # IC-Atoms5: The number of different values of an attribute must be less than the cardinality of its class
        logging.info("Checking IC-Atoms5")
        matches2_5 = outbounds.join(attributes, on='nodes', rsuffix='_nodes', how='inner').join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_5 = matches2_5[matches2_5.apply(lambda row: row["misc_properties_nodes"]["DistinctVals"] > row["misc_properties_edges"]["Count"], axis=1)]
        if violations2_5.shape[0] > 0:
            correct = False
            print("IC-Atoms5 violation: The number of different values of an attribute is greater than the cardinality of its class")
            display(violations2_5)

        # IC-Atoms6: Every relationship has one phantom
        logging.info("Checking IC-Atoms6")
        matches2_6 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
        violations2_6 = relationships[~relationships["name"].isin((matches2_6.reset_index(drop=False))["edges"])]
        if violations2_6.shape[0] > 0:
            correct = False
            print("IC-Atoms6 violation: There are relationships without phantom")
            display(violations2_6)

        # IC-Atoms7: Every relationship has two ends (Definition 4)
        logging.info("Checking IC-Atoms7")
        matches2_7 = incidences.join(ids, on='nodes', rsuffix='_nodes', how='inner').join(relationships, on='edges', rsuffix='_edges', how='inner')
        violations2_7 = matches2_7.groupby(matches2_7.index.get_level_values('edges')).size()
        if violations2_7[violations2_7 != 2].shape[0] > 0:
            correct = False
            print("IC-Atoms7 violation: There are non-binary relationships")
            display(violations2_7[violations2_7 != 2])

        # Not necessary to check from here on if the catalog only contains the atoms in the schema
        if design:
            # ------------------------------------------------------------------------------------------- ICs on structs
            # IC-Structs1: Every struct has one phantom
            logging.info("Checking IC-Structs1")
            matches3_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations3_1 = structs[~structs["name"].isin((matches3_1.reset_index(drop=False))["edges"])]
            if violations3_1.shape[0] > 0:
                correct = False
                print("IC-Structs1 violation: There are structs without phantom")
                display(violations3_1)

            # IC-Structs2: Structs are transitive on themselves
            logging.info("Checking IC-Structs2")
            matches3_2_partial = structOutbounds.reset_index(drop=False).set_index('nodes', drop=False, verify_integrity=False).rename_axis("joinattr") \
                            .join(
                                structInbounds.reset_index(drop=False).set_index('nodes', drop=False, verify_integrity=False).rename_axis("joinattr"),
                                on='joinattr', rsuffix='_firsthop', how='inner')
            matches3_2 = matches3_2_partial.set_index('edges_firsthop', drop=False, verify_integrity=False).rename_axis("joinattr") \
                            .join(
                                structOutbounds.reset_index(drop=False).set_index('edges', drop=False, verify_integrity=False).rename_axis("joinattr"),
                                on='joinattr', rsuffix='_secondhop', how='inner')[["edges", "nodes_secondhop"]].reset_index(drop=True).rename(columns={"nodes_secondhop": "nodes"})
            violations3_2 = df_difference(matches3_2, self.get_transitives().reset_index(drop=False)[["edges", "nodes"]])
            if violations3_2.shape[0] > 0:
                correct = False
                print("IC-Structs2 violation: There are missing elements in some struct")
                display(violations3_2)

            # IC-Structs3: Every struct has one root
            logging.info("Checking IC-Structs3")
            matches3_3 = outbounds[outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Root', False))].groupby('edges').size()
            violations3_3 = structs[~structs["name"].isin((matches3_3[matches3_3 == 1].reset_index(drop=False))["edges"])]
            if violations3_3.shape[0] > 0:
                correct = False
                print("IC-Structs3 violation: There are structs without exactly one root")
                display(violations3_3)

            # IC-Structs-b: All attributes in a struct are connected to its root by a unique path of relationships, which are all part of the struct, too (Definition 7-b)
            logging.info("Checking IC-Structs-b -> To be implemented")

            # IC-Structs-c: All roots of structs inside a struct are connected to its root by a unique path of relationships, which are all part of the struct, too (Definition 7-c)
            logging.info("Checking IC-Structs-c -> To be implemented")

            # IC-Structs-d: All sets inside a struct must contain a unique path of relationships connecting the parent struct to either the attribute or root of the struct inside the set (Definition 7-d)
            logging.info("Checking IC-Structs-d -> To be implemented")

            # IC-Structs-e: (Definition 7-e)
            logging.info("Checking IC-Structs-e -> To be implemented")

            # ---------------------------------------------------------------------------------------------- ICs on sets
            # IC-Sets1: Every set has one phantom
            logging.info("Checking IC-Sets1")
            matches4_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations4_1 = sets[~sets["name"].isin((matches4_1.reset_index(drop=False))["edges"])]
            if violations4_1.shape[0] > 0:
                correct = False
                print("IC-Sets1 violation: There are sets without phantom")
                display(violations4_1)

            # IC-Sets2: Sets are transitive on structs
            logging.info("Checking IC-Sets2 -> To be implemented")

            # IC-Sets3: Sets cannot directly contain classes
            logging.info("Checking IC-Sets3 -> To be implemented")

            # IC-Sets4: Sets cannot directly contain other sets
            logging.info("Checking IC-Sets4 -> To be implemented")

            # ----------------------------------------------------------------------------------------- ICs about design
            # IC-Design1: All the first levels must be sets
            logging.info("Checking IC-Design1")
            matches5_1 = self.get_inbound_firstLevel()
            violations5_1 = matches5_1[~matches5_1["misc_properties"].apply(lambda x: x['Kind'] == 'SetIncidence')]
            if violations5_1.shape[0] > 0:
                correct = False
                print("IC-Design1 violation: All first levels must be sets")
                display(violations5_1)

            # IC-Design2: All the atoms in the schema are connected to the first level
            logging.info("Checking IC-Design2")
            matches5_2 = self.get_inbound_firstLevel().join(pd.concat([self.get_outbounds(), self.get_transitives()]).reset_index(level="nodes"), on="edges", rsuffix='_tokeep', how='inner')
            atoms5_2 = pd.concat([self.get_ids(), self.get_attributes(), self.get_phantom_relationships()])
            violations5_2 = atoms5_2[~atoms5_2["name"].isin(matches5_2["nodes"])]
            if violations5_2.shape[0] > 0:
                correct = False
                print("IC-Design2 violation: Atoms disconnected from the first level")
                display(violations5_2)

        return correct
