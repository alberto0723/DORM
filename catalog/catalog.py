import logging
from pathlib import Path
import hypernetx as hnx
import matplotlib
matplotlib.use('Qt5Agg') #This sets the backend to plot (default TkAgg does not work)
import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display


class Catalog:
    """This class manages the catalog of a database using hypergraphs
    It uses HyperNetX (https://github.com/pnnl/HyperNetX)
    """
    def __init__(self, config):
        self.H = hnx.Hypergraph([])
        self.config = config

    def add_class(self, class_name, cardinality, att_list):
        """Besides the class name and the number of instances of the class, this method requires
        a list of attributes, where each attribute is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that can contain any key, but at least it should contain
        DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logging.info("Adding class "+class_name)
        if class_name in self.H.edges.dataframe.index:
            raise ValueError(f"The class '{class_name}' already exists")
        self.H.add_edge(class_name, Kind='Class', Count=cardinality)
        # This adds a special attribute to identify instances in the class
        # First element in the pair is the node name and the second its properties
        nodes = [(class_name+'_ID', {'Kind': 'Identifier', 'DataType': 'Serial', 'Size': 8, 'DistinctVals': cardinality})]
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(class_name, class_name+'_ID', {'Direction': 'Inbound'})]
        for att in att_list:
            if att['name'] in self.H.nodes.dataframe.index:
                raise ValueError(f"The attribute '{att['name']}' already exists")
            prop = att['prop']
            prop['Kind'] = 'Attribute'
            nodes.append((att['name'], prop))
            incidences.append((class_name, att['name'], {'Direction': 'Outbound'}))
        self.H.add_nodes_from(nodes)
        self.H.add_incidences_from(incidences)

    def add_relationship(self, relationship_name, ends_list):
        """Besides the association name, this method requires
        a list of ends (usually should be only two), where each end is a dictionary with the keys 'name' and 'multiplicity'.
        The latter is another dictionary that contains
        DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logging.info("Adding relationship "+relationship_name)
        if relationship_name in self.H.edges.dataframe.index:
            raise ValueError(f"The relationship '{relationship_name}' already exists")
        self.H.add_edge(relationship_name, Kind='Relationship')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node( 'Phantom_'+relationship_name, Kind='Phantom')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(relationship_name, 'Phantom_'+relationship_name, {'Direction': 'Inbound'})]
        for end in ends_list:
            incidences.append((relationship_name, end['name']+'_ID', {'Direction': 'Outbound', 'Multiplicity': end['multiplicity']}))
        self.H.add_incidences_from(incidences)

    def show_graphical(self, extra = True):
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
                    node_colors.append('blue')
                else:
                    node_colors.append('white')
                    node_labels[i[0]] = ''
            else:
                node_colors.append('red')
        # Customize edge graphical display
        edge_lines = []
        edge_colors = []
        for i in self.H.edges.dataframe['misc_properties'].items():
            if i[1].get('Kind') == 'Class':
                edge_lines.append('dotted')
                edge_colors.append('white')
            elif i[1].get('Kind') == 'Relationship':
                edge_lines.append('solid')
                edge_colors.append('white')
            else:
                edge_lines.append('solid')
                edge_colors.append('lightblue')
        # Graphical display
        fig = plt.figure(figsize=(4, 4))
        hnx.drawing.draw(self.H,
                         edge_labels_on_edge=True,
                         node_labels=node_labels,
                         nodes_kwargs={'facecolors': node_colors},
                         edges_kwargs={'linestyles': edge_lines, 'facecolors': edge_colors, 'edgecolor': 'black'},
                         #edge_labels_kwargs={'color': 'black'}
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

    def is_correct(self):
        """This method checks all the integrity constrains of the catalog
        It can be expensive, so just do it at the end, not for each operation
        """
        correct = True
        nodes = self.H.nodes.dataframe.rename_axis("nodes")
        nodes["name"] = nodes.index
        edges = self.H.edges.dataframe.rename_axis("edges")
        edges["name"] = edges.index
        incidences = self.H.incidences.dataframe
        ids = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Identifier')]
        attributes = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Attribute')]
        classes = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Class')]
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound')]

        # IC1: The catalog must be connected
        logging.info("Checking IC1")
        if not self.H.is_connected(s=1):
            correct = False
            print("IC1 violation: The catalog is not connected")

        # IC2: Every class has one ID which is inbound
        logging.info("Checking IC2")
        matches2 = inbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        violations2 = classes[~classes["name"].isin((matches2.reset_index(drop=False))["edges"])]
        if violations2.shape[0] > 0:
            correct = False
            print("IC2 violation: There are classes without identifier")
            display(violations2)

        # IC3: Every ID belongs to one class which is inbound
        logging.info("Checking IC3")
        matches3 = inbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations3 = ids[~ids["name"].isin((matches3.reset_index(drop=False))["nodes"])]
        if violations3.shape[0] > 0:
            correct = False
            print("IC3 violation: There are IDs without a class")
            display(violations3)

        # IC4: Every attribute must belong at least one class which is outbound
        logging.info("Checking IC4")
        matches4 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations4 = attributes[~attributes["name"].isin((matches4.reset_index(drop=False))["nodes"])]
        if violations4.shape[0] > 0:
            correct = False
            print("IC4 violation: There are attributes without a class")
            display(violations4)

        # IC5: An attribute cannot belong to more than one class
        logging.info("Checking IC5->Not implemented yet")
        matches5 = incidences.join(classes, on='edges', rsuffix='_edges', how='inner')

        # IC6: The number of different values of an attribute must be less than the cardinality of its class
        logging.info("Checking IC6")
        matches6 = outbounds.join(attributes, on='nodes', rsuffix='_nodes', how='inner').join(classes, on='edges', rsuffix='_edges', how='inner')
        violations6 = matches6[matches6.apply(lambda row: row["misc_properties_nodes"]["DistinctVals"] > row["misc_properties_edges"]["Count"], axis=1)]
        if violations6.shape[0] > 0:
            correct = False
            print("IC6 violation: The number of different values of an attribute is greater than the cardinality of its class")
            display(violations6)

        return correct
