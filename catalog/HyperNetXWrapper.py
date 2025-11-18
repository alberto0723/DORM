import logging
import hypernetx as hnx
import os
import pickle
from IPython.display import display
import matplotlib.pyplot as plt
import matplotlib

from .config import Config

matplotlib.use('Qt5Agg')  # This sets the backend to plot (default TkAgg does not work)

logger = logging.getLogger("HyperNetXWrapper")


class HyperNetXWrapper:
    """This class manages the basics of the catalog of a database using hypergraphs.
    It uses HyperNetX (https://github.com/pnnl/HyperNetX)
    It implements all the basic stuff and auxiliary, private functions of the catalog to simplify the use of the library.
    """
    def __init__(self, file_path=None, hypergraph=None):
        self.config = Config()
        if hypergraph is not None:
            self.H = hypergraph
        elif file_path is not None:
            logger.info(f"Loading hypergraph from '{file_path}'")
            with open(file_path, "rb") as f:
                self.H = pickle.load(f)
        else:
            # In this case, the hypergraph will be filled with load_domain or load_design
            self.H = hnx.Hypergraph([])

    def save(self, file_path=None) -> None:
        if file_path is not None:
            logger.info(f"Saving hypergraph in '{file_path}'")
            # Create the directory (if it doesn't exist)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Save the hypergraph to a pickle file
            with open(file_path, "wb") as f:
                pickle.dump(self.H, f)

    def add_class(self, class_name, properties, att_list) -> None:
        """Besides the class name and the number of instances of the class, this method requires
        a list of attributes, where each attribute is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that can contain any key, but at least it should contain
        'DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logger.info("Adding class "+class_name)
        if self.is_attribute_in_H(class_name) or self.is_association_end_in_H(class_name) or self.is_edge_in_H(class_name):
            raise ValueError(f"🚨 Some element called '{class_name}' already exists")
        # First element in the pair is the name and the second its properties
        properties["Kind"] = 'Class'
        edges = [(class_name, properties)]
        # This adds a special attribute to identify instances in the class
        # First element in the pair is the node name and the second its properties
        nodes = [(self.config.prepend_phantom+class_name, {'Kind': 'Phantom', 'Subkind': 'Class'})]
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(class_name, self.config.prepend_phantom+class_name, {'Kind': 'ClassIncidence', 'Direction': 'Inbound'})]
        # Check if attribute names are repeated
        unique_attr = set([att["name"] for att in att_list])
        if len(unique_attr) < len(att_list):
            raise ValueError(f"🚨 Some attribute in '{class_name}' is repeated")
        for att in att_list:
            if self.is_attribute_in_H(att['name']) or self.is_association_end_in_H(att['name']) or self.is_edge_in_H(att['name']):
                raise ValueError(f"🚨 Some element end called '{att['name']}' already exists")
            incidence_properties = {'Kind': 'ClassIncidence',
                                    'Direction': 'Outbound',
                                    'DistinctVals': att['prop'].pop('DistinctVals'),
                                    'Identifier': att['prop'].pop('Identifier', False)}
            incidences.append((class_name, att['name'], incidence_properties))
            if att['name'] in self.get_nodes():
                if att['prop']['DataType'] != self.H.get_properties(att['name'], level=1, prop_name="DataType"):
                    raise ValueError(f"🚨 Some node called '{att['name']}' already exists, but its DataType does not coincide")
                if att['prop']['Size'] != self.H.get_properties(att['name'], level=1, prop_name="Size"):
                    raise ValueError(f"🚨 Some node called '{att['name']}' already exists, but its Size does not coincide")
            else:
                att['prop']['Kind'] = 'Attribute'
                nodes.append((att['name'], att['prop']))
        self.H.add_nodes_from(nodes)
        self.H.add_edges_from(edges)
        self.H.add_incidences_from(incidences)

    def add_association(self, association_name, ends_list) -> None:
        """Besides the association name, this method requires
        a list of ends (usually should be only two), where each end is a dictionary with the keys 'name' and 'multiplicity'.
        The latter is another dictionary that contains
        'DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logger.info("Adding association "+association_name)
        if self.is_attribute_in_H(association_name) or self.is_association_end_in_H(association_name) or self.is_edge_in_H(association_name):
            raise ValueError(f"🚨 The element '{association_name}' already exists")
        if len(ends_list) != 2:
            raise ValueError(f"🚨 The association '{association_name}' should have exactly two ends, but has {len(ends_list)}")
        self.H.add_edge(association_name, Kind='Association')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+association_name, Kind='Phantom', Subkind='Association')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(association_name, self.config.prepend_phantom+association_name, {'Kind': 'AssociationIncidence', 'Direction': 'Inbound'})]
        for end in ends_list:
            if not self.is_class_in_H(end['class']):
                raise ValueError(f"🚨 The class '{end['class']}' in '{association_name}' does not exists")
            end_name = end['prop'].get('End_name', None)
            if end_name is None:
                raise ValueError(f"🚨 Association end '{association_name}' does not have a name for its end towards '{end['class']}'")
            if self.is_attribute_in_H(end_name) or self.is_association_end_in_H(end_name) or self.is_edge_in_H(end_name):
                raise ValueError(f"🚨 There is already an element called '{end_name}'")
            if end['prop'].get('MultiplicityMax', None) is None or end['prop'].get('MultiplicityMin', None) is None:
                raise ValueError(f"🚨 '{association_name}' does not have both min and max multiplicity for its end '{end_name}'")
            end['prop']['Kind'] = 'AssociationIncidence'
            end['prop']['Direction'] = 'Outbound'
            incidences.append((association_name, self.get_phantom_of_edge_by_name_in_H(end['class']), end['prop']))
        self.H.add_incidences_from(incidences)

    def add_generalization(self, generalization_name, properties, superclass, subclasses_list) -> None:
        """ Besides the generalization name, this method requires some properties (expected to be two booleans) for
        disjointness and completeness, the name of the superclass and a list of subclasses,
        where each subclass is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that contains at least one constraint predicate that discriminates the subclass.
        """
        logger.info("Adding generalization "+generalization_name)
        if self.is_attribute_in_H(generalization_name) or self.is_association_end_in_H(generalization_name) or self.is_edge_in_H(generalization_name):
            raise ValueError(f"🚨 The element called '{generalization_name}' already exists")
        self.H.add_edge(generalization_name, Kind='Generalization', Disjoint=properties.get('Disjoint', False), Complete=properties.get('Complete', False))
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+generalization_name, Kind='Phantom', Subkind='Generalization')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(generalization_name, self.config.prepend_phantom+generalization_name, {'Kind': 'GeneralizationIncidence', 'Direction': 'Inbound'})]
        if not self.is_class_in_H(superclass):
            raise ValueError(f"🚨 The superclass '{superclass}' in '{generalization_name}' does not exists")
        # First element in the pair of incidences is the edge name and the second the node
        incidences.append((generalization_name,  self.get_phantom_of_edge_by_name_in_H(superclass), {'Kind': 'GeneralizationIncidence', 'Subkind': 'Superclass', 'Direction': 'Outbound'}))
        if len(subclasses_list) < 1:
            raise ValueError(f"🚨 The generalization '{generalization_name}' should have at least one subclass")
        for sub in subclasses_list:
            if superclass == sub['class']:
                raise ValueError(f"🚨 The same class '{superclass}' cannot play super and sub roles in generalization '{generalization_name}'")
            if not self.is_class_in_H(sub['class']):
                raise ValueError(f"🚨 The subclass '{superclass}' in '{generalization_name}' does not exists")
            sub['prop']['Kind'] = 'GeneralizationIncidence'
            sub['prop']['Subkind'] = 'Subclass'
            sub['prop']['Direction'] = 'Outbound'
            incidences.append((generalization_name, self.get_phantom_of_edge_by_name_in_H(sub['class']), sub['prop']))
        self.H.add_incidences_from(incidences)

    def add_struct(self, struct_name, anchor, elements) -> None:
        logger.info("Adding struct "+struct_name)
        if self.is_edge_in_H(struct_name):
            raise ValueError(f"🚨 The hyperedge '{struct_name}' already exists")
        if not anchor:
            raise ValueError(f"🚨 Struct '{struct_name}' does not have any anchor")
        for elem in anchor:
            if not self.is_class_in_H(elem) and not self.is_association_in_H(elem):
                raise ValueError(f"🚨 The anchor of '{struct_name}' (i.e., '{elem}') must be either a class or an association")
        self.H.add_edge(struct_name, Kind='Struct')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+struct_name, Kind='Phantom', Subkind="Struct")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(struct_name, self.config.prepend_phantom+struct_name, {'Kind': 'StructIncidence', 'Direction': 'Inbound'})]
        for elem in list(set(elements+anchor)):
            if self.is_attribute_in_H(elem):
                incidences.append((struct_name, elem, {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_association_in_H(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name_in_H(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_class_in_H(elem):
                # Add the class to the struct
                incidences.append((struct_name, self.get_phantom_of_edge_by_name_in_H(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
                # Add the identifier to the struct
                incidences.append((struct_name, self.get_class_id_by_name_in_H(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': False}))
                # We do need to have the generalizations in the struct to generate a restricted struct correctly including superclasses
                for g in self.get_generalizations_by_class_name_in_H(elem, return_superclasses=False, visited=[]):
                    incidences.append((struct_name, self.get_phantom_of_edge_by_name_in_H(g), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': False}))
            elif self.is_struct_in_H(elem) or self.is_set_in_H(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name_in_H(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_generalization_in_H(elem):
                pass
            else:
                raise ValueError(f"🚨 Creating struct '{struct_name}' could not find '{elem}' to place it inside (check both domain and design)")
        self.H.add_incidences_from(incidences)

    def add_set(self, set_name, elements) -> None:
        logger.info("Adding set "+set_name)
        if set_name in self.get_edges():
            raise ValueError(f"🚨 The hyperedge '{set_name}' already exists")
        if len(elements) == 0:
            raise ValueError(f"🚨 The set '{set_name}' should have some elements, but has {len(elements)}")
        self.H.add_edge(set_name, Kind='Set')
        # This adds a special phantom node required to represent different cases of inclusion in sets
        self.H.add_node('Phantom_'+set_name, Kind='Phantom', Subkind="Set")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(set_name, self.config.prepend_phantom+set_name, {'Kind': 'SetIncidence', 'Direction': 'Inbound'})]
        for elem in elements:
            if self.is_class_in_H(elem):
                incidences.append((set_name, self.get_phantom_of_edge_by_name_in_H(elem), {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_association_in_H(elem) or self.is_struct_in_H(elem):
                incidences.append((set_name, self.get_phantom_of_edge_by_name_in_H(elem), {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_attribute_in_H(elem):
                raise ValueError(f"🚨 Sets cannot contain attributes (adding '{elem}' into '{set_name}')")
            elif self.is_set_in_H(elem):
                raise ValueError(f"🚨 Sets cannot contain sets (adding '{elem}' into '{set_name}')")
            else:
                raise ValueError(f"🚨 Creating set '{set_name}' could not find the kind of '{elem}' to place it inside (the element may not exist in the domain)")
        self.H.add_incidences_from(incidences)

    def is_attribute_in_H(self, attribute_name) -> bool:
        if attribute_name in self.H.nodes.dataframe.index:
            return self.H.nodes.dataframe.loc[attribute_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Attribute'
        else:
            return False

    def is_edge_in_H(self, edge_name) -> bool:
        return edge_name in self.H.edges.dataframe.index

    def is_class_in_H(self, class_name) -> bool:
        if class_name in self.H.edges.dataframe.index:
            return self.H.edges.dataframe.loc[class_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Class'
        else:
            return False

    def is_association_in_H(self, association_name) -> bool:
        if association_name in self.H.edges.dataframe.index:
            return self.H.edges.dataframe.loc[association_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Association'
        else:
            return False

    def is_generalization_in_H(self, generalization_name) -> bool:
        if generalization_name in self.H.edges.dataframe.index:
            return self.H.edges.dataframe.loc[generalization_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Generalization'
        else:
            return False

    def is_struct_in_H(self, struct_name) -> bool:
        if struct_name in self.H.edges.dataframe.index:
            return self.H.edges.dataframe.loc[struct_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Struct'
        else:
            return False

    def is_set_in_H(self, set_name) -> bool:
        if set_name in self.H.edges.dataframe.index:
            return self.H.edges.dataframe.loc[set_name].get("misc_properties", {}).get('Kind', "FakeValue") == 'Set'
        else:
            return False

    def is_association_end_in_H(self, end_name) -> bool:
        ends = self.H.incidences.dataframe[self.H.incidences.dataframe["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                             x['Kind'] == 'AssociationIncidence' and
                                                                             x['End_name'] == end_name)]
        return not ends.empty

    def get_classes_in_H(self) -> list[str]:
        edges = self.H.edges.dataframe
        return edges[edges['misc_properties'].apply(lambda prop: prop['Kind'] == 'Class')].index.tolist()

    def get_attribute_names_in_H(self, H: hnx.Hypergraph = None) -> list[str]:
        if H is None:
            nodes = self.H.nodes.dataframe
        else:
            nodes = H.nodes.dataframe
        attribute_names = nodes[nodes["misc_properties"].apply(lambda prop: prop['Kind'] == 'Attribute')]
        return attribute_names.index.values.tolist()

    def get_association_ends_in_H(self, H: hnx.Hypergraph = None) -> list[str]:
        if H is None:
            incidences = self.H.incidences.dataframe
        else:
            incidences = H.incidences.dataframe
        association_ends = incidences[incidences["misc_properties"].apply(lambda prop: prop['Direction'] == 'Outbound' and prop['Kind'] == 'AssociationIncidence')]
        return association_ends['misc_properties'].apply(lambda prop: prop['End_name']).values.tolist()

    def get_edge_by_phantom_name_in_H(self, phantom_name) -> str:
        phantom_incidences = self.H.incidences.dataframe.xs(phantom_name, level="nodes", drop_level=False)
        phantom_inbounds = phantom_incidences[phantom_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return phantom_inbounds.index[0][0]

    def get_phantom_of_edge_by_name_in_H(self, edge_name) -> str:
        edge_incidences = self.H.incidences.dataframe.xs(edge_name, level="edges", drop_level=False)
        edge_inbounds = edge_incidences[edge_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return edge_inbounds.index[0][1]

    def get_generalizations_by_class_name_in_H(self, class_name, return_superclasses: bool, visited: list[str] = None) -> list[str]:
        if visited is None:
            visited = []
        phantom_name = self.get_phantom_of_edge_by_name_in_H(class_name)
        incidences = self.H.incidences.dataframe
        subclass_outbounds = incidences[(incidences.index.get_level_values("nodes") == phantom_name) &
                                        (incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                             x['Kind'] == 'GeneralizationIncidence' and
                                                                             x['Subkind'] == 'Subclass'))]
        direct_superclass = incidences[(incidences.index.get_level_values("edges").isin(subclass_outbounds.index.get_level_values("edges"))) &
                                       (incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                             x['Kind'] == 'GeneralizationIncidence' and
                                                                             x['Subkind'] == 'Superclass'))]
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            generalization = direct_superclass.index[0][0]
            superclass = self.get_edge_by_phantom_name_in_H(direct_superclass.index[0][1])
            assert superclass not in visited, f"☠️ Generalization cycle found for '{superclass}' in '{visited}'"
            if return_superclasses:
                return [superclass]+self.get_generalizations_by_class_name_in_H(superclass, return_superclasses, visited + [class_name])
            else:
                return [generalization]+self.get_generalizations_by_class_name_in_H(superclass, return_superclasses, visited + [class_name])

    def get_class_id_by_name_in_H(self, class_name) -> str:
        superclasses = self.get_generalizations_by_class_name_in_H(class_name, return_superclasses=True)
        incidences = self.H.incidences.dataframe
        if not superclasses:
            class_incidences = incidences.xs(class_name, level="edges", drop_level=False)
        else:
            # The top of the hierarchy should be the first in the list
            class_incidences = incidences.xs(superclasses[-1], level="edges", drop_level=False)
        class_id = class_incidences[class_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                         x['Kind'] == 'ClassIncidence' and
                                                                                         x['Identifier'])]
        assert not class_id.empty, f"Class {class_name} does not have an identifier"
        return class_id.index[0][1]

    def show_textual(self) -> None:
        # Textual display
        print("-----------------------------------------------Nodes: ")
        display(self.H.nodes.dataframe)
        print("-----------------------------------------------Edges: ")
        display(self.H.edges.dataframe)
        print("------------------------------------------Incidences: ")
        display(self.H.incidences.dataframe)

    def show_graphical(self) -> None:
        # Customize node graphical display
        node_colors = []
        node_labels = {}
        for i in self.H.nodes.dataframe['misc_properties'].items():
            node_labels[i[0]] = i[0]
            assert i[1].get('Kind') in ['Identifier', 'Attribute', 'Phantom'], f"☠️ Undefined representation for node '{i[0]}' of kind '{i[1].get('Kind')}'"
            if i[1].get('Kind') == 'Identifier':
                node_colors.append('blue')
            elif i[1].get('Kind') == 'Attribute':
                node_colors.append('green')
            elif i[1].get('Kind') == 'Phantom':
                if self.config.show_phantoms:
                    node_colors.append('yellow')
                else:
                    node_colors.append('white')
                    node_labels[i[0]] = ''
        # Customize edge graphical display
        edge_lines = []
        for i in self.H.edges.dataframe['misc_properties'].items():
            assert i[1].get('Kind') in ['Class', 'Relationship', 'Struct', 'Set'], f"☠️ Wrong kind of edge {i[1].get('Kind')} for {i[0]}"
            if i[1].get('Kind') == 'Class':
                edge_lines.append('dotted')
            elif i[1].get('Kind') == 'Relationship':
                edge_lines.append('dashed')
            elif i[1].get('Kind') == 'Struct':
                edge_lines.append('dashdot')
            elif i[1].get('Kind') == 'Set':
                edge_lines.append('solid')

        # Graphical display
        hnx.drawing.draw(self.H,
                         edge_labels_on_edge=True,
                         layout_kwargs={'seed': 666},
                         node_labels=node_labels,
                         nodes_kwargs={'facecolors': node_colors},
                         edges_kwargs={'linestyles': edge_lines, 'edgecolor': 'black'},
                         # 'facecolors': edge_colors}, # This fills the edges, but they are not transparent
                         # edge_labels_kwargs={'color': 'black'} # This does not work
                         )
        plt.show()
