import logging
import os
import hypernetx as hnx
import networkx as nx
import pickle
from IPython.display import display
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
import sqlparse

from . import config
from .tools import drop_duplicates, df_difference, show_textual_hypergraph, show_graphical_hypergraph

logger = logging.getLogger("Catalog")

class Catalog:
    """This class manages the catalog of a database using hypergraphs
    It uses HyperNetX (https://github.com/pnnl/HyperNetX)
    """
    def __init__(self, file=None):
        self.config = config.Config()
        if file is None:
            self.H = hnx.Hypergraph([])
        else:
            logger.info("Loading hypergraph from " + str(file))
            with open(file, "rb") as f:
                self.H = pickle.load(f)

    def save(self, file):
        logger.info("Saving hypergraph in " + str(file))
        # Create the directory (if it doesn't exist)
        os.makedirs(os.path.dirname(file), exist_ok=True)
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

    def get_edges_firstlevel(self):
        firstLevelPhantoms = df_difference(
            pd.concat([self.get_inbound_structs(), self.get_inbound_sets()], ignore_index=False).reset_index()[["nodes"]],
                self.get_outbounds().reset_index()[["nodes"]])
        firstLevelEdges = pd.merge(firstLevelPhantoms, pd.concat([self.get_inbound_structs(), self.get_inbound_sets()], ignore_index=False).reset_index(drop=False), on="nodes", how="inner")
        return firstLevelEdges

    def get_incidences(self):
        incidences = self.H.incidences.dataframe
        return incidences

    def get_attributes(self):
        nodes = self.get_nodes()
        attributes = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Attribute')]
        return attributes

    def get_ids(self):
        outbounds = self.get_outbound_classes()
        incidences = outbounds[outbounds["misc_properties"].apply(lambda x: x['Identifier'])].reset_index(level='edges', drop=True)
        ids = self.get_attributes()[self.get_attributes()["name"].isin(incidences.index)]
        return ids

    def get_class_id_by_name(self, class_name):
        superclasses = self.get_superclasses_by_class_name(class_name, [])
        if not superclasses:
            class_outbounds = self.get_outbound_class_by_name(class_name)
        else:
            # The top of the hierarchy should be the first in the list
            class_outbounds = self.get_outbound_class_by_name(superclasses[0])
        class_id = class_outbounds[class_outbounds["misc_properties"].apply(lambda x: x['Identifier'])]
        if class_id.empty:
            return None
        else:
            return class_id.index[0][1]

    def get_phantoms(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom')]
        return phantoms

    def get_phantom_classes(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and x['Subkind'] == 'Class')]
        return phantoms

    def get_phantom_associations(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and x['Subkind'] == 'Association')]
        return phantoms

    def get_phantom_generalizations(self):
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and x['Subkind'] == 'Generalization')]
        return phantoms

    def get_edge_by_phantom_name(self, phantom_name):
        return self.get_inbounds()[self.get_inbounds().index.get_level_values('nodes') == phantom_name].index[0][0]

    def get_phantom_of_edge_by_name(self, edge_name):
        return self.get_inbounds().loc[edge_name].index[0]

    def get_classes(self):
        edges = self.get_edges()
        classes = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Class')]
        return classes

    def get_associations(self):
        edges = self.get_edges()
        associations = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Association')]
        return associations

    def get_generalizations(self):
        edges = self.get_edges()
        associations = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Generalization')]
        return associations

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

    def get_inbound_classes(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'ClassIncidence')]
        return inbounds

    def get_inbound_associations(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'AssociationIncidence')]
        return inbounds

    def get_inbound_generalizations(self):
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and x.get('Kind') == 'GeneralizationIncidence')]
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

    def get_outbound_associations(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'AssociationIncidence')]
        return outbounds

    def get_outbound_generalization_superclasses(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'GeneralizationIncidence' and x.get('Subkind') == 'Superclass')]
        return outbounds

    def get_outbound_generalization_subclasses(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'GeneralizationIncidence' and x.get('Subkind') == 'Subclass')]
        return outbounds

    def get_outbound_structs(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'StructIncidence')]
        return outbounds

    def get_outbound_association_by_name(self, rel_name):
        elements = self.get_outbound_associations().query('edges == "' + rel_name + '"')
        return elements

    def get_outbound_struct_by_name(self, struct_name):
        elements = self.get_outbound_structs().query('edges == "' + struct_name + '"')
        return elements

    def get_outbound_set_by_name(self, set_name):
        elements = self.get_outbound_sets().query('edges == "' + set_name + '"')
        return elements

    def get_outbound_class_by_name(self, class_name):
        elements = self.get_outbound_classes().query('edges == "' + class_name + '"')
        return elements

    def get_outbound_sets(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'SetIncidence')]
        return outbounds

    def get_outbound_classes(self):
        incidences = self.get_incidences()
        outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and x.get('Kind') == 'ClassIncidence')]
        return outbounds

    def get_transitives(self):
        incidences = self.get_incidences()
        transitives = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Transitive')]
        return transitives

    def get_transitives_by_edge_name(self, edge):
        transitives = self.get_transitives()[self.get_transitives().index.get_level_values('edges') == edge]
        return transitives

    def get_inbound_firstLevel(self):
        firstLevelPhantoms = df_difference(pd.concat([self.get_inbound_structs(), self.get_inbound_sets()], ignore_index=False).reset_index()[["nodes"]],
                                    self.get_outbounds().reset_index()[["nodes"]])
        firstLevelIncidences = self.get_inbounds().join(firstLevelPhantoms.set_index("nodes"), on="nodes", how='inner')
        return firstLevelIncidences

    def get_anchor_associations_by_struct_name(self, struct_name):
        elements = self.get_outbound_struct_by_name(struct_name)
        anchor_elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        anchor_associations = pd.merge(anchor_elements, inbounds, on="nodes", how="inner")["edges"].tolist()
        return anchor_associations

    def get_anchor_points_by_struct_name(self, struct_name):
        elements = self.get_outbound_struct_by_name(struct_name)
        elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        loose_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)["nodes"].tolist()
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner').index.tolist()
        anchor_points = loose_ends+classes
        return anchor_points

    def get_anchor_end_names_by_struct_name(self, struct_name):
        elements = self.get_outbound_struct_by_name(struct_name)
        elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        loose_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner').index.tolist()
        if not loose_ends.empty:
            end_names = loose_ends.apply(lambda x: str(x.get("misc_properties").get("End_name")), axis=1).tolist()
            return end_names+classes
        else:
            return classes

    def get_restricted_struct_hypergraph(self, struct_name):
        edge_names = []
        for elem in drop_duplicates(self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes").tolist() + self.get_anchor_points_by_struct_name(struct_name)):
            if self.is_class_phantom(elem) or self.is_association_phantom(elem):
                edge_names.append(self.get_edge_by_phantom_name(elem))
        return self.H.restrict_to_edges(edge_names)

    def get_attributes_by_struct_name(self, struct_name):
        attribute_names = []
        for elem in self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes"):
            if self.is_attribute(elem):
                attribute_names.append(elem)
        return attribute_names

    def get_superclasses_by_class_name(self, class_name, visited):
        all_links = self.get_outbound_generalization_superclasses().reset_index(level="nodes", drop=False).merge(
            self.get_outbound_generalization_subclasses().reset_index(level="nodes", drop=False), on="edges",
            suffixes=("_superclass", "_subclass"), how="inner")
        direct_superclass = all_links[all_links["nodes_subclass"] == self.get_phantom_of_edge_by_name(class_name)]
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            superclass = self.get_edge_by_phantom_name(direct_superclass.iloc[0]["nodes_superclass"])
            if superclass in visited:
                # This should not happen, because it means there is a cycle, but we need to stop recursion
                return [superclass]
            else:
                return self.get_superclasses_by_class_name(superclass, visited + [class_name])+[superclass]

    def get_generalizations_by_class_name(self, class_name, visited):
        all_links = self.get_outbound_generalization_superclasses().reset_index(level="nodes", drop=False).merge(
            self.get_outbound_generalization_subclasses().reset_index(level="nodes", drop=False), on="edges",
            suffixes=("_superclass", "_subclass"), how="inner")
        direct_superclass = all_links[all_links["nodes_subclass"] == self.get_phantom_of_edge_by_name(class_name)]
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            superclass = self.get_edge_by_phantom_name(direct_superclass.iloc[0]["nodes_superclass"])
            generalization = direct_superclass.index[0]
            if superclass in visited:
                # This should not happen, because it means there is a cycle, but we need to stop recursion
                return [generalization]
            else:
                return self.get_superclasses_by_class_name(superclass, visited + [class_name])+[generalization]

    def is_attribute(self, name):
        return name in self.get_attributes().index

    def is_id(self, name):
        return name in self.get_ids().index

    def is_class(self, name):
        return name in self.get_classes().index

    def is_class_phantom(self, name):
        return name in self.get_phantom_classes().index

    def is_association_phantom(self, name):
        return name in self.get_phantom_associations().index

    def is_generalization_phantom(self, name):
        return name in self.get_phantom_generalizations().index

    def is_hyperedge(self, name):
        return name in self.get_edges()["name"]

    def is_association(self, name):
        return name in self.get_associations().index

    def is_generalization(self, name):
        return name in self.get_generalizations().index

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
        logger.info("Adding class "+class_name)
        if self.is_hyperedge(class_name):
            raise ValueError(f"Some hyperedge called '{class_name}' already exists")
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
            raise ValueError(f"Some attribute in '{class_name}' is repeated")
        for att in att_list:
            if att['name'] in self.get_nodes()["name"]:
                raise ValueError(f"Some node called '{att['name']}' already exists")
            incidence_properties = {'Kind': 'ClassIncidence', 'Direction': 'Outbound'}
            incidence_properties['DistinctVals'] = att['prop'].pop('DistinctVals')
            incidence_properties['Identifier'] = att['prop'].pop('Identifier', False)
            incidences.append((class_name, att['name'], incidence_properties))
            if att['name'] in self.get_nodes()["name"]:
                if att['prop']['DataType'] != self.H.get_properties(att['name'], level=1, prop_name="DataType"):
                    raise ValueError(f"Some node called '{att['name']}' already exists, but its DataType does not coincide")
                if att['prop']['Size'] != self.H.get_properties(att['name'], level=1, prop_name="Size"):
                    raise ValueError(f"Some node called '{att['name']}' already exists, but its Size does not coincide")
            else:
                att['prop']['Kind'] = 'Attribute'
                nodes.append((att['name'], att['prop']))
        self.H.add_nodes_from(nodes)
        self.H.add_edges_from(edges)
        self.H.add_incidences_from(incidences)

    def add_association(self, association_name, ends_list):
        """Besides the association name, this method requires
        a list of ends (usually should be only two), where each end is a dictionary with the keys 'name' and 'multiplicity'.
        The latter is another dictionary that contains
        'DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logger.info("Adding association "+association_name)
        if self.is_hyperedge(association_name):
            raise ValueError(f"The hyperedge '{association_name}' already exists")
        if len(ends_list) != 2:
            raise ValueError(f"The association '{association_name}' should have exactly two ends, but has {len(ends_list)}")
        self.H.add_edge(association_name, Kind='Association')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+association_name, Kind='Phantom', Subkind='Association')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(association_name, self.config.prepend_phantom+association_name, {'Kind': 'AssociationIncidence', 'Direction': 'Inbound'})]
        for end in ends_list:
            if not self.is_class(end['class']):
                raise ValueError(f"The class '{end['class']}' in '{association_name}' does not exists")
            end['prop']['Kind'] = 'AssociationIncidence'
            end['prop']['Direction'] = 'Outbound'
            incidences.append((association_name, self.get_phantom_of_edge_by_name(end['class']), end['prop']))
        self.H.add_incidences_from(incidences)

    def add_generalization(self, generalization_name, properties, superclass, subclasses_list):
        """ Besides the generalization name, this method requires some properties (expected to be two booleans for
        disjointness and completeness, the name of the superclass and a list of subclasses,
        where each subclass is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that contains at least one constraint predicate that discriminates the subclass.
        """
        logger.info("Adding generalization "+generalization_name)
        if self.is_hyperedge(generalization_name):
            raise ValueError(f"The hyperedge '{generalization_name}' already exists")
        self.H.add_edge(generalization_name, Kind='Generalization', Disjoint=properties.get('Disjoint', False), Complete=properties.get('Complete', False))
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+generalization_name, Kind='Phantom', Subkind='Generalization')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(generalization_name, self.config.prepend_phantom+generalization_name, {'Kind': 'GeneralizationIncidence', 'Direction': 'Inbound'})]
        if not self.is_class(superclass):
            raise ValueError(f"The superclass '{superclass}' in '{generalization_name}' does not exists")
        # First element in the pair of incidences is the edge name and the second the node
        incidences.append((generalization_name,  self.get_phantom_of_edge_by_name(superclass), {'Kind': 'GeneralizationIncidence', 'Subkind': 'Superclass', 'Direction': 'Outbound'}))
        if len(subclasses_list) < 1:
            raise ValueError(f"The generalization '{generalization_name}' should have at least one subclass")
        for sub in subclasses_list:
            if superclass == sub['class']:
                raise ValueError(f"The same class '{superclass}' cannot play super and sub roles in generalization '{generalization_name}'")
            if not self.is_class(sub['class']):
                raise ValueError(f"The subclass '{superclass}' in '{generalization_name}' does not exists")
            # TODO: Discriminant should be validated here
            sub['prop']['Kind'] = 'GeneralizationIncidence'
            sub['prop']['Subkind'] = 'Subclass'
            sub['prop']['Direction'] = 'Outbound'
            incidences.append((generalization_name, self.get_phantom_of_edge_by_name(sub['class']), sub['prop']))
        self.H.add_incidences_from(incidences)

    def add_struct(self, struct_name, anchor, elements):
        logger.info("Adding struct "+struct_name)
        if self.is_hyperedge(struct_name):
            raise ValueError(f"The hyperedge '{struct_name}' already exists")
        for element in anchor:
            if not self.is_class(element) and not self.is_association(element):
                raise ValueError(f"The anchor of '{struct_name}' (i.e., '{element}') must be either a class or a association")
        # TODO: Check if the associations in the anchor are connected (considering inheritance of associations)
        # TODO: Check if the struct is connected
        self.H.add_edge(struct_name, Kind='Struct')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+struct_name, Kind='Phantom', Subkind="Struct")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(struct_name, self.config.prepend_phantom+struct_name, {'Kind': 'StructIncidence', 'Direction': 'Inbound'})]
        for elem in drop_duplicates(anchor+elements):
            if self.is_attribute(elem):
                incidences.append((struct_name, elem, {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_association(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_class(elem):
                superclasses = self.get_superclasses_by_class_name(elem, [])
                incidences.append((struct_name, self.get_class_id_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': False}))
                for c in superclasses+[elem]:
                    # Only one element of a hierarchy can be included by the user in a struct
                    if c != elem and c in anchor+elements:
                        raise ValueError(f"Only one class per hierarchy can be included in a struct ('{struct_name}' got '{elem} and '{c}')")
                    else:
                        incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
                for g in self.get_generalizations_by_class_name(elem, []):
                    incidences.append((struct_name, self.get_phantom_of_edge_by_name(g), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_struct(elem) or self.is_set(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
                for outbound_elem in self.get_outbounds().loc[elem].index:
                    if outbound_elem not in [self.get_phantom_of_edge_by_name(anchor)] + elements:
                        incidences.append((struct_name, outbound_elem, {'Kind': 'StructIncidence', 'Direction': 'Transitive'}))
                try:
                    for transitive_elem in self.get_transitives().loc[elem].index:
                        if transitive_elem not in [self.get_phantom_of_edge_by_name(anchor)] + elements:
                            incidences.append((struct_name, transitive_elem, {'Kind': 'StructIncidence', 'Direction': 'Transitive'}))
                except KeyError:
                    pass
            elif self.is_generalization(elem):
                pass
            else:
                raise ValueError(f"Creating struct '{struct_name}' could not find '{elem}' to place it inside")
        self.H.add_incidences_from(incidences)

    def add_set(self, set_name, elements):
        logger.info("Adding set "+set_name)
        if set_name in self.get_edges()["name"]:
            raise ValueError(f"The hyperedge '{set_name}' already exists")
        if len(elements) == 0:
            raise ValueError(f"The set '{set_name}' should have some elements, but has {len(elements)}")
        self.H.add_edge(set_name, Kind='Set')
        # This adds a special phantom node required to represent different cases of inclusion in sets
        self.H.add_node('Phantom_'+set_name, Kind='Phantom', Subkind="Set")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(set_name, self.config.prepend_phantom+set_name, {'Kind': 'SetIncidence', 'Direction': 'Inbound'})]
        for elem in elements:
            if self.is_attribute(elem):
                incidences.append((set_name, elem, {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_association(elem) or self.is_struct(elem):
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
                raise ValueError(f"Creating set '{set_name}' could not find the kind of '{elem}' to place it inside")
        self.H.add_incidences_from(incidences)

    def show_graphical(self):
        # Graphical display
        show_graphical_hypergraph(self.H, self.config.phantom)

    def show_textual(self):
        # Textual display
        show_textual_hypergraph(self.H)

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
        associations = self.get_associations()
        generalizations = self.get_generalizations()
        structs = self.get_structs()
        sets = self.get_sets()
        inbounds = self.get_inbounds()
        structInbounds = self.get_inbound_structs()
        outbounds = self.get_outbounds()
        structOutbounds = self.get_outbound_structs()
        transitives = self.get_transitives()

        # -------------------------------------------------------------------------------------------------- Generic ICs
        # Pre-check emptiness
        logger.info("Checking emptiness")
        if self.get_nodes().shape[0] == 0 or self.get_edges().shape[0] == 0 or self.get_incidences().shape[0] == 0:
            print(f"This is a degenerated hypergraph: {self.get_nodes().shape[0]} nodes, {self.get_edges().shape[0]} edges, and {self.get_incidences().shape[0]} incidences")
            return False

        # IC-Generic1: Names must be unique
        logger.info("Checking IC-Generic1")
        union1_1 = pd.concat([self.get_nodes()["name"], self.get_edges()["name"]], ignore_index=True)
        violations1_1 = union1_1.groupby(union1_1).size()
        if violations1_1[violations1_1 > 1].shape[0] > 0:
            correct = False
            print("IC-Generic1 violation: Some names are not unique")
            display(violations1_1[violations1_1 > 1])

        # IC-Generic2: The catalog must be connected
        logger.info("Checking IC-Generic2")
        if not self.H.is_connected(s=1):
            correct = False
            print("IC-Generic2 violation: The catalog is not connected")

        # IC-Generic3: Every phantom belongs to one edge
        logger.info("Checking IC-Generic3")
        matches1_3 = inbounds.join(edges, on='edges', rsuffix='_edges', how='inner')
        violations1_3 = phantoms[~phantoms["name"].isin((matches1_3.reset_index(drop=False))["nodes"])]
        if violations1_3.shape[0] > 0:
            correct = False
            print("IC-Generic3 violation: There are phantoms without an edge")
            display(violations1_3)

        # IC-Generic4: Every edge has at least one inbound
        logger.info("Checking IC-Generic4")
        matches1_4 = self.get_inbounds().reset_index(level='nodes', drop=True).reset_index(drop=False)['edges']
        violations1_4 = df_difference(edges.reset_index(drop=False)['edges'], matches1_4)
        if violations1_4.shape[0] > 0:
            correct = False
            print("IC-Generic4 violation: There are edges without inbound")
            display(violations1_4)

        # IC-Generic5: Every edge has at least one outbound
        logger.info("Checking IC-Generic5")
        matches1_5 = self.get_outbounds().reset_index(level='nodes', drop=True).reset_index(drop=False)['edges']
        violations1_5 = df_difference(edges.reset_index(drop=False)['edges'], matches1_5)
        if violations1_5.shape[0] > 0:
            correct = False
            print("IC-Generic4 violation: There are edges without outbound")
            display(violations1_5)

        # IC-Generic6: An edge cannot have more than one inbound
        logger.info("Checking IC-Generic6")
        violations1_6 = inbounds.groupby(inbounds.index.get_level_values('edges')).size()
        if violations1_6[violations1_6 > 1].shape[0] > 0:
            correct = False
            print("IC-Generic6 violation: There are edges with more than one inbound")
            display(violations1_6[violations1_6 > 1])

        # IC-Generic7: An edge cannot be cyclic
        logger.info("Checking IC-Generic7")
        violations1_7 = pd.merge(inbounds, pd.concat([outbounds, transitives]), on=["nodes", "edges"], how="inner")
        if violations1_7.shape[0] > 0:
            correct = False
            print("IC-Generic7 violation: There are cyclic edges")
            display(violations1_7)

        # IC-Generic8: Outbounds and transitive of an edge must be disjoint
        logger.info("Checking IC-Generic8")
        violations1_8 = pd.merge(outbounds, transitives, on=["nodes", "edges"], how="inner")
        if violations1_8.shape[0] > 0:
            correct = False
            print("IC-Generic8 violation: There are edges with common outbound and transitive incidences")
            display(violations1_8)

        # ------------------------------------------------------------------------------------------------- ICs on atoms
        # IC-Atoms2: Every ID belongs to one class which is outbound
        logger.info("Checking IC-Atoms2")
        matches2_2 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_2 = ids[~ids["name"].isin((matches2_2.reset_index(drop=False))["nodes"])]
        if violations2_2.shape[0] > 0:
            correct = False
            print("IC-Atoms2 violation: There are IDs without a class")
            display(violations2_2)

        # IC-Atoms3: Every attribute must belong at least one class which is outbound
        logger.info("Checking IC-Atoms3")
        matches2_3 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_3 = attributes[~attributes["name"].isin((matches2_3.reset_index(drop=False))["nodes"])]
        if violations2_3.shape[0] > 0:
            correct = False
            print("IC-Atoms3 violation: There are attributes without a class")
            display(violations2_3)

        # IC-Atoms4: An attribute cannot belong to more than one class
        logger.info("Checking IC-Atoms4")
        matches2_4 = incidences.join(classes, on='edges', rsuffix='_edges', how='inner').join(attributes, on='nodes', rsuffix='_nodes', how='inner')
        violations2_4 = matches2_4.groupby(matches2_4.index.get_level_values('nodes')).size()
        if violations2_4[violations2_4 > 1].shape[0] > 0:
            correct = False
            print("IC-Atoms4 violation: There are attributes with more than one class")
            display(violations2_4[violations2_4 > 1])

        # IC-Atoms5: The number of different values of an attribute must be less or equal than the cardinality of its class
        logger.info("Checking IC-Atoms5")
        matches2_5 = outbounds.join(classes, on='edges', rsuffix='_class', how='inner')
        violations2_5 = matches2_5[matches2_5.apply(lambda row: row["misc_properties"]["DistinctVals"] > row["misc_properties_class"]["Count"], axis=1)]
        if violations2_5.shape[0] > 0:
            correct = False
            print("IC-Atoms5 violation: The number of different values of an attribute is greater than the cardinality of its class")
            display(violations2_5)

        # IC-Atoms6: Every association has one phantom
        logger.info("Checking IC-Atoms6")
        matches2_6 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
        violations2_6 = associations[~associations["name"].isin((matches2_6.reset_index(drop=False))["edges"])]
        if violations2_6.shape[0] > 0:
            correct = False
            print("IC-Atoms6 violation: There are associations without phantom")
            display(violations2_6)

        # IC-Atoms7: Every association has two ends (Definition 4)
        logger.info("Checking IC-Atoms7")
        matches2_7 = incidences.join(ids, on='nodes', rsuffix='_nodes', how='inner').join(associations, on='edges', rsuffix='_edges', how='inner').groupby(['edges']).size()
        violations2_7 = matches2_7[matches2_7 != 2]
        if violations2_7.shape[0] > 0:
            correct = False
            print("IC-Atoms7 violation: There are non-binary associations")
            display(violations2_7)

        # IC-Atoms8: The number of different values of an identifier must coincide with the cardinality of its class
        logger.info("Checking IC-Atoms8")
        matches2_8 = outbounds.join(classes, on='edges', rsuffix='_class', how='inner')
        violations2_8 = matches2_8[matches2_8.apply(lambda row: row["misc_properties"]["Identifier"] and row["misc_properties"]["DistinctVals"] != row["misc_properties_class"]["Count"], axis=1)]
        if violations2_8.shape[0] > 0:
            correct = False
            print("IC-Atoms5 violation: The number of different values of an identified must coincide with the cardinality of its class")
            display(violations2_8)

        # IC-Atoms9: One class cannot have more than one direct superclass
        logger.info("Checking IC-Atoms9")
        matches2_9 = self.get_outbound_generalization_subclasses().groupby(["nodes"]).size()
        violations2_9 = matches2_9[matches2_9 > 1]
        if violations2_9.shape[0] > 0:
            correct = False
            print("IC-Atoms9 violation: There are classes with more than one superclass")
            display(violations2_9)

        # IC-Atoms10: Every generalization outgoing of a subclass must have a discriminant
        logger.info("Checking IC-Atoms10")
        violations2_10 = self.get_outbound_generalization_subclasses()[~self.get_outbound_generalization_subclasses().apply(lambda row: "Constraint" in row["misc_properties"], axis=1)]
        if violations2_10.shape[0] > 0:
            correct = False
            print("IC-Atoms10 violation: There are generalization subclasses without discriminant constraint")
            display(violations2_10)

        # IC-Atoms11: Every generalization has disjointness and completeness constraints
        logger.info("Checking IC-Atoms11")
        matches2_11 = generalizations[generalizations.apply(lambda row: "Disjoint" in row["misc_properties"] and "Complete" in row["misc_properties"], axis=1)]
        violations2_11 = df_difference(generalizations["name"], matches2_11["name"])
        if violations2_11.shape[0] > 0:
            correct = False
            print("IC-Atoms11 violation: There are generalizations without completeness and disjointness constraints")
            display(violations2_11)

        # IC-Atoms12: Generalizations cannot have cycles
        logger.info("Checking IC-Atoms12")
        violations2_12 = classes[classes.apply(lambda row: row["name"] in self.get_superclasses_by_class_name(row["name"], []), axis=1)]
        if violations2_12.shape[0] > 0:
            correct = False
            print("IC-Atoms12 violation: There are some cyclic generalizations")
            display(violations2_12)

        # IC-Atoms13: Every class has one ID or belongs to a generalization hierarchy
        logger.info("Checking IC-Atoms13")
        matches2_13 = outbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        possible_violations2_13 = classes[~classes["name"].isin((matches2_13.reset_index(drop=False))["edges"])]
        for row in possible_violations2_13.itertuples():
            superclasses = self.get_superclasses_by_class_name(row.Index, [])
            if not superclasses:
                correct = False
                print(f"IC-Atoms13 violation: There is some class '{row.Index}' without identifier (neither direct, nor inherited from a superclass)")

        # IC-Atoms14: Not two classes in a hierarchy can have ID
        logger.info("Checking IC-Atoms14")
        matches2_14 = outbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        possible_violations2_14 = classes[classes["name"].isin((matches2_14.reset_index(drop=False))["edges"])]
        for row in possible_violations2_14.itertuples():
            superclasses = self.get_superclasses_by_class_name(row.Index, [])
            identified_superclasses = [s for s in superclasses if s in possible_violations2_14.index]
            if identified_superclasses:
                correct = False
                print(f"IC-Atoms14 violation: There is some class '{row.Index}' with identifier in a generalization hierarchy with also identifiers '{identified_superclasses}'")

        # IC-Atoms15: The top of every hierarchy has an ID
        logger.info("Checking IC-Atoms15")
        matches2_15 = df_difference(self.get_outbound_generalization_superclasses().reset_index(drop=False)['nodes'], self.get_outbound_generalization_subclasses().reset_index(drop=False)['nodes'])
        for top_phantom in matches2_15:
            top_class = self.get_edge_by_phantom_name(top_phantom)
            if self.get_class_id_by_name(top_class) is None:
                correct = False
                print(f"IC-Atoms15 violation: The class '{top_class}' in the top of a hierarchy should have an identifier")

        # Not necessary to check from here on if the catalog only contains the atoms in the domain
        if design:
            # ------------------------------------------------------------------------------------------- ICs on structs
            # IC-Structs1: Every struct has one phantom
            logger.info("Checking IC-Structs1")
            matches3_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations3_1 = structs[~structs["name"].isin((matches3_1.reset_index(drop=False))["edges"])]
            if violations3_1.shape[0] > 0:
                correct = False
                print("IC-Structs1 violation: There are structs without phantom")
                display(violations3_1)

            # IC-Structs2: Structs are transitive on themselves
            logger.info("Checking IC-Structs2")
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

            # IC-Structs3: Every struct has at least one anchor
            logger.info("Checking IC-Structs3")
            matches3_3 = outbounds[outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Anchor', False))].groupby('edges').size()
            violations3_3 = structs[~structs["name"].isin((matches3_3[matches3_3 > 0].reset_index(drop=False))["edges"])]
            if violations3_3.shape[0] > 0:
                correct = False
                print("IC-Structs3 violation: There are structs without exactly one anchor")
                display(violations3_3)

            # IC-Structs4: Anchors can be either classes or associations
            logger.info("Checking IC-Structs3")
            matches3_4 = outbounds[outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Anchor', False))].reset_index(drop=False)['nodes']
            violations3_4 = df_difference(matches3_4, pd.concat([self.get_phantom_classes(), self.get_phantom_associations()])["name"])
            if violations3_4.shape[0] > 0:
                correct = False
                print("IC-Structs4 violation: There are structs with an anchor which is neither class nor association")
                display(violations3_4)

            # IC-Structs5: Anchors are connected
            logger.info("Checking IC-Structs5")
            for struct in self.get_structs().index:
                edge_names = []
                struct_outbounds = self.get_outbound_struct_by_name(struct)
                for elem in struct_outbounds[struct_outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Anchor', False))].reset_index(level='edges', drop=True).index:
                    if self.is_class_phantom(elem) or self.is_association_phantom(elem):
                        edge_names.append(self.get_edge_by_phantom_name(elem))
                restricted_struct = self.H.restrict_to_edges(edge_names)
                # Check if the restricted struct is connected
                if not restricted_struct.is_connected(s=1):
                    correct = False
                    print(f"IC-Structs-5 violation: The anchor of struct '{struct}' is not connected")

            # IC-Structs-b: All attributes in a struct are connected to its anchor by a unique path of associations, which are all part of the struct, too (Definition 7-b)
            logger.info("Checking IC-Structs-b")
            for struct_name in self.get_structs().index:
                attribute_names = self.get_attributes_by_struct_name(struct_name)
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                # Check if the restricted struct is connected
                if not restricted_struct.is_connected(s=1):
                    correct = False
                    print(f"IC-Structs-b violation: The struct '{struct_name}' is not connected")
                anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                bipartite = restricted_struct.remove_edges(self.get_anchor_associations_by_struct_name(struct_name)).bipartite()
                for attr in attribute_names:
                    paths = []
                    for anchor in anchor_points:
                        paths += list(nx.all_simple_paths(bipartite, source=anchor, target=attr))
                    if len(paths) > 1:
                        correct = False
                        print(f"IC-Structs-b violation: The struct '{struct_name}' has multiple paths '{paths}', which generates ambiguity in the meaning of some attribute")

            # IC-Structs-c: All anchors of structs inside a struct are connected to its anchor by a unique path of associations, which are all part of the struct, too (Definition 7-c)
            logger.info("Checking IC-Structs-c -> To be implemented (for nested structs)")

            # IC-Structs-d: All sets inside a struct must contain a unique path of associations connecting the parent struct to either the attribute or anchor of the struct inside the set (Definition 7-d)
            logger.info("Checking IC-Structs-d -> To be implemented (for nested sets)")

            # IC-Structs-e: All associations inside a struct connect either a class or another struct (Definition 7-e)
            #               This needs to be relaxed to simply structs being connected
            logger.info("Checking IC-Structs-e (relaxed)")
            for struct_name in self.get_structs().index:
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                # Check if the restricted struct is connected
                if not restricted_struct.is_connected(s=1):
                    correct = False
                    print(f"IC-Structs-e violation: The struct '{struct_name}' is not connected")

            # ---------------------------------------------------------------------------------------------- ICs on sets
            # IC-Sets1: Every set has one phantom
            logger.info("Checking IC-Sets1")
            matches4_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations4_1 = sets[~sets["name"].isin((matches4_1.reset_index(drop=False))["edges"])]
            if violations4_1.shape[0] > 0:
                correct = False
                print("IC-Sets1 violation: There are sets without phantom")
                display(violations4_1)

            # IC-Sets2: Sets are transitive on structs
            logger.info("Checking IC-Sets2 -> To be implemented")

            # IC-Sets3: Sets cannot directly contain classes
            logger.info("Checking IC-Sets3")
            violations4_3 = pd.merge(self.get_outbound_sets(), self.get_inbound_classes(), on='nodes', suffixes=('_setOutbounds', '_classInbounds'), how='inner')
            if violations4_3.shape[0] > 0:
                correct = False
                print("IC-Sets3 violation: There are sets that contain classes")
                display(violations4_3)

            # IC-Sets4: Sets cannot directly contain other sets
            logger.info("Checking IC-Sets4")
            violations4_4 = pd.merge(self.get_outbound_sets(), self.get_inbound_sets(), on='nodes', suffixes=('_setOutbounds', '_setInbounds'), how='inner')
            if violations4_4.shape[0] > 0:
                correct = False
                print("IC-Sets4 violation: There are sets that contain other sets")
                display(violations4_4)

            # ----------------------------------------------------------------------------------------- ICs about design
            # IC-Design1: All the first levels must be sets
            logger.info("Checking IC-Design1")
            matches5_1 = self.get_inbound_firstLevel()
            violations5_1 = matches5_1[~matches5_1["misc_properties"].apply(lambda x: x['Kind'] == 'SetIncidence')]
            if violations5_1.shape[0] > 0:
                correct = False
                print("IC-Design1 violation: All first levels must be sets")
                display(violations5_1)

            # IC-Design2: All the atoms in the domain are connected to the first level
            logger.info("Checking IC-Design2")
            matches5_2 = self.get_inbound_firstLevel().join(pd.concat([self.get_outbounds(), self.get_transitives()]).reset_index(level="nodes"), on="edges", rsuffix='_tokeep', how='inner')
            atoms5_2 = pd.concat([self.get_attributes(), self.get_phantom_associations()])
            violations5_2 = atoms5_2[~atoms5_2["name"].isin(matches5_2["nodes"])]
            if violations5_2.shape[0] > 0:
                correct = False
                print("IC-Design2 violation: Atoms disconnected from the first level")
                display(violations5_2)

            # # IC-Design3: All domain elements must appear in some struct
            logger.info("Checking IC-Design3")
            atoms = pd.concat([self.get_inbound_classes().reset_index(drop=False)["nodes"], self.get_inbound_associations().reset_index(drop=False)["nodes"], attributes.reset_index(drop=False)["nodes"]])
            violations5_3 = atoms[~atoms.isin(structOutbounds.index.get_level_values("nodes"))]
            if violations5_3.shape[0] > 0:
                correct = False
                print("IC-Design3 violation: Some atoms do not belong to any struct")
                display(violations5_3)

        return correct


    def check_query_structure(self, project_attributes, filter_attributes, join_edges, required_attributes):
        # Check if the hypergraph contains all the projected attributes
        non_existing_attributes = df_difference(pd.DataFrame(project_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the projection does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the filter attributes
        non_existing_attributes = df_difference(pd.DataFrame(filter_attributes), pd.concat([self.get_ids(), self.get_attributes()])["name"].reset_index(drop=True))
        if non_existing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the filter does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the join hyperedges
        non_existing_associations = df_difference(pd.DataFrame(join_edges), pd.concat([self.get_classes(), self.get_associations()])["name"].reset_index(drop=True))
        if non_existing_associations.shape[0] > 0:
            raise ValueError(f"Some class or association in the join does not belong to the catalog: {non_existing_associations.values.tolist()[0]}")

        restricted_domain = self.H.restrict_to_edges(join_edges)
        # Check if the restricted domain is connected
        if not restricted_domain.is_connected(s=1):
            raise ValueError(f"Some query elements (i.e., attributes, classes and associations) are not connected")

        # Check if the restricted domain contains all the required attributes
        hop1 = pd.merge(restricted_domain.nodes.dataframe, self.get_inbound_classes().reset_index(drop=False), left_on="uid", right_on="nodes", suffixes=('_classPhantoms', '_inbounds'), how="inner")
        hop2 = pd.merge(hop1, self.get_outbound_classes().reset_index(drop=False), left_on="edges", right_on="edges", suffixes=('', '_outbounds'), how="inner")
        hop3 = pd.merge(hop2, self.get_attributes().reset_index(drop=False), left_on="nodes_outbounds", right_on="nodes", suffixes=('_carriedOutbounds', '_attributes'), how="inner")
        implicit_ids = hop3[hop3["misc_properties_attributes"].apply(lambda x: x.get('Identifier', False))]["nodes_attributes"]
        explicit = restricted_domain.nodes.dataframe.reset_index(drop=False)["uid"]
        missing_attributes = df_difference(pd.DataFrame(required_attributes), pd.concat([explicit, implicit_ids], ignore_index=True))
        if missing_attributes.shape[0] > 0:
            raise ValueError(f"Some attribute in the query is not covered by the joined elements: {missing_attributes.values.tolist()[0]}")

    def parse_query(self, query):
        # Get the query and parse it
        project_attributes = query.get("project")
        for a in project_attributes:
            if not self.is_attribute(a):
                raise ValueError(f"Projected '{a}' is not an attribute")
        join_edges = query.get("join")
        for e in join_edges:
            if not (self.is_class(e) or self.is_association(e)):
                raise ValueError(f"Chosen edge '{e}' is neither a class nor a association")
        filter_clause = query.get("filter", "TRUE")
        filter_attributes = []
        if "filter" in query:
            where_clause = "WHERE "+filter_clause
            where_parsed = sqlparse.parse(where_clause)[0].tokens[0]

            # This extracts the attribute names
            # TODO: Parenthesis are not considered by now. It will require some kind of recursion
            for atom in where_parsed.tokens:
                if atom.ttype is None:  # This is a clause in the predicate
                    for token in atom.tokens:
                        if token.ttype is None:  # This is an attribute in the predicate
                            if not self.is_attribute(token.value):
                                raise ValueError(f"Filtering '{token.value}' is not an attribute")
                            filter_attributes.append(token.value)
        required_attributes = list(set(project_attributes + filter_attributes))

        self.check_query_structure(project_attributes, filter_attributes, join_edges, required_attributes)
        return project_attributes, filter_attributes, join_edges, required_attributes, filter_clause