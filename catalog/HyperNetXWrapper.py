from typing import Self
import logging
import os
import hypernetx as hnx
import pickle
from IPython.display import display
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

from .config import Config
from .tools import drop_duplicates, df_difference

# Libraries initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

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
            self.H = hnx.Hypergraph([])

    def save(self, file_path=None) -> None:
        if file_path is not None:
            logger.info(f"Saving hypergraph in '{file_path}'")
            # Create the directory (if it doesn't exist)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Save the hypergraph to a pickle file
            with open(file_path, "wb") as f:
                pickle.dump(self.H, f)

    def get_nodes(self) -> pd.DataFrame:
        nodes = self.H.nodes.dataframe.rename_axis("nodes")
        nodes["name"] = nodes.index
        return nodes

    def get_edges(self) -> pd.DataFrame:
        edges = self.H.edges.dataframe.rename_axis("edges")
        edges["name"] = edges.index
        return edges

    def get_struct_names_inside_set_name(self, set_name) -> list[str]:
        return pd.merge(self.get_outbound_set_by_name(set_name), self.get_inbound_structs().reset_index("edges", drop=False), on="nodes", how="inner")["edges"].tolist()

    def get_incidences(self) -> pd.DataFrame:
        incidences = self.H.incidences.dataframe
        return incidences

    def get_attributes(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        attributes = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Attribute')]
        return attributes

    def get_attribute_by_name(self, attr_name) -> pd.Series:
        attribute = self.get_attributes().query('nodes == "' + attr_name + '"')
        return attribute.iloc[0]

    def get_association_ends(self) -> pd.DataFrame:
        ends = self.get_outbound_associations()
        if not ends.empty:
            ends.reset_index(drop=False, inplace=True)
            ends["name"] = ends.apply(lambda x: x["misc_properties"]["End_name"], axis=1)
            ends.set_index('name', drop=False, inplace=True)
            ends.drop(columns=['weight'], inplace=True)
        return ends

    def get_association_ends_by_name(self, association_name) -> pd.DataFrame:
        ends = self.get_association_ends().query('edges == "' + association_name + '"')
        return ends

    def get_class_name_by_end_name(self, end_name) -> str:
        association_end = self.get_association_ends()[self.get_association_ends()["misc_properties"].apply(lambda x: x["End_name"] == end_name)]
        return self.get_edge_by_phantom_name(association_end.iloc[0].nodes)

    def get_ids(self) -> pd.DataFrame:
        outbounds = self.get_outbound_classes()
        incidences = outbounds[outbounds["misc_properties"].apply(lambda x: x['Identifier'])].reset_index(level='edges', drop=True)
        ids = self.get_attributes()[self.get_attributes()["name"].isin(incidences.index)]
        return ids

    def get_class_id_by_name(self, class_name) -> str | None:
        superclasses = self.get_superclasses_by_class_name(class_name)
        if not superclasses:
            class_outbounds = self.get_outbound_class_by_name(class_name)
        else:
            # The top of the hierarchy should be the first in the list
            class_outbounds = self.get_outbound_class_by_name(superclasses[0])
        class_id = class_outbounds[class_outbounds["misc_properties"].apply(lambda x: x['Identifier'])]
        if class_id.empty:
            # This should not happen, it will crash, because it is never checked later
            return None
        else:
            return class_id.index[0][1]

    def get_phantoms(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom')]
        return phantoms

    def get_phantom_classes(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and
                                                                  x['Subkind'] == 'Class')]
        return phantoms

    def get_phantom_associations(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and
                                                                  x['Subkind'] == 'Association')]
        return phantoms

    def get_phantom_generalizations(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and
                                                                  x['Subkind'] == 'Generalization')]
        return phantoms

    def get_edge_by_phantom_name(self, phantom_name) -> str:
        return self.get_inbounds()[self.get_inbounds().index.get_level_values('nodes') == phantom_name].index[0][0]

    def get_phantom_of_edge_by_name(self, edge_name) -> str:
        return self.get_inbounds().loc[edge_name].index[0]

    def get_classes(self) -> pd.DataFrame:
        edges = self.get_edges()
        classes = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Class')]
        return classes

    def get_associations(self) -> pd.DataFrame:
        edges = self.get_edges()
        associations = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Association')]
        return associations

    def get_generalizations(self) -> pd.DataFrame:
        edges = self.get_edges()
        associations = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Generalization')]
        return associations

    def get_structs(self) -> pd.DataFrame:
        edges = self.get_edges()
        structs = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Struct')]
        return structs

    def get_sets(self) -> pd.DataFrame:
        edges = self.get_edges()
        sets = edges[edges["misc_properties"].apply(lambda x: x['Kind'] == 'Set')]
        return sets

    def get_inbounds(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return inbounds

    def get_inbound_classes(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and
                                                                            x['Kind'] == 'ClassIncidence')]
        return inbounds

    def get_inbound_associations(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and
                                                                            x['Kind'] == 'AssociationIncidence')]
        return inbounds

    def get_inbound_generalizations(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and
                                                                            x['Kind'] == 'GeneralizationIncidence')]
        return inbounds

    def get_inbound_structs(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and
                                                                            x['Kind'] == 'StructIncidence')]
        return inbounds

    def get_inbound_sets(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        inbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound' and
                                                                            x['Kind'] == 'SetIncidence')]
        return inbounds

    def get_outbounds(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound')]
            return outbounds

    def get_outbound_associations(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'AssociationIncidence')]
            return outbounds

    def get_outbound_generalization_superclasses(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'GeneralizationIncidence' and
                                                                                 x['Subkind'] == 'Superclass')]
            return outbounds

    def get_outbound_generalization_subclasses(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'GeneralizationIncidence' and
                                                                                 x['Subkind'] == 'Subclass')]
            return outbounds

    def get_outbound_structs(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'StructIncidence')]
            return outbounds

    def get_outbound_association_by_name(self, ass_name) -> pd.DataFrame:
        elements = self.get_outbound_associations().query('edges == "' + ass_name + '"')
        return elements

    def get_outbound_struct_by_name(self, struct_name) -> pd.DataFrame:
        elements = self.get_outbound_structs().query('edges == "' + struct_name + '"')
        return elements

    def get_outbound_set_by_name(self, set_name) -> pd.DataFrame:
        elements = self.get_outbound_sets().query('edges == "' + set_name + '"')
        return elements

    def get_outbound_class_by_name(self, class_name) -> pd.DataFrame:
        elements = self.get_outbound_classes().query('edges == "' + class_name + '"')
        return elements

    def get_outbound_sets(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'SetIncidence')]
            return outbounds

    def get_outbound_classes(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            outbounds = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                 x['Kind'] == 'ClassIncidence')]
            return outbounds

    def get_transitives(self) -> pd.DataFrame:
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            transitives = incidences[incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Transitive')]
            return transitives

    def get_transitives_by_edge_name(self, edge) -> pd.DataFrame:
        transitives = self.get_transitives()[self.get_transitives().index.get_level_values('edges') == edge]
        return transitives

    def get_inbound_firstLevel(self) -> pd.DataFrame:
        firstLevel_phantoms = df_difference(pd.concat([self.get_inbound_structs(), self.get_inbound_sets()], ignore_index=False).reset_index()[["nodes"]],
                                           self.get_outbounds().reset_index()[["nodes"]])
        firstLevel_incidences = self.get_inbounds().join(firstLevel_phantoms.set_index("nodes"), on="nodes", how='inner')
        return firstLevel_incidences

    def get_anchor_associations_by_struct_name(self, struct_name) -> list[str]:
        elements = self.get_outbound_struct_by_name(struct_name)
        anchor_elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        anchor_associations = pd.merge(anchor_elements, inbounds, on="nodes", how="inner")["edges"].tolist()
        return anchor_associations

    def get_anchor_points_by_struct_name(self, struct_name) -> list[str]:
        # This is not considering that an anchor of a struct can be in a nested struct (only at first level)
        elements = self.get_outbound_struct_by_name(struct_name)
        elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        loose_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)["nodes"].tolist()
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner').index.tolist()
        anchor_points = drop_duplicates(loose_ends+classes)
        return anchor_points

    def get_anchor_end_names_by_struct_name(self, struct_name) -> list[str]:
        elements = self.get_outbound_struct_by_name(struct_name)
        elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        association_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner')
        loose_ends = association_ends[~association_ends["nodes"].isin(classes.index)]
        if loose_ends.empty:
            return classes.index.tolist()
        else:
            end_names = loose_ends.apply(lambda x: str(x.get("misc_properties").get("End_name")), axis=1).tolist()
            return classes.index.tolist()+end_names

    def get_loose_association_end_names_by_struct_name(self, struct_name) -> list[str]:
        elements = self.get_outbound_struct_by_name(struct_name)
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        association_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner')
        loose_ends = association_ends[~association_ends["nodes"].isin(classes.index)]
        if loose_ends.empty:
            return []
        else:
            end_names = loose_ends.apply(lambda x: str(x.get("misc_properties").get("End_name")), axis=1).tolist()
            return end_names

    def get_restricted_struct_hypergraph(self, struct_name, only_anchor=False) -> Self:
        anchor_points = self.get_anchor_points_by_struct_name(struct_name)
        if only_anchor:
            outbounds = [self.get_phantom_of_edge_by_name(ass) for ass in self.get_anchor_associations_by_struct_name(struct_name)]
        else:
            outbounds = self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes").tolist()
        edge_names = []
        for elem in drop_duplicates(outbounds + anchor_points):
            if self.is_class_phantom(elem) or self.is_association_phantom(elem) or self.is_generalization_phantom(elem):
                edge_names.append(self.get_edge_by_phantom_name(elem))
                if self.is_class_phantom(elem) and elem in outbounds:
                    edge_names.extend(self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(elem)))
                    edge_names.extend(self.get_generalizations_by_class_name(self.get_edge_by_phantom_name(elem)))
        # It takes all attributes in the classes, but we only want those in the outbounds, so we remove them one by one
        result = HyperNetXWrapper(hypergraph=self.H.restrict_to_edges(edge_names))
        to_be_removed = []
        for attr_name in result.get_attributes().index:
            if attr_name not in outbounds:
                to_be_removed.append(attr_name)
        result.H.remove_nodes(to_be_removed, inplace=True)
        return result

    def get_attribute_names_by_struct_name(self, struct_name) -> list[str]:
        return pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_attributes(), on="nodes", how="inner").index.tolist()

    def get_subclasses_by_class_name(self, class_name, visited: list[str] = None) -> list[str]:
        """
        Gives the names of the subclasses of a given class (the class itself is not included in the list)
        :param class_name:
        :param visited: This is necessary for recursion purposes. Initially, it should be just an empty list
        :return: List of subclasses (no sorting can be assumed)
        """
        if visited is None:
            visited = []
        all_links = self.get_outbound_generalization_superclasses().reset_index(level="nodes", drop=False).merge(
            self.get_outbound_generalization_subclasses().reset_index(level="nodes", drop=False), on="edges",
            suffixes=("_superclass", "_subclass"), how="inner")
        direct_subclasses = all_links[all_links["nodes_superclass"] == self.get_phantom_of_edge_by_name(class_name)]
        if direct_subclasses.empty:
            return []
        else:
            subclasses = []
            for subclass_phantom in direct_subclasses["nodes_subclass"]:
                subclass = self.get_edge_by_phantom_name(subclass_phantom)
                assert subclass not in visited, f"☠️ Generalization cycle found for '{subclass}' in '{visited}'"
                subclasses.extend([subclass]+self.get_subclasses_by_class_name(subclass, visited + [class_name]))
            return subclasses

    def get_superclasses_by_class_name(self, class_name, visited: list[str] = None) -> list[str]:
        """
        Gives the names of the superclasses of a given class (the class itself is not included in the list)
        :param class_name:
        :param visited: This is necessary for recursion purposes. Initially, it should be just an empty list
        :return: List of superclasses sorted from the bottom top of the hierarchy to the top
        """
        if visited is None:
            visited = []
        all_links = self.get_outbound_generalization_superclasses().reset_index(level="nodes", drop=False).merge(
            self.get_outbound_generalization_subclasses().reset_index(level="nodes", drop=False), on="edges",
            suffixes=("_superclass", "_subclass"), how="inner")
        direct_superclass = all_links[all_links["nodes_subclass"] == self.get_phantom_of_edge_by_name(class_name)]
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            superclass = self.get_edge_by_phantom_name(direct_superclass.iloc[0]["nodes_superclass"])
            assert superclass not in visited, f"☠️ Generalization cycle found for '{superclass}' in '{visited}'"
            return [superclass]+self.get_superclasses_by_class_name(superclass, visited + [class_name])

    def get_generalizations_by_class_name(self, class_name, visited: list[str] = None) -> list[str]:
        if visited is None:
            visited = []
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
            assert superclass not in visited, f"☠️ Generalization cycle found for '{superclass}' in '{visited}'"
            return [generalization]+self.get_generalizations_by_class_name(superclass, visited + [class_name])

    def get_discriminant_by_class_name(self, class_name) -> str:
        return self.get_outbound_generalization_subclasses().reset_index(level="edges", drop=True).loc[
            self.get_phantom_of_edge_by_name(class_name)].misc_properties.get("Constraint", None)

    def is_attribute(self, name) -> bool:
        return name in self.get_attributes().index

    def is_association_end(self, name) -> bool:
        return name in self.get_association_ends().index

    def is_id(self, name) -> bool:
        return name in self.get_ids().index

    def is_class(self, name) -> bool:
        return name in self.get_classes().index

    def is_phantom(self, name) -> bool:
        return name in self.get_phantoms().index

    def is_class_phantom(self, name) -> bool:
        return name in self.get_phantom_classes().index

    def is_association_phantom(self, name) -> bool:
        return name in self.get_phantom_associations().index

    def is_generalization_phantom(self, name) -> bool:
        return name in self.get_phantom_generalizations().index

    def is_hyperedge(self, name) -> bool:
        return name in self.get_edges()["name"]

    def is_association(self, name) -> bool:
        return name in self.get_associations().index

    def is_generalization(self, name) -> bool:
        return name in self.get_generalizations().index

    def is_struct(self, name) -> bool:
        return name in self.get_structs().index

    def is_set(self, name) -> bool:
        return name in self.get_sets().index

    def check_multiplicities_to_one(self, path) -> (bool, bool):
        """
        This method checks if minimum multiplicities in the path are all at least to-one,
        and  if maximum multiplicities in the path are all at most to-one.
        :param path: List of associations.
        :return: Boolean indicating if the path is at least to-one.
        :return: Boolean indicating if the path is at most to-one.
        """
        correct = (True, True)
        for i, current in enumerate(path):
            if self.is_association(current) or self.is_generalization(current):
                assert i > 0, f"☠️ Path '{path}' cannot start with a relationship"
                assert i < len(path)-1, f"☠️ Path '{path}' cannot end with a relationship"
                assert self.is_phantom(path[i-1]) and self.is_phantom(path[i+1]), f"☠️ Path '{path}' must alternate relationships and phantoms"
            if self.is_association(current):
                ends_ahead = self.get_association_ends_by_name(current).query('nodes != "' + path[i-1] + '"')
                assert ends_ahead.shape[0] == 1, f"☠️ Unexpected multiple association ends ahead in association '{current}' of path '{path}'"
                properties = ends_ahead.iloc[0].misc_properties
                assert "MultiplicityMin" in properties, f"☠️ MultiplicityMin not provided for association end '{ends_ahead.iloc[0].name}'"
                assert "MultiplicityMax" in properties, f"☠️ MultiplicityMax not provided for association end '{ends_ahead.iloc[0].name}'"
                correct = (correct[0] and (properties.get("MultiplicityMin") >= 1), correct[1] and (properties.get("MultiplicityMax") <= 1))
            # If it is not an association it still can be a generalization
            elif self.is_generalization(current):
                # Max is always to-one independently of the direction
                # Min is also to-one if it goes upward, but less than one if it goes downwards
                correct = (correct[0] and (self.get_edge_by_phantom_name(path[i+1]) in self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(path[i-1]))), correct[1])
        return correct

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
