from typing import Self
import logging
import os
import hypernetx as hnx
import pickle
from IPython.display import display
import pandas as pd
import pandas.testing as pdt
import matplotlib.pyplot as plt
import matplotlib
import duckdb
import uuid

from .config import Config
from .tools import drop_duplicates, df_difference

import time

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
    def __init__(self, name, file_path=None, hypergraph=None):
        self.config = Config()
        os.makedirs("temp_duckdb", exist_ok=True)
        self.duckdb_filename = "temp_duckdb\\"+name+".duckdb"
        if os.path.exists(self.duckdb_filename):
            os.remove(self.duckdb_filename)
        try:
            self.sql = duckdb.connect(self.duckdb_filename)
            logger.info(f"Connection to duckDB '{self.duckdb_filename}' created successfully")
        except duckdb.Error as e:
            raise ValueError(f"🚨 Unable to connect to DuckDB database '{self.duckdb_filename}':", e)
        if hypergraph is not None:
            self.H = hypergraph
            self.fill_duckDB()
            self.create_temp_tables()
        elif file_path is not None:
            logger.info(f"Loading hypergraph from '{file_path}'")
            with open(file_path, "rb") as f:
                self.H = pickle.load(f)
            self.fill_duckDB()
            self.create_temp_tables()
        else:
            self.H = hnx.Hypergraph([])
            self.fill_duckDB()

    def __del__(self):
        # Remove the temporal DuckDB file
        if os.path.exists(self.duckdb_filename):
            self.sql.close()
            os.remove(self.duckdb_filename)

    def fill_duckDB(self):
        #self.H.add_nodes_from([("Fake", {'Kind': None, 'Subkind': None, 'DataType': None, 'Size': None})])
        #self.H.add_edges_from([("Fake", {'Kind': None})])
        #self.H.add_incidences_from([("Fake", "Fake", {'Kind': None, 'Direction': None, 'DistinctVals': None, 'Identifier': None, 'Anchor': None})])
        df_nodes = self.H.nodes.to_dataframe.reset_index()
        df_nodes_with_json = pd.json_normalize(df_nodes["misc_properties"])
        df_nodes_flattened = pd.concat([df_nodes.drop(columns="misc_properties"), df_nodes_with_json], axis=1)
        for required in ['Kind', 'DataType', 'Size']:
            if required not in df_nodes_flattened.columns:
                df_nodes_flattened[required] = None
        self.sql.register("nodes", df_nodes_flattened)
        df_edges = self.H.edges.to_dataframe.reset_index()
        df_edges_with_json = pd.json_normalize(df_edges["misc_properties"])
        df_edges_flattened = pd.concat([df_edges.drop(columns="misc_properties"), df_edges_with_json], axis=1)
        for required in ['Kind']:
            if required not in df_edges_flattened.columns:
                df_edges_flattened[required] = None
        self.sql.register("edges", df_edges_flattened)
        df_incidences = self.H.incidences.to_dataframe.reset_index()
        df_incidences_with_json = pd.json_normalize(df_incidences["misc_properties"])
        df_incidences_flattened = pd.concat([df_incidences.drop(columns="misc_properties"), df_incidences_with_json], axis=1)
        for required in ['Kind', 'Subkind', 'Direction', 'End_name', 'MultiplicityMin', 'MultiplicityMax', 'Identifier']:
            if required not in df_incidences_flattened.columns:
                df_incidences_flattened[required] = None
        self.sql.register("incidences", df_incidences_flattened)
        # display(self.query("SELECT * FROM nodes;"))
        # display(self.query("SELECT * FROM edges;"))
        # display(self.query("SELECT * FROM incidences;"))
        if self.temp_table_exists('class_ids'):
            self.query("DROP TABLE class_ids;")
        self.query("""
            CREATE TEMP TABLE class_ids AS
                SELECT edges, nodes
                FROM incidences
                WHERE Kind = 'ClassIncidence' AND Direction = 'Outbound' AND Identifier;
            """)
        if self.temp_table_exists('association_ends'):
            self.query("DROP TABLE association_ends;")
        # The query requires and outer join to deal with restricted hypergraphs
        self.query("""
            CREATE TEMP TABLE association_ends AS
                SELECT a.edges AS association, a.End_name AS name, c.edges AS class, a.nodes AS phantom, a.MultiplicityMin, a.MultiplicityMax
                FROM incidences a
                    LEFT OUTER JOIN incidences c ON a.nodes=c.nodes AND a.edges<>c.edges
                WHERE a.Kind='AssociationIncidence' AND a.Direction='Outbound'
                    AND (c.nodes IS NULL OR (c.Kind='ClassIncidence' AND c.Direction='Inbound'));
            """)

    def create_temp_tables(self):
        self.query("""
            CREATE TEMP TABLE struct_attributes AS
                SELECT i.edges AS struct, n.uid AS attribute
                FROM incidences i 
                    JOIN nodes n ON i.nodes=n.uid
                WHERE i.Direction='Outbound' AND i.Kind='StructIncidence'
                    AND n.kind='Attribute';
            """)
        self.query("""
            CREATE TEMP TABLE sub_super_pairs AS
                SELECT sub_phantom.edges AS subclass, super.edges AS generalization, super_phantom.edges AS superclass
                FROM incidences super
                    JOIN incidences super_phantom ON super.nodes = super_phantom.nodes
                    JOIN incidences sub ON super.edges = sub.edges
                    JOIN incidences sub_phantom ON sub.nodes = sub_phantom.nodes
                WHERE super_phantom.Direction = 'Inbound' AND sub_phantom.Direction = 'Inbound'
                    AND super.Kind = 'GeneralizationIncidence' AND super.Subkind = 'Superclass' AND super.Direction = 'Outbound'
                    AND sub.Kind = 'GeneralizationIncidence' AND sub.Subkind = 'Subclass' AND sub.Direction = 'Outbound'
            """)

    def get_attribute_names_from_hypergraph(self, temp_H):
        df_nodes = temp_H.nodes.to_dataframe.reset_index()
        df_nodes_with_json = pd.json_normalize(df_nodes["misc_properties"])
        df_nodes_flattened = pd.concat([df_nodes.drop(columns="misc_properties"), df_nodes_with_json], axis=1)
        for required in ['Kind']:
            if required not in df_nodes_flattened.columns:
                df_nodes_flattened[required] = None
        self.sql.register("tmp_nodes", df_nodes_flattened)
        df_incidences = temp_H.incidences.to_dataframe.reset_index()
        df_incidences_with_json = pd.json_normalize(df_incidences["misc_properties"])
        df_incidences_flattened = pd.concat([df_incidences.drop(columns="misc_properties"), df_incidences_with_json], axis=1)
        for required in ['Kind', 'Direction', 'End_name']:
            if required not in df_incidences_flattened.columns:
                df_incidences_flattened[required] = None
        self.sql.register("tmp_incidences", df_incidences_flattened)
        return self.query("""
            SELECT uid AS name
            FROM tmp_nodes 
            WHERE Kind='Attribute'
            UNION ALL
            SELECT a.End_name AS name
            FROM tmp_incidences a
                LEFT OUTER JOIN tmp_incidences c ON a.nodes=c.nodes AND a.edges<>c.edges
            WHERE a.Kind='AssociationIncidence' AND a.Direction='Outbound'
                AND (c.nodes IS NULL OR (c.Kind='ClassIncidence' AND c.Direction='Inbound'));
            """)

    def query(self, query) -> pd.DataFrame:
        return self.sql.execute(query).fetchdf()

    def temp_table_exists(self, table_name) -> bool:
        return self.sql.execute(f"""
            SELECT COUNT(*) > 0 AS table_exists
            FROM information_schema.tables
            WHERE table_catalog = 'temp' AND table_name = '{table_name}'
            """).fetchone()[0]

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
        return pd.merge(self.get_outbound_set_by_name(set_name), self.get_inbound_structs().reset_index("edges", drop=False), on="nodes", how="inner")["edges"].to_list()

    def get_incidences(self) -> pd.DataFrame:
        incidences = self.H.incidences.dataframe
        return incidences

    def get_attributes(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name, DataType, Size FROM nodes WHERE Kind='Attribute';")

    def get_attribute_by_name(self, attr_name) -> pd.Series:
        return self.query(f"SELECT uid AS name, DataType, Size FROM nodes WHERE uid='{attr_name}' AND Kind='Attribute';").iloc[0]

    def get_association_ends(self) -> pd.DataFrame:
        return self.query("""SELECT * FROM association_ends;""")

    def get_association_ends_by_name(self, association_name) -> pd.DataFrame:
        return self.query(f"SELECT * FROM association_ends WHERE association='{association_name}';")

    def get_class_name_by_end_name(self, end_name) -> str:
        return self.query(f"SELECT class FROM association_ends WHERE name='{end_name}';").iloc[0, 0]

    def get_ids(self) -> pd.DataFrame:
        return self.query("SELECT nodes as name FROM class_ids;")

    def get_class_id_by_name(self, class_name) -> str:
        superclasses = self.get_superclasses_by_class_name(class_name)
        if not superclasses:
            edge_name = class_name
        else:
            # The top of the hierarchy should be the first in the list
            edge_name = superclasses[-1]
        class_id = self.query(f"""
            SELECT nodes
            FROM class_ids
            WHERE edges = '{edge_name}';
            """)
        assert not class_id.empty, f"Class {class_name} does not have an identifier"
        return class_id.iat[0, 0]

    def get_class_by_attribute_name(self, attribute_name) -> str:
        classes = self.get_outbound_classes().query('nodes == "' + attribute_name + '"').index.get_level_values("edges")
        assert len(classes) == 1, f"Attribute {attribute_name} does not have exactly one class"
        return classes[0]

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

    def get_phantom_structs(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and
                                                                  x['Subkind'] == 'Struct')]
        return phantoms

    def get_phantom_sets(self) -> pd.DataFrame:
        nodes = self.get_nodes()
        phantoms = nodes[nodes["misc_properties"].apply(lambda x: x['Kind'] == 'Phantom' and
                                                                  x['Subkind'] == 'Set')]
        return phantoms

    def get_edge_by_phantom_name(self, phantom_name) -> str:
        # return self.get_inbounds()[self.get_inbounds().index.get_level_values('nodes') == phantom_name].index[0][0]
        incidences = self.get_incidences()
        phantom_incidences = incidences.xs(phantom_name, level="nodes", drop_level=False)
        phantom_inbounds = phantom_incidences[phantom_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return phantom_inbounds.index[0][0]

    def get_phantom_of_edge_by_name(self, edge_name) -> str:
        # return self.get_inbounds().loc[edge_name].index[0]
        incidences = self.get_incidences()
        edge_incidences = incidences.xs(edge_name, level="edges", drop_level=False)
        edge_inbounds = edge_incidences[edge_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Inbound')]
        return edge_inbounds.index[0][1]

    def get_classes(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name, Count FROM edges WHERE Kind='Class';")

    def get_associations(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name FROM edges WHERE Kind='Association';")

    def get_class_and_association_names(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name FROM edges WHERE Kind IN ('Class','Association');")

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
        # elements = self.get_outbound_associations().query('edges == "' + ass_name + '"')
        # return elements
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            class_incidences = incidences.xs(ass_name, level="edges", drop_level=False)
            outbounds = class_incidences[class_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                             x['Kind'] == 'AssociationIncidence')]
            return outbounds

    def get_outbound_struct_by_name(self, struct_name) -> pd.DataFrame:
        # elements = self.get_outbound_structs().query('edges == "' + struct_name + '"')
        # return elements
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            class_incidences = incidences.xs(struct_name, level="edges", drop_level=False)
            outbounds = class_incidences[class_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                             x['Kind'] == 'StructIncidence')]
            return outbounds

    def get_outbound_set_by_name(self, set_name) -> pd.DataFrame:
        # elements = self.get_outbound_sets().query('edges == "' + set_name + '"')
        # return elements
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            class_incidences = incidences.xs(set_name, level="edges", drop_level=False)
            outbounds = class_incidences[class_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                             x['Kind'] == 'SetIncidence')]
            return outbounds

    def get_outbound_class_by_name(self, class_name) -> pd.DataFrame:
        # elements = self.get_outbound_classes().query('edges == "' + class_name + '"')
        # return elements
        incidences = self.get_incidences()
        if incidences.empty:
            return incidences
        else:
            class_incidences = incidences.xs(class_name, level="edges", drop_level=False)
            outbounds = class_incidences[class_incidences["misc_properties"].apply(lambda x: x['Direction'] == 'Outbound' and
                                                                                             x['Kind'] == 'ClassIncidence')]
            return outbounds

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

    def get_transitive_firstLevels(self, edge_list: list[str], visited: list[str] = None) -> list[str]:
        """
        Given some edges, returns the list of first levels containing them, following nested structs and sets.
        :param edge_list: List of edges to find
        :param visited: Visited edges to avoid potential recursion (which should not happen)
        :return: List of first levels containing the given edges
        """
        if visited is None:
            visited = edge_list
        else:
            visited = visited + edge_list
        firstLevels = []
        next_edge_list = []
        hops = pd.merge(pd.concat([self.get_outbound_sets(), self.get_outbound_structs()]).reset_index(level="edges", drop=False), self.get_inbounds()[self.get_inbounds().index.get_level_values("edges").isin(edge_list)].reset_index(level="edges", drop=False), on='nodes', how='inner', suffixes=('_parent', '_child'))
        for edge_name in edge_list:
            parents = hops.query(f"edges_child == '{edge_name}'")["edges_parent"]
            if parents.empty:
                # It may happen that some classes are not actually present in the design (because of generalizations)
                if self.is_set(edge_name):
                    firstLevels.append(edge_name)
            else:
                next_edge_list.extend([edge for edge in parents.to_list() if edge not in visited])
        if next_edge_list:
            firstLevels.extend(self.get_transitive_firstLevels(next_edge_list, visited))
        return firstLevels

    def get_atoms_including_transitivity_by_edge_name(self, edge_name, visited: list[str] = None) -> list[str]:
        if visited is None:
            visited = [edge_name]
        else:
            visited.append(edge_name)
        atom_names = []
        for node_name in self.get_outbounds().query('edges == "' + edge_name + '"').index.get_level_values("nodes"):
            if self.is_attribute(node_name) or self.is_class_phantom(node_name) or self.is_association_phantom(node_name):
                atom_names.append(node_name)
            elif self.is_generalization_phantom(node_name):
                pass
            # This should only be either a set or struct phantom
            else:
                assert self.is_phantom(node_name), f"Node '{node_name}' is expected to be a phantom"
                next_edge = self.get_edge_by_phantom_name(node_name)
                assert self.is_struct(next_edge) or self.is_set(next_edge), f"Edge '{next_edge}' is expected to be either a struct or a set"
                assert next_edge not in visited, f"☠️ Cycle of edges detected: {next_edge} already in {visited}"
                atom_names.extend(self.get_atoms_including_transitivity_by_edge_name(next_edge, visited))
        visited.pop()
        return atom_names

    def get_inbound_firstLevel(self) -> pd.DataFrame:
        firstLevel_phantoms = df_difference(pd.concat([self.get_phantom_structs(), self.get_phantom_sets()], ignore_index=False).reset_index()[["nodes"]],
                                           self.get_outbounds().reset_index()[["nodes"]])
        firstLevel_incidences = self.get_inbounds().join(firstLevel_phantoms.set_index("nodes"), on="nodes", how='inner')
        return firstLevel_incidences

    def get_anchor_associations_by_struct_name(self, struct_name) -> list[str]:
        elements = self.get_outbound_struct_by_name(struct_name)
        anchor_elements = elements[elements["misc_properties"].apply(lambda x: x['Anchor'])]
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        anchor_associations = pd.merge(anchor_elements, inbounds, on="nodes", how="inner")["edges"].to_list()
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
        loose_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)["nodes"].to_list()
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner').index.to_list()
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
            return classes.index.to_list()
        else:
            end_names = loose_ends.apply(lambda x: str(x.get("misc_properties").get("End_name")), axis=1).to_list()
            return classes.index.to_list()+end_names

    def get_loose_association_end_names_by_struct_name(self, struct_name) -> list[str]:
        elements = self.get_outbound_struct_by_name(struct_name)
        inbounds = self.get_inbound_associations()
        inbounds["edges"] = inbounds.index.get_level_values("edges")
        associations = pd.merge(elements, inbounds, on="nodes", suffixes=("_elements", "_inbounds"), how='inner')
        outbounds = self.get_outbound_associations()
        outbounds["nodes"] = outbounds.index.get_level_values("nodes")
        association_ends = pd.merge(associations, outbounds, on="edges", suffixes=("_associations", "_outbounds"), how='inner').groupby("nodes").filter(lambda x: len(x) == 1)
        classes = pd.merge(elements, self.get_inbound_classes(), on="nodes", suffixes=("_elements", "_classes"), how='inner')
        tight_ends = []
        for elem_phantom_name in elements.index.get_level_values("nodes"):
            if self.is_struct_phantom(elem_phantom_name):
                tight_ends.extend(self.get_anchor_points_by_struct_name(self.get_edge_by_phantom_name(elem_phantom_name)))
            if self.is_set_phantom(elem_phantom_name):
                hop_elem_phantom_name = self.get_outbound_set_by_name(self.get_edge_by_phantom_name(elem_phantom_name)).index.get_level_values("nodes").to_list()[0]
                assert self.is_struct_phantom(hop_elem_phantom_name) or self.is_class_phantom(hop_elem_phantom_name), f"☠️ The set '{elem_phantom_name}' contains '{hop_elem_phantom_name}', which is neither a struct nor a class"
                if self.is_struct_phantom(hop_elem_phantom_name):
                    tight_ends.extend(self.get_anchor_points_by_struct_name(self.get_edge_by_phantom_name(hop_elem_phantom_name)))
                else:
                    tight_ends.append(hop_elem_phantom_name)
        superclass_phantoms = []
        for class_phantom_name in classes.index.to_list():
            superclass_phantoms.extend(self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(class_phantom_name)))
        superclasses = [self.get_phantom_of_edge_by_name(p) for p in superclass_phantoms]
        loose_ends = association_ends[~association_ends["nodes"].isin(classes.index.to_list()+superclasses+tight_ends)]

        if loose_ends.empty:
            return []
        else:
            end_names = loose_ends.apply(lambda x: str(x.get("misc_properties").get("End_name")), axis=1).to_list()
            return end_names

    def get_restricted_struct_hypergraph(self, struct_name, only_anchor=False) -> Self:
        anchor_points = self.get_anchor_points_by_struct_name(struct_name)
        if only_anchor:
            outbounds = [self.get_phantom_of_edge_by_name(ass) for ass in self.get_anchor_associations_by_struct_name(struct_name)]
        else:
            outbounds = self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes").to_list()
        edge_names = []
        for elem in drop_duplicates(outbounds + anchor_points):
            if self.is_class_phantom(elem) or self.is_association_phantom(elem) or self.is_generalization_phantom(elem):
                edge_names.append(self.get_edge_by_phantom_name(elem))
                if self.is_class_phantom(elem) and elem in outbounds:
                    edge_names.extend(self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(elem)))
                    edge_names.extend(self.get_generalizations_by_class_name(self.get_edge_by_phantom_name(elem)))
        # It takes all attributes in the classes, but we only want those in the outbounds, so we remove them one by one
        result = HyperNetXWrapper(name="restricted_"+uuid.uuid4().hex, hypergraph=self.H.restrict_to_edges(edge_names))
        to_be_removed = []
        for attr_name in result.get_attributes()["name"].to_list():
            if attr_name not in outbounds:
                to_be_removed.append(attr_name)
        result.H.remove_nodes(to_be_removed, inplace=True)
        return result

    def get_attribute_names_by_struct_name(self, struct_name) -> list[str]:
        return self.query(f"SELECT attribute FROM struct_attributes WHERE struct='{struct_name}';")["attribute"].to_list()

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
        direct_superclass = self.query(f"""
                    SELECT superclass
                    FROM sub_super_pairs
                    WHERE subclass = '{class_name}';
                    """)
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            superclass = direct_superclass.iat[0, 0]
            assert superclass not in visited, f"☠️ Generalization cycle found for '{superclass}' in '{visited}'"
            return [superclass]+self.get_superclasses_by_class_name(superclass, visited + [class_name])

    def get_generalizations_by_class_name(self, class_name, visited: list[str] = None) -> list[str]:
        if visited is None:
            visited = []
        direct_superclass = self.query(f"""
                    SELECT generalization, superclass
                    FROM sub_super_pairs
                    WHERE subclass = '{class_name}';
                    """)
        if direct_superclass.empty:
            return []
        else:
            # This means there is one superclass (multiple-inheritance is not allowed)
            generalization = direct_superclass.iat[0, 0]
            superclass = direct_superclass.iat[0, 1]
            assert superclass not in visited, f"☠️ Generalization cycle found for '{superclass}' in '{visited}'"
            return [generalization]+self.get_generalizations_by_class_name(superclass, visited + [class_name])

    def get_discriminant_by_class_name(self, class_name) -> str:
        return self.get_outbound_generalization_subclasses().reset_index(level="edges", drop=True).loc[
            self.get_phantom_of_edge_by_name(class_name)].misc_properties.get("Constraint", None)

    def is_attribute(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid = '{name}' AND Kind='Attribute';").empty

    def is_association_end(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM association_ends WHERE name='{name}';").empty

    def is_id(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM class_ids WHERE nodes='{name}';").empty

    def is_class(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Class';").empty

    def is_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom';").empty

    def is_class_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Class';").empty

    def is_association_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Association';").empty

    def is_generalization_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Generalization';").empty

    def is_struct_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Struct';").empty

    def is_set_phantom(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Set';").empty

    def is_edge(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}';").empty

    def is_association(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Association';").empty

    def is_generalization(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Generalization';").empty

    def is_struct(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Struct';").empty

    def is_set(self, name) -> bool:
        return not self.query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Set';").empty

    def has_cycle(self, edge_name, visited: list[str] = None) -> bool:
        if visited is None:
            visited = [edge_name]
        else:
            visited.append(edge_name)
        cyclic = False
        for node_name in self.get_outbounds().query('edges == "' + edge_name + '"').index.get_level_values("nodes"):
            if self.is_phantom(node_name):
                next_edge = self.get_edge_by_phantom_name(node_name)
                if self.is_struct(next_edge) or self.is_set(next_edge):
                    if next_edge in visited:
                        cyclic = True
                    else:
                        cyclic = cyclic or self.has_cycle(next_edge, visited)
        visited.pop()
        return cyclic

    def check_multiplicities_to_one(self, path) -> (bool, bool):
        """
        This method checks if minimum multiplicities in the path are all at least to-one,
        and if maximum multiplicities in the path are all at most to-one.
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
                ends_ahead = self.get_association_ends_by_name(current).query('phantom != "' + path[i-1] + '"')
                assert ends_ahead.shape[0] == 1, f"☠️ Unexpected multiple association ends ahead in association '{current}' of path '{path}'"
                assert not pd.isnull(ends_ahead.iloc[0]["MultiplicityMin"]), f"☠️ MultiplicityMin not provided for association end '{ends_ahead.iloc[0].end}'"
                assert not pd.isnull(ends_ahead.iloc[0]["MultiplicityMax"]), f"☠️ MultiplicityMax not provided for association end '{ends_ahead.iloc[0].end}'"
                correct = (correct[0] and (ends_ahead.iloc[0]["MultiplicityMin"] >= 1), correct[1] and (ends_ahead.iloc[0]["MultiplicityMax"] <= 1))
            # If it is not an association it still can be a generalization
            elif self.is_generalization(current):
                # Max is always to-one independently of the direction
                # Min is also to-one if it goes upward, but less than one if it goes downwards
                correct = (correct[0] and (self.get_edge_by_phantom_name(path[i+1]) in self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(path[i-1]))), correct[1])
        return correct

    def exists_more_generic_struct_in_set(self, struct_name, set_name) -> bool:
        found = False
        struct_anchor_classes = []
        for key in self.get_anchor_end_names_by_struct_name(struct_name):
            if self.is_class_phantom(key):
                struct_anchor_classes.append(self.get_edge_by_phantom_name(key))
        struct_phantom_list = pd.merge(self.get_outbound_set_by_name(set_name), self.get_phantom_structs(), on="nodes", how="inner").index
        for current_struct_phantom in struct_phantom_list:
            current_struct_name = self.get_edge_by_phantom_name(current_struct_phantom)
            if current_struct_name != struct_name:
                current_struct_anchor_classes = []
                for key in self.get_anchor_end_names_by_struct_name(current_struct_name):
                    if self.is_class_phantom(key):
                        current_struct_anchor_classes.append(self.get_edge_by_phantom_name(key))
                for anchor in struct_anchor_classes:
                    for current_anchor in current_struct_anchor_classes:
                        if anchor != current_anchor:
                            superclasses = self.get_superclasses_by_class_name(anchor)
                            found = found or (current_anchor in superclasses)
        return found

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
