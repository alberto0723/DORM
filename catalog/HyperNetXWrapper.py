from abc import abstractmethod
from typing import Self
import logging
import os
import hypernetx as hnx
import pickle
from IPython.display import display
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import duckdb
import uuid

from .config import Config
from .tools import custom_progress, drop_str_duplicates, str_list_difference

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
        elif file_path is not None:
            logger.info(f"Loading hypergraph from '{file_path}'")
            with open(file_path, "rb") as f:
                self.H = pickle.load(f)
        else:
            # In this case, the hypergraph will be filled with load_domain or load_design
            self.H = hnx.Hypergraph([])

    def __del__(self):
        # Remove the temporal DuckDB file
        if os.path.exists(self.duckdb_filename):
            self.sql.close()
            os.remove(self.duckdb_filename)

    ##############################################################################################
    # Basic methods that do NOT requery DuckDB views.
    # They are used for basic checks during the addition of elements to the hypergraph
    ##############################################################################################
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

    def get_association_ends_in_H(self) -> list[str]:
        incidences = self.H.incidences.dataframe
        association_ends = incidences[incidences["misc_properties"].apply(lambda prop: prop['Direction'] == 'Outbound' and prop['Kind'] == 'AssociationIncidence')]
        return association_ends['misc_properties'].apply(lambda prop: prop['End_name']).values.tolist()

    def get_classes_in_H(self) -> list[str]:
        edges = self.H.edges.dataframe
        return edges[edges['misc_properties'].apply(lambda prop: prop['Kind'] == 'Class')].index.tolist()

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

    ##############################################################################################
    # Methods to fill the views in DuckDB
    ##############################################################################################
    @abstractmethod
    def generate_struct_attribute_list(self, struct_name: str) -> dict[str, list[dict[str, str]]]:
        pass

    def fill_duckDB(self):
        custom_progress("Replicating the hypergraph in DuckDB")
        # Create the main views of the hypergraph in DuckDB
        df_nodes = self.H.nodes.to_dataframe.reset_index()
        df_nodes_with_json = pd.json_normalize(df_nodes["misc_properties"])
        df_nodes_flattened = pd.concat([df_nodes.drop(columns="misc_properties"), df_nodes_with_json], axis=1)
        for required in ['Kind', 'DataType', 'Size', 'Subkind']:
            if required not in df_nodes_flattened.columns:
                df_nodes_flattened[required] = None
        self.sql.register("nodes", df_nodes_flattened)
        df_edges = self.H.edges.to_dataframe.reset_index()
        df_edges_with_json = pd.json_normalize(df_edges["misc_properties"])
        df_edges_flattened = pd.concat([df_edges.drop(columns="misc_properties"), df_edges_with_json], axis=1)
        for required in ['Kind', 'Count', 'Complete', 'Disjoint']:
            if required not in df_edges_flattened.columns:
                df_edges_flattened[required] = None
        self.sql.register("edges", df_edges_flattened)
        df_incidences = self.H.incidences.to_dataframe.reset_index()
        df_incidences_with_json = pd.json_normalize(df_incidences["misc_properties"])
        df_incidences_flattened = pd.concat([df_incidences.drop(columns="misc_properties"), df_incidences_with_json], axis=1)
        for required in ['Kind', 'Subkind', 'Direction', 'End_name', 'MultiplicityMin', 'MultiplicityMax', 'Identifier', 'Anchor', 'Constraint']:
            if required not in df_incidences_flattened.columns:
                df_incidences_flattened[required] = None
        self.sql.register("incidences", df_incidences_flattened)
        # Create other derived views in DuckDB
        self.query("""
            CREATE TEMP TABLE class_ids AS
                SELECT edges, nodes
                FROM incidences
                WHERE Kind = 'ClassIncidence' AND Direction = 'Outbound' AND Identifier;
            """)
        # This query requires and outer join to deal with restricted hypergraphs (which may be incomplete)
        self.query("""
            CREATE TEMP TABLE association_ends AS
                SELECT a.edges AS association, a.End_name AS name, c.edges AS class, a.nodes AS phantom, a.MultiplicityMin, a.MultiplicityMax
                FROM incidences a
                    LEFT OUTER JOIN incidences c ON a.nodes=c.nodes AND a.edges<>c.edges
                WHERE a.Kind='AssociationIncidence' AND a.Direction='Outbound'
                    AND (c.nodes IS NULL OR (c.Kind='ClassIncidence' AND c.Direction='Inbound'));
            """)
        self.query("""
            CREATE TEMP TABLE sub_super_pairs AS
                SELECT sub_phantom.edges AS subclass, super.edges AS generalization, super_phantom.edges AS superclass, sub.Constraint
                FROM incidences super
                    JOIN incidences super_phantom ON super.nodes = super_phantom.nodes
                    JOIN incidences sub ON super.edges = sub.edges
                    JOIN incidences sub_phantom ON sub.nodes = sub_phantom.nodes
                WHERE super_phantom.Direction = 'Inbound' AND sub_phantom.Direction = 'Inbound'
                    AND super.Kind = 'GeneralizationIncidence' AND super.Subkind = 'Superclass' AND super.Direction = 'Outbound'
                    AND sub.Kind = 'GeneralizationIncidence' AND sub.Subkind = 'Subclass' AND sub.Direction = 'Outbound'
            """)
        self.query("""
            CREATE TEMP TABLE struct_association_ends AS
                SELECT outgoing.edges AS struct, incoming.nodes AS association_phantom, incoming.edges AS association, outgoing.Anchor, ending.nodes AS end_phantom, ending.End_name, classes.edges AS end_class
                FROM incidences outgoing
                    JOIN incidences incoming ON incoming.nodes=outgoing.nodes
                    JOIN incidences ending ON incoming.edges=ending.edges 
                    JOIN incidences classes ON ending.nodes=classes.nodes
                WHERE outgoing.Direction='Outbound' AND outgoing.Kind='StructIncidence'
                    AND incoming.Direction='Inbound' AND incoming.Kind='AssociationIncidence'
                    AND ending.Direction = 'Outbound' AND ending.Kind='AssociationIncidence'
                    AND classes.Direction='Inbound' AND classes.Kind='ClassIncidence';
            """)
        self.query("""
            CREATE TEMP TABLE struct_attributes AS
                SELECT i.edges AS struct, n.uid AS attribute
                FROM incidences i 
                    JOIN nodes n ON i.nodes=n.uid
                WHERE i.Direction='Outbound' AND i.Kind='StructIncidence'
                    AND n.kind='Attribute';
            """)
        self.query("""
            CREATE TEMP TABLE containments AS
                SELECT outgoing.edges AS parent_edge, outgoing.Anchor, n.uid AS phantom, incomming.edges AS child_edge, n.Subkind AS child_kind, 
                    CASE WHEN outgoing.Kind='SetIncidence' THEN 'Set'
                         WHEN outgoing.Kind='StructIncidence' THEN 'Struct'
                         ELSE NULL
                    END AS parent_kind
                FROM nodes n
                    JOIN incidences outgoing ON n.uid=outgoing.nodes
                    JOIN incidences incomming ON n.uid=incomming.nodes
                WHERE n.Kind='Phantom'  
                    AND outgoing.Direction = 'Outbound' AND outgoing.Kind IN ('SetIncidence', 'StructIncidence')
                    AND incomming.Direction = 'Inbound'
            """)
        self.query("""
            CREATE TEMP TABLE outgoing_atoms AS
                SELECT i.edges AS edge, n.uid AS atom
                FROM incidences i 
                    JOIN nodes n ON i.nodes=n.uid
                WHERE i.Direction='Outbound'
                    AND (n.Kind='Attribute' OR (n.Kind='Phantom' AND n.SubKind IN ('Class', 'Association')));
            """)
        self.query(f"""
            CREATE TEMP TABLE root_edges AS
                SELECT edges AS name, (i_external.Kind='SetIncidence') AS is_set
                FROM incidences i_external
                WHERE i_external.Direction = 'Inbound'
                    AND NOT EXISTS(SELECT 1 
                                    FROM incidences i_internal 
                                    WHERE i_internal.Direction = 'Outbound' 
                                        AND i_external.nodes = i_internal.nodes);
                    """)
        self.query("CREATE TEMP TABLE struct_attribute_list (struct TEXT, attribute_list BLOB);")
        for struct_name in self.get_structs():
            attribute_list = self.generate_struct_attribute_list(struct_name)
            self.sql.execute("INSERT INTO struct_attribute_list (struct, attribute_list) VALUES (?, ?);",
                            (struct_name, pickle.dumps(attribute_list)))
        self.query("CREATE TEMP TABLE atoms_including_transitivity_by_edge_name (edge TEXT, atom TEXT);")
        for edge_name in self.get_root_edges():
            for attribute_name in self.generate_atoms_including_transitivity_by_edge_name(edge_name):
                self.sql.execute("INSERT INTO atoms_including_transitivity_by_edge_name (edge, atom) VALUES (?, ?);",
                             (edge_name, attribute_name))

    ##############################################################################################
    # Methods that use the views in DuckDB
    ##############################################################################################
    def get_struct_attribute_list(self, struct_name: str) -> dict[str, list[dict[str, str]]]:
        return pickle.loads(self.sql.execute(f"SELECT attribute_list FROM struct_attribute_list WHERE struct='{struct_name}';").fetchone()[0])

    def get_parents(self, edge_name):
        return self.query(f"""
            SELECT parent_edge
            FROM containments
            WHERE child_edge='{edge_name}';
            """)

    def get_attribute_names_from_hypergraph(self, temp_H) -> list[str]:
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

        return self.str_list_query("""
            SELECT uid AS name
            FROM tmp_nodes 
            WHERE Kind='Attribute'
            UNION ALL
            SELECT a.End_name AS name
            FROM tmp_incidences a
                --LEFT OUTER JOIN tmp_incidences c ON a.nodes=c.nodes AND a.edges<>c.edges
            WHERE a.Kind='AssociationIncidence' AND a.Direction='Outbound'
                --AND (c.nodes IS NULL OR (c.Kind='ClassIncidence' AND c.Direction='Inbound'));
            """)

    def query(self, query: str) -> pd.DataFrame:
        return self.sql.execute(query).fetchdf()

    def bool_query(self, query: str) -> bool:
        return self.sql.execute(query).fetchone() is not None

    def str_list_query(self, query: str) -> list[str]:
        return self.sql.execute(query).fetch_arrow_table().column(0).to_pylist()

    def save(self, file_path=None) -> None:
        if file_path is not None:
            logger.info(f"Saving hypergraph in '{file_path}'")
            # Create the directory (if it doesn't exist)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Save the hypergraph to a pickle file
            with open(file_path, "wb") as f:
                pickle.dump(self.H, f)

    def get_nodes(self) -> list[str]:
        return self.H.nodes.dataframe.index.to_list()

    def get_edges(self) -> list[str]:
        return self.H.edges.dataframe.index.to_list()

    def get_incidences(self) -> pd.DataFrame:
        incidences = self.H.incidences.dataframe
        return incidences.reset_index(drop=False)[["edges", "nodes"]]

    def get_attributes(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name, DataType, Size FROM nodes WHERE Kind='Attribute';")

    def get_attribute_by_name(self, attr_name: str) -> pd.Series:
        return self.query(f"SELECT uid AS name, DataType, Size FROM nodes WHERE uid='{attr_name}' AND Kind='Attribute';").iloc[0]

    def get_association_ends(self) -> pd.DataFrame:
        return self.query("SELECT * FROM association_ends;")

    def get_association_ends_by_name(self, association_name: str) -> pd.DataFrame:
        return self.query(f"SELECT * FROM association_ends WHERE association='{association_name}';")

    def get_class_name_by_end_name(self, end_name: str) -> str:
        return self.str_list_query(f"SELECT class FROM association_ends WHERE name='{end_name}';")[0]

    def get_ids(self) -> list[str]:
        return self.str_list_query("SELECT nodes as name FROM class_ids;")

    def get_class_id_by_name(self, class_name: str) -> str:
        superclasses = self.get_generalizations_by_class_name(class_name, return_superclasses=True)
        if not superclasses:
            edge_name = class_name
        else:
            # The top of the hierarchy should be the first in the list
            edge_name = superclasses[-1]
        class_id = self.str_list_query(f"SELECT nodes FROM class_ids WHERE edges = '{edge_name}';")
        assert class_id, f"Class {class_name} does not have an identifier"
        return class_id[0]

    def get_class_by_attribute_name(self, attribute_name: str) -> str:
        classes = self.str_list_query(f"SELECT edges AS class FROM incidences WHERE Kind = 'ClassIncidence' AND Direction = 'Outbound' AND nodes='{attribute_name}';")
        assert len(classes) == 1, f"Attribute {attribute_name} does not have exactly one class"
        return classes[0]

    def get_struct_list_per_attribute(self, attr_list: list[str]) -> pd.DataFrame:
        return self.query("""
            SELECT i1.edges AS class_name, i1.nodes AS attribute_name, ARRAY_AGG(i2.edges) AS struct_list
            FROM incidences i1
                JOIN incidences i2 ON i1.nodes = i2.nodes
            WHERE i1.Direction = 'Outbound' AND i1.Kind = 'ClassIncidence'
                AND i2.Direction = 'Outbound' AND i2.Kind='StructIncidence' AND i2.nodes IN ('""" + "','".join(attr_list) + """')
            GROUP BY i1.edges, i1.nodes;""")

    def get_phantoms(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom';")

    def get_phantom_classes(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom' AND Subkind='Class';")

    def get_phantom_associations(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom' AND Subkind='Association';")

    def get_phantom_generalizations(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom' AND Subkind='Generalization';")

    def get_phantom_sets(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom' AND Subkind='Set';")

    def get_phantom_structs(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM nodes WHERE Kind='Phantom' AND Subkind='Struct';")

    def get_edge_by_phantom_name(self, phantom_name: str) -> str:
        return self.str_list_query(f"SELECT edges FROM incidences WHERE Direction = 'Inbound' AND nodes='{phantom_name}';")[0]

    def get_phantom_of_edge_by_name(self, edge_name: str) -> str:
        return self.str_list_query(f"SELECT nodes FROM incidences WHERE Direction = 'Inbound' AND edges='{edge_name}';")[0]

    def get_classes(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name, Count FROM edges WHERE Kind='Class';")

    def get_associations(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM edges WHERE Kind='Association';")

    def get_class_and_association_names(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM edges WHERE Kind IN ('Class','Association');")

    def get_generalizations(self) -> pd.DataFrame:
        return self.query("SELECT uid AS name, Complete, Disjoint FROM edges WHERE Kind='Generalization';")

    def get_structs(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM edges WHERE Kind='Struct';")

    def get_sets(self) -> list[str]:
        return self.str_list_query("SELECT uid AS name FROM edges WHERE Kind='Set';")

    def get_inbounds(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes FROM incidences WHERE Direction = 'Inbound';")

    def get_inbound_classes(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes FROM incidences WHERE Direction = 'Inbound' AND Kind='ClassIncidence';")

    def get_inbound_associations(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes FROM incidences WHERE Direction = 'Inbound' AND Kind='AssociationIncidence';")

    def get_outbounds(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes FROM incidences WHERE Direction = 'Outbound';")

    def get_outbound_classes(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes, Identifier, DistinctVals FROM incidences WHERE Direction = 'Outbound' AND Kind='ClassIncidence';")

    def get_outbound_atoms_by_name(self, edge_name: str) -> list[str]:
        return self.str_list_query(f"SELECT atom FROM outgoing_atoms WHERE edge='{edge_name}';")

    def get_outbound_design_edges_by_name(self, edge_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge AS design_edge FROM containments WHERE parent_edge='{edge_name}' AND child_kind IN ('Set', 'Struct');")

    def get_outbound_associations(self) -> pd.DataFrame:
        return self.query("SELECT edges, nodes, End_name, MultiplicityMax, MultiplicityMin FROM incidences WHERE Direction = 'Outbound' AND Kind='AssociationIncidence';")

    def get_outbound_structs(self) -> pd.DataFrame:
        return self.query(f"SELECT edges, nodes, Anchor FROM incidences WHERE Direction='Outbound' AND Kind='StructIncidence';")

    def get_outbound_sets(self) -> pd.DataFrame:
        return self.query(f"SELECT edges, nodes FROM incidences WHERE Direction='Outbound' AND Kind='SetIncidence';")

    def get_outbound_generalization_superclasses(self) -> list[str]:
        return self.str_list_query(f"""
            SELECT DISTINCT c.edges AS superclass
            FROM incidences g
                JOIN incidences c ON g.nodes=c.nodes
            WHERE g.Direction='Outbound' AND g.Kind='GeneralizationIncidence' AND g.Subkind='Superclass'
                AND c.Direction='Inbound' AND c.Kind='ClassIncidence';
            """)

    def get_outbound_generalization_by_superclasses_name(self, class_name: str) -> pd.DataFrame:
        return self.query(f"""
            SELECT gen.uid AS name, gen.Complete, gen.Disjoint
            FROM incidences gen_inc
                JOIN incidences class_inc ON gen_inc.nodes=class_inc.nodes
                JOIN edges gen ON gen.uid=gen_inc.edges
            WHERE gen_inc.Direction = 'Outbound' AND gen_inc.Kind='GeneralizationIncidence' AND gen_inc.Subkind='Superclass' 
                AND class_inc.Direction='Inbound' AND class_inc.Kind='ClassIncidence' AND class_inc.edges='{class_name}';
            """)

    def get_outbound_generalization_subclasses(self) -> pd.DataFrame:
        return self.query(f"""
            SELECT c.edges AS subclass, g.Constraint
            FROM incidences g
                JOIN incidences c ON g.nodes=c.nodes
            WHERE g.Direction='Outbound' AND g.Kind='GeneralizationIncidence' AND g.Subkind='Subclass'
                AND c.Direction='Inbound' AND c.Kind='ClassIncidence';
            """)

    def get_outbound_generalization_subclasses_by_gen_name(self, gen_name: str) -> list[str]:
        return self.str_list_query(f"SELECT nodes AS phantom FROM incidences WHERE Direction='Outbound' AND Kind='GeneralizationIncidence' AND Subkind='Subclass' AND edges='{gen_name}';")

    def get_outbound_association_by_phantom_name(self, phantom_name: str) -> pd.DataFrame:
        return self.query(f"""
            SELECT classes.edges AS Class, outgoing.End_name 
            FROM incidences outgoing
                JOIN incidences incomming ON outgoing.edges=incomming.edges
                JOIN incidences classes ON outgoing.nodes=classes.nodes
            WHERE outgoing.Direction='Outbound' AND outgoing.Kind='AssociationIncidence' 
                AND incomming.Direction='Inbound' AND incomming.Kind='AssociationIncidence' AND incomming.nodes='{phantom_name}'
                AND classes.Direction='Inbound' AND classes.Kind='ClassIncidence';
            """)

    def get_outbound_struct_by_name(self, struct_name: str) -> pd.DataFrame:
        return self.query(f"SELECT nodes, Anchor FROM incidences WHERE Direction = 'Outbound' AND Kind='StructIncidence' AND edges='{struct_name}';")

    def get_association_names_by_struct_name(self, struct_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Association' AND parent_edge='{struct_name}';")

    def get_struct_names_by_struct_name(self, struct_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Struct' AND parent_edge='{struct_name}';")

    def get_set_names_by_struct_name(self, struct_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Set' AND parent_edge='{struct_name}';")

    def get_class_names_by_struct_name(self, struct_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Class' AND parent_edge='{struct_name}';")

    def get_struct_names_by_set_name(self, set_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Set' AND child_kind='Struct' AND parent_edge='{set_name}';")

    def get_class_names_by_set_name(self, set_name: str) -> list[str]:
        return self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Set' AND child_kind='Class' AND parent_edge='{set_name}';")

    def get_phantom_names_by_set_name(self, set_name: str) -> list[str]:
        return self.str_list_query(f"SELECT nodes AS name FROM incidences WHERE Direction = 'Outbound' AND Kind='SetIncidence' AND edges='{set_name}';")

    def get_unique_outbound_struct_by_phantom_list(self, phantom_list: list[str]) -> list[str]:
        return self.str_list_query("""
            SELECT DISTINCT edges
            FROM incidences
            WHERE Direction = 'Outbound' AND Kind='StructIncidence' AND nodes IN ('""" + "','".join(phantom_list) + "');")

    def get_anchors_by_struct_name(self, struct_name) -> list[str]:
        return self.str_list_query(f"SELECT nodes FROM incidences WHERE Direction = 'Outbound' AND Kind='StructIncidence' AND edges='{struct_name}' AND Anchor;")

    def get_outbound_class_by_name(self, class_name) -> list[str]:
        return self.str_list_query(f"SELECT nodes AS attribute FROM incidences WHERE Kind = 'ClassIncidence' AND Direction = 'Outbound' AND edges='{class_name}';")

    def get_transitive_roots(self, edge_list: list[str], visited: list[str] = None) -> list[str]:
        """
        Given some edges, returns the list of roots containing them, following nested structs and sets.
        :param edge_list: List of edges to find
        :param visited: Visited edges to avoid potential recursion (which should not happen)
        :return: List of roots containing the given edges
        """
        if visited is None:
            visited = edge_list
        else:
            visited = visited + edge_list
        roots = []
        next_edge_list = []
        for edge_name in edge_list:
            parents = self.get_parents(edge_name)
            if parents.empty:
                # It may happen that some classes are not actually present in the design (because of generalizations)
                if self.is_set(edge_name):
                    roots.append(edge_name)
            else:
                next_edge_list.extend([edge for edge in parents["parent_edge"] if edge not in visited])
        if next_edge_list:
            roots.extend(self.get_transitive_roots(next_edge_list, visited))
        return roots

    def generate_atoms_including_transitivity_by_edge_name(self, edge_name, visited: list[str] = None) -> list[str]:
        if visited is None:
            visited = [edge_name]
        else:
            visited.append(edge_name)
        atom_names = self.get_outbound_atoms_by_name(edge_name)
        for next_edge in self.get_outbound_design_edges_by_name(edge_name):
            assert next_edge not in visited, f"☠️ Cycle of edges detected while generating atoms inside an edge including_transitivity: {next_edge} already in {visited}"
            atom_names.extend(self.generate_atoms_including_transitivity_by_edge_name(next_edge, visited))
        visited.pop()
        return atom_names

    def get_atoms_including_transitivity_by_edge_name(self, edge_name) -> list[str]:
        return self.str_list_query(f"SELECT atom FROM atoms_including_transitivity_by_edge_name WHERE edge='{edge_name}';")

    def get_root_edges(self, is_set: bool = True) -> list[str]:
        return self.str_list_query(f"SELECT name FROM root_edges WHERE is_set={is_set};")

    def get_anchor_associations_by_struct_name(self, struct_name) -> list[str]:
        return self.str_list_query(
                f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Association' AND parent_edge='{struct_name}' and Anchor;")

    def get_anchor_points_by_struct_name(self, struct_name) -> list[str]:
        """
        Returns the anchor classes of a struct (i.e., the classes of the end points)
        :param struct_name: Name of the struct
        :return: A list of class names
        """
        # TODO: This is not considering that an anchor of a struct can be in a nested struct (only as root at first level)
        association_ends = self.str_list_query(f"SELECT end_class FROM struct_association_ends WHERE struct='{struct_name}' AND Anchor;")
        classes = self.str_list_query(f"SELECT child_edge FROM containments WHERE parent_kind='Struct' AND child_kind='Class' AND parent_edge='{struct_name}' AND Anchor;")
        return drop_str_duplicates(association_ends + classes)

    def get_anchor_end_names_by_struct_name(self, struct_name) -> list[str]:
        """
        Returns the anchor of a struct
        :param struct_name: Name of the struct
        :return: A list of class names and association end names
        """
        # Need to remove association ends that appear in two associations, hence the NOT EXISTS
        association_ends = self.query(f"""
            SELECT end_phantom, End_name
            FROM struct_association_ends external
            WHERE struct='{struct_name}' AND Anchor AND NOT EXISTS(
                SELECT *
                FROM struct_association_ends internal
                WHERE struct='{struct_name}' AND Anchor AND external.end_class=internal.end_class AND external.End_name<>internal.End_name
                );
            """)
        classes = self.query(f"SELECT phantom, child_edge AS name FROM containments WHERE parent_kind='Struct' AND child_kind='Class' AND parent_edge='{struct_name}' AND Anchor;")
        superclasses = []
        for class_name in classes["name"].values:
            superclasses.extend(self.get_generalizations_by_class_name(class_name, return_superclasses=True))
        superclass_phantoms = [self.get_phantom_of_edge_by_name(p) for p in superclasses]
        loose_ends = association_ends[~association_ends["end_phantom"].isin(classes["phantom"].values.tolist()+superclass_phantoms)]
        return classes["name"].values.tolist()+loose_ends["End_name"].values.tolist()

    def get_loose_association_end_names_by_struct_name(self, struct_name) -> list[str]:
        """
        Returns the loose association ends of a struct.
        :param struct_name: Name of the struct
        :return: A list of association end names
        """
        # Need to remove association ends that appear in two associations, hence the NOT EXISTS
        association_ends = self.query(f"""
            SELECT end_phantom, End_name
            FROM struct_association_ends external
            WHERE struct='{struct_name}' AND NOT EXISTS(
                SELECT *
                FROM struct_association_ends internal
                WHERE struct='{struct_name}' AND external.end_class=internal.end_class AND external.End_name<>internal.End_name
                );
            """)
        classes = self.query(f"SELECT phantom, child_edge AS name FROM containments WHERE parent_kind='Struct' AND child_kind='Class' AND parent_edge='{struct_name}';")
        tight_ends = []
        for edge_name in self.get_outbound_design_edges_by_name(struct_name):
            if self.is_struct(edge_name):
                tight_ends.extend(self.get_anchor_points_by_struct_name(edge_name))
            if self.is_set(edge_name):
                hop_elem_phantom_name = self.get_phantom_names_by_set_name(edge_name)[0]
                assert self.is_struct_phantom(hop_elem_phantom_name) or self.is_class_phantom(hop_elem_phantom_name), f"☠️ The set '{edge_name}' contains '{hop_elem_phantom_name}', which is neither a struct nor a class"
                if self.is_struct_phantom(hop_elem_phantom_name):
                    tight_ends.extend(self.get_anchor_points_by_struct_name(self.get_edge_by_phantom_name(hop_elem_phantom_name)))
                else:
                    tight_ends.append(hop_elem_phantom_name)
        superclasses = []
        for class_name in classes["name"].values:
            superclasses.extend(self.get_generalizations_by_class_name(class_name, return_superclasses=True))
        superclass_phantoms = [self.get_phantom_of_edge_by_name(p) for p in superclasses]
        loose_ends = association_ends[~association_ends["end_phantom"].isin(classes["phantom"].values.tolist()+superclass_phantoms+tight_ends)]
        return loose_ends["End_name"].values.tolist()

    def recursive_contents_by_struct_name(self, struct_name: str) -> [list[str], list[str]]:
        edge_names = self.get_anchor_points_by_struct_name(struct_name) + self.get_class_names_by_struct_name(struct_name) + self.get_association_names_by_struct_name(struct_name)
        attribute_names = self.get_attribute_names_by_struct_name(struct_name)
        sub_struct_names = self.get_struct_names_by_struct_name(struct_name)
        for sub_set in self.get_set_names_by_struct_name(struct_name):
            # Sets can only contain either classes or structs
            sub_classes = self.get_class_names_by_set_name(sub_set)
            edge_names.extend(sub_classes)
            attribute_names.extend([self.get_class_id_by_name(c) for c in sub_classes])
            sub_struct_names.extend(self.get_struct_names_by_set_name(sub_set))
        for sub_struct in sub_struct_names:
            sub_edge_names, sub_attribute_names = self.recursive_contents_by_struct_name(sub_struct)
            edge_names.extend(sub_edge_names)
            attribute_names.extend(sub_attribute_names)
        return edge_names, attribute_names

    def get_restricted_struct_hypergraph(self, struct_name, only_anchor: bool = False, with_attributes: bool = True) -> Self:
        """
        Gives the domain elements inside a given struct.
        :param struct_name: The name of the struct.
        :param only_anchor: Restrict the domain to only edges participating in the anchor of the struct.
        :param with_attributes: Indicates if attributes are included in the resulting hypergraph or not.
        :return: A domain hypergraph.
        """
        if only_anchor:
            edge_names = self.get_anchor_points_by_struct_name(struct_name) + self.get_anchor_associations_by_struct_name(struct_name)
            attribute_names = []
        else:
            edge_names, attribute_names = self.recursive_contents_by_struct_name(struct_name)
            edge_names = drop_str_duplicates(edge_names)
        extended_edge_names = []
        for elem in edge_names:
            if self.is_class(elem):
                extended_edge_names.extend(self.get_generalizations_by_class_name(elem, return_superclasses=True))
                extended_edge_names.extend(self.get_generalizations_by_class_name(elem, return_superclasses=False))
        edge_names = drop_str_duplicates(edge_names + extended_edge_names)
        # It takes all attributes in the classes, but we only want those in the outbounds of the struct, so we remove them one by one
        result = HyperNetXWrapper(name="restricted_"+uuid.uuid4().hex, hypergraph=self.H.restrict_to_edges(edge_names))
        # We use the attributes in the original hypergraph to avoid creating the DuckDB views for restricted structs
        to_be_removed = self.get_attributes()["name"].values.tolist()
        if with_attributes:
            to_be_removed = str_list_difference(to_be_removed, attribute_names)

        result.H.remove_nodes(to_be_removed, inplace=True)
        return result

    def get_attribute_names_by_struct_name(self, struct_name) -> list[str]:
        return self.str_list_query(f"SELECT attribute FROM struct_attributes WHERE struct='{struct_name}';")

    def get_subclasses_by_class_name(self, class_name, visited: list[str] = None) -> list[str]:
        """
        Gives the names of the subclasses of a given class (the class itself is not included in the list)
        :param class_name:
        :param visited: This is necessary for recursion purposes. Initially, it should be just an empty list
        :return: List of subclasses (no sorting can be assumed)
        """
        if visited is None:
            visited = []
        direct_subclasses = self.query(f"""
                    SELECT subclass
                    FROM sub_super_pairs
                    WHERE superclass = '{class_name}';
                    """)
        subclasses = []
        for subclass in direct_subclasses["subclass"]:
            assert subclass not in visited, f"☠️ Generalization cycle found for '{subclass}' in '{visited}'"
            subclasses.extend([subclass]+self.get_subclasses_by_class_name(subclass, visited + [class_name]))
        return subclasses

    def get_generalizations_by_class_name(self, class_name, return_superclasses: bool, visited: list[str] = None) -> list[str]:
        """
        Gives the names of the superclasses or generalizations of a given class (the class itself is not included in the list)
        :param class_name: Name of the bottom of the hierarchy
        :return_superclass: Indicates if the method returns a list of superclasses or generalizations
        :param visited: This is necessary for recursion purposes. Initially, it should be just an empty list
        :return: List of superclasses/generalizations sorted from the bottom top of the hierarchy to the top
        """
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
            if return_superclasses:
                return [superclass]+self.get_generalizations_by_class_name(superclass, return_superclasses, visited + [class_name])
            else:
                return [generalization]+self.get_generalizations_by_class_name(superclass, return_superclasses, visited + [class_name])

    def get_discriminant_by_class_name(self, class_name) -> str:
        return self.query(f"""
                    SELECT sub_super_pairs.Constraint
                    FROM sub_super_pairs
                    WHERE subclass = '{class_name}';
                    """).iloc[0, 0]

    def is_attribute(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid = '{name}' AND Kind='Attribute' LIMIT 1;")

    def is_association_end(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM association_ends WHERE name='{name}' LIMIT 1;")

    def is_id(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM class_ids WHERE nodes='{name}' LIMIT 1;")

    def is_class(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Class' LIMIT 1;")

    def is_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' LIMIT 1;")

    def is_class_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Class' LIMIT 1;")

    def is_association_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Association' LIMIT 1;")

    def is_generalization_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Generalization' LIMIT 1;")

    def is_struct_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Struct' LIMIT 1;")

    def is_set_phantom(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM nodes WHERE uid='{name}' AND Kind='Phantom' AND Subkind='Set' LIMIT 1;")

    def is_edge(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' LIMIT 1;")

    def is_association(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Association' LIMIT 1;")

    def is_generalization(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Generalization' LIMIT 1;")

    def is_struct(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Struct' LIMIT 1;")

    def is_set(self, name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM edges WHERE uid='{name}' AND Kind='Set' LIMIT 1;")

    def is_atom_in_fist_level(self, atom_name, edge_name) -> bool:
        return self.bool_query(f"SELECT 'Found' FROM atoms_including_transitivity_by_edge_name WHERE atom='{atom_name}' AND edge='{edge_name}' LIMIT 1;")

    def has_cycle(self, edge_name, visited: list[str] = None) -> bool:
        if visited is None:
            visited = [edge_name]
        else:
            visited.append(edge_name)
        cyclic = False
        for next_edge in self.get_outbound_design_edges_by_name(edge_name):
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
                correct = (correct[0] and (self.get_edge_by_phantom_name(path[i+1]) in self.get_generalizations_by_class_name(self.get_edge_by_phantom_name(path[i-1]), return_superclasses=True)), correct[1])
        return correct

    def exists_more_generic_struct_in_set(self, struct_name, set_name) -> bool:
        found = False
        struct_anchor_classes = []
        for key in self.get_anchor_end_names_by_struct_name(struct_name):
            if self.is_class(key):
                struct_anchor_classes.append(key)
        for current_struct_name in self.get_struct_names_by_set_name(set_name):
            if current_struct_name != struct_name:
                current_struct_anchor_classes = []
                for key in self.get_anchor_end_names_by_struct_name(current_struct_name):
                    if self.is_class(key):
                        current_struct_anchor_classes.append(key)
                for anchor in struct_anchor_classes:
                    for current_anchor in current_struct_anchor_classes:
                        if anchor != current_anchor:
                            superclasses = self.get_generalizations_by_class_name(anchor, return_superclasses=True)
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
