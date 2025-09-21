from abc import abstractmethod
import logging
import warnings
import json
import networkx as nx
from IPython.display import display
import pandas as pd
import sqlparse
from pathlib import Path
from tqdm import tqdm

from . import config
from .tools import custom_warning, custom_progress, combine_buckets, drop_duplicates, df_difference, extract_up_to_folder
from .HyperNetXWrapper import HyperNetXWrapper
from .XML2JSON.domain.DomainTranslator import translate as translate_domain
from .XML2JSON.design.DesignTranslator import translate as translate_design

# Libraries initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("Catalog")
warnings.showwarning = custom_warning


class Catalog(HyperNetXWrapper):
    """This class contains the main generic operations to build the catalog of a database using hypergraphs.
    It uses HyperNetX (https://github.com/pnnl/HyperNetX).
    Moreover, it implements the most general consistency checks.
    """
    # This attributes keep track of the metadata of the catalog, including domain and design files
    metadata = {}
    # This attribute keeps a dataframe with all the insertion guards
    guards = None

    def __init__(self, *args, **kwargs):
        logger.info("Creating a catalog")
        super().__init__(*args, **kwargs)

    def get_metadata(self) -> dict[str, str]:
        return self.metadata

    def add_class(self, class_name, properties, att_list) -> None:
        """Besides the class name and the number of instances of the class, this method requires
        a list of attributes, where each attribute is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that can contain any key, but at least it should contain
        'DataType' (string), 'Size' (numeric), 'DistinctVals' (numeric).
        """
        logger.info("Adding class "+class_name)
        if self.is_attribute(class_name) or self.is_association_end(class_name) or self.is_edge(class_name):
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
            if self.is_attribute(att['name']) or self.is_association_end(att['name']) or self.is_edge(att['name']):
                raise ValueError(f"🚨 Some element end called '{att['name']}' already exists")
            incidence_properties = {'Kind': 'ClassIncidence',
                                    'Direction': 'Outbound',
                                    'DistinctVals': att['prop'].pop('DistinctVals'),
                                    'Identifier': att['prop'].pop('Identifier', False)}
            incidences.append((class_name, att['name'], incidence_properties))
            if att['name'] in self.get_nodes()["name"]:
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
        if self.is_attribute(association_name) or self.is_association_end(association_name) or self.is_edge(association_name):
            raise ValueError(f"🚨 The element '{association_name}' already exists")
        if len(ends_list) != 2:
            raise ValueError(f"🚨 The association '{association_name}' should have exactly two ends, but has {len(ends_list)}")
        self.H.add_edge(association_name, Kind='Association')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+association_name, Kind='Phantom', Subkind='Association')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(association_name, self.config.prepend_phantom+association_name, {'Kind': 'AssociationIncidence', 'Direction': 'Inbound'})]
        for end in ends_list:
            if not self.is_class(end['class']):
                raise ValueError(f"🚨 The class '{end['class']}' in '{association_name}' does not exists")
            end_name = end['prop'].get('End_name', None)
            if end_name is None:
                raise ValueError(f"🚨 Association end '{association_name}' does not have a name for its end towards '{end['class']}'")
            if self.is_attribute(end_name) or self.is_association_end(end_name) or self.is_edge(end_name):
                raise ValueError(f"🚨 There is already an element called '{end_name}'")
            if end['prop'].get('MultiplicityMax', None) is None or end['prop'].get('MultiplicityMin', None) is None:
                raise ValueError(f"🚨 '{association_name}' does not have both min and max multiplicity for its end '{end_name}'")
            end['prop']['Kind'] = 'AssociationIncidence'
            end['prop']['Direction'] = 'Outbound'
            incidences.append((association_name, self.get_phantom_of_edge_by_name(end['class']), end['prop']))
        self.H.add_incidences_from(incidences)

    def add_generalization(self, generalization_name, properties, superclass, subclasses_list) -> None:
        """ Besides the generalization name, this method requires some properties (expected to be two booleans) for
        disjointness and completeness, the name of the superclass and a list of subclasses,
        where each subclass is a dictionary with the keys 'name' and 'prop'.
        The latter is another dictionary that contains at least one constraint predicate that discriminates the subclass.
        """
        logger.info("Adding generalization "+generalization_name)
        if self.is_attribute(generalization_name) or self.is_association_end(generalization_name) or self.is_edge(generalization_name):
            raise ValueError(f"🚨 The element called '{generalization_name}' already exists")
        self.H.add_edge(generalization_name, Kind='Generalization', Disjoint=properties.get('Disjoint', False), Complete=properties.get('Complete', False))
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+generalization_name, Kind='Phantom', Subkind='Generalization')
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(generalization_name, self.config.prepend_phantom+generalization_name, {'Kind': 'GeneralizationIncidence', 'Direction': 'Inbound'})]
        if not self.is_class(superclass):
            raise ValueError(f"🚨 The superclass '{superclass}' in '{generalization_name}' does not exists")
        # First element in the pair of incidences is the edge name and the second the node
        incidences.append((generalization_name,  self.get_phantom_of_edge_by_name(superclass), {'Kind': 'GeneralizationIncidence', 'Subkind': 'Superclass', 'Direction': 'Outbound'}))
        if len(subclasses_list) < 1:
            raise ValueError(f"🚨 The generalization '{generalization_name}' should have at least one subclass")
        for sub in subclasses_list:
            if superclass == sub['class']:
                raise ValueError(f"🚨 The same class '{superclass}' cannot play super and sub roles in generalization '{generalization_name}'")
            if not self.is_class(sub['class']):
                raise ValueError(f"🚨 The subclass '{superclass}' in '{generalization_name}' does not exists")
            sub['prop']['Kind'] = 'GeneralizationIncidence'
            sub['prop']['Subkind'] = 'Subclass'
            sub['prop']['Direction'] = 'Outbound'
            incidences.append((generalization_name, self.get_phantom_of_edge_by_name(sub['class']), sub['prop']))
        self.H.add_incidences_from(incidences)

    def add_struct(self, struct_name, anchor, elements) -> None:
        logger.info("Adding struct "+struct_name)
        if self.is_edge(struct_name):
            raise ValueError(f"🚨 The hyperedge '{struct_name}' already exists")
        if not anchor:
            raise ValueError(f"🚨 Struct '{struct_name}' does not have any anchor")
        for elem in anchor:
            if not self.is_class(elem) and not self.is_association(elem):
                raise ValueError(f"🚨 The anchor of '{struct_name}' (i.e., '{elem}') must be either a class or an association")
        self.H.add_edge(struct_name, Kind='Struct')
        # This adds a special phantom node required to represent different cases of inclusion in structs
        self.H.add_node(self.config.prepend_phantom+struct_name, Kind='Phantom', Subkind="Struct")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(struct_name, self.config.prepend_phantom+struct_name, {'Kind': 'StructIncidence', 'Direction': 'Inbound'})]
        for elem in drop_duplicates(elements+anchor):
            if self.is_attribute(elem):
                incidences.append((struct_name, elem, {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_association(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_class(elem):
                # Only one element of a hierarchy can be included by the user in the elements of a struct
                included_superclasses = [c for c in self.get_superclasses_by_class_name(elem) if c in elements]
                if included_superclasses:
                    raise ValueError(f"🚨 Only one class per hierarchy can be included in the elements of a struct ('{struct_name}' got '{elem} and '{included_superclasses}')")
                # Add the class to the struct
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
                # Add the identifier to the struct
                incidences.append((struct_name, self.get_class_id_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': False}))
                # We do need to have the generalizations in the struct to generate a restricted struct correctly including superclasses
                for g in self.get_generalizations_by_class_name(elem, []):
                    incidences.append((struct_name, self.get_phantom_of_edge_by_name(g), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': False}))
            elif self.is_struct(elem) or self.is_set(elem):
                incidences.append((struct_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'StructIncidence', 'Direction': 'Outbound', 'Anchor': (elem in anchor)}))
            elif self.is_generalization(elem):
                pass
            else:
                raise ValueError(f"🚨 Creating struct '{struct_name}' could not find '{elem}' to place it inside (check both domain and design)")
        self.H.add_incidences_from(incidences)
        # Check if the classes and associations in the struct are connected
        restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
        if not restricted_struct.H.is_connected():
            raise ValueError(f"🚨 Struct '{struct_name}' is not connected")
        # Check if attributes in the struct are connected
        connected_attributes = restricted_struct.get_attributes().index
        for elem in elements:
            if self.is_attribute(elem) and elem not in connected_attributes:
                raise ValueError(f"🚨 Attribute '{elem}' in struct '{struct_name}' is not connected")
        # Check if the associations in the anchor are connected (this should consider inheritance of associations)
        if not restricted_struct.H.restrict_to_edges(anchor).is_connected():
            raise ValueError(f"🚨 The anchor of struct '{struct_name}' is not connected")

    def add_set(self, set_name, elements) -> None:
        logger.info("Adding set "+set_name)
        if set_name in self.get_edges()["name"]:
            raise ValueError(f"🚨 The hyperedge '{set_name}' already exists")
        if len(elements) == 0:
            raise ValueError(f"🚨 The set '{set_name}' should have some elements, but has {len(elements)}")
        self.H.add_edge(set_name, Kind='Set')
        # This adds a special phantom node required to represent different cases of inclusion in sets
        self.H.add_node('Phantom_'+set_name, Kind='Phantom', Subkind="Set")
        # First element in the pair of incidences is the edge name and the second the node
        incidences = [(set_name, self.config.prepend_phantom+set_name, {'Kind': 'SetIncidence', 'Direction': 'Inbound'})]
        for elem in elements:
            if self.is_class(elem):
                incidences.append((set_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_association(elem) or self.is_struct(elem):
                incidences.append((set_name, self.get_phantom_of_edge_by_name(elem), {'Kind': 'SetIncidence', 'Direction': 'Outbound'}))
            elif self.is_attribute(elem):
                raise ValueError(f"🚨 Sets cannot contain attributes (adding '{elem}' into '{set_name}')")
            elif self.is_set(elem):
                raise ValueError(f"🚨 Sets cannot contain sets (adding '{elem}' into '{set_name}')")
            else:
                raise ValueError(f"🚨 Creating set '{set_name}' could not find the kind of '{elem}' to place it inside (the element may not exist in the domain)")
        self.H.add_incidences_from(incidences)

    def load_domain(self, file_path: Path, file_format="JSON") -> None:
        logger.info(f"Loading domain from '{file_path}'")
        self.metadata["domain"] = Path(file_path).stem
        assert file_format in ["JSON", "XML"], "🚨 The format of the domain specification file must be either 'JSON' or 'XML'"
        if file_format == "XML":
            custom_progress(f"Reading XML domain")
            new_file_path = file_path.with_suffix(".json")
            custom_progress(f"Generating JSON and storing it in {new_file_path}")
            with open(new_file_path, 'w') as f:
                f.write(translate_domain(file_path))
            file_path = new_file_path
        # Open and load the JSON file
        custom_progress(f"Loading domain from {file_path}")
        with open(file_path, 'r') as f:
            domain = json.load(f)
        # Create and fill the catalog
        for cl in tqdm(domain.get("classes"), desc="Creating classes", leave=config.show_progress):
            self.add_class(cl.get("name"), cl.get("prop"), cl.get("attr"))
        for ass in tqdm(domain.get("associations", []), desc="Creating associations", leave=config.show_progress):
            self.add_association(ass.get("name"), ass.get("ends"))
        for gen in tqdm(domain.get("generalizations", []), desc="Creating generalizations", leave=config.show_progress):
            self.add_generalization(gen.get("name"), gen.get("prop"), gen.get("superclass"), gen.get("subclasses"))
        self.guards = pd.DataFrame(domain.get("guards", []))

    def load_design(self, file_path: Path, file_format="JSON") -> None:
        logger.info(f"Loading design from '{file_path}'")
        assert file_format in ["JSON", "XML"], "🚨 The format of the design specification file must be either 'JSON' or 'XML'"
        if file_format == "XML":
            custom_progress(f"Reading XML design")
            new_file_path = file_path.with_suffix(".json")
            custom_progress(f"Generating JSON and storing it in {new_file_path}")
            with open(new_file_path, 'w') as f:
                f.write(translate_design(file_path))
            file_path = new_file_path
        # Open and load the JSON file
        custom_progress(f"Loading design from {file_path}")
        with open(file_path, 'r') as f:
            design = json.load(f)
        domain_path = extract_up_to_folder(file_path, "designs").parent.joinpath("domains").joinpath(design.get("domain", None)).with_suffix("."+file_format).resolve()
        if "domain" not in self.metadata:
            self.load_domain(domain_path, file_format)
        # Check if the domain in the catalog and that of the design coincide
        if self.metadata.get("domain", "Non-existent") != Path(domain_path).stem:
            raise ValueError(f"🚨 The domain of the design '{Path(domain_path).stem}' does not coincide with that of the catalog '{self.metadata.get('domain', 'Non-existent')}'")
        self.metadata["design"] = Path(file_path).stem

        # Create and fill the catalog
        for h in tqdm(design.get("hyperedges"), desc="Creating design constructs", leave=config.show_progress):
            if h.get("kind") == "Struct":
                self.add_struct(h.get("name"), h.get("anchor"), h.get("elements"))
            elif h.get("kind") == "Set":
                self.add_set(h.get("name"), h.get("elements"))
            else:
                raise ValueError(f"🚨 Unknown kind of hyperedge '{h.get('kind')}'")

        logger.info("Checking the insertion guards")
        # Check insertion guards
        for guard in tqdm(self.guards.itertuples(), desc="Checking guards", leave=config.show_progress):
            self.get_insertion_alternatives(guard.pattern, guard.data)

    @staticmethod
    def get_domain_attribute_from_path(attr_path: list[dict[str, str]]) -> str:
        final_hop = attr_path[-1]
        assert final_hop.get("kind", "") in ["Attribute", "AssociationEnd"], f"☠️ Incorrect attribute path '{attr_path}', which should end with either an Attribute or AssociationEnd"
        assert "name" in final_hop, f"☠️ Incorrect attribute path '{attr_path}', whose final entry should have a name"
        if final_hop.get("kind") == "Attribute":
            return final_hop["name"]
        else:
            assert "id" in final_hop, f"☠️ Incorrect attribute path '{attr_path}', whose final entry is an AssociationEnd without 'id' key in it"
            return final_hop.get("id")

    @abstractmethod
    def generate_attr_projection_clause(self, attr_path: list[dict[str, str]]) -> None:
        """
        This generates the projection clause for a given an attribute path as produced by 'get_struct_attributes'
        :param attr_path: List of element hops
        :return: Projection clause depending on the implementation
        """
        assert len(attr_path) > 0, f"☠️ Incorrect length of attribute path '{attr_path}', which cannot be zero"
        assert all("name" in hop for hop in attr_path), f"☠️ Incorrect attribute path '{attr_path}', whose hops should have a name"
        assert attr_path[-1].get("kind", "") in ["Attribute", "AssociationEnd"], f"☠️ Incorrect attribute path '{attr_path}', whose last hop should be either an Attribute or AssociationEnd"
        return None

    def get_struct_attributes(self, struct_name) -> list[tuple[str, list[dict[str, str]]]]:
        """
        This generates the correspondence between attribute names in a struct and their corresponding attribute.
        It is necessary to do it to consider loose ends (i.e., associations without class), which generate foreign keys.
        It includes the attributes in nested structs and sets.
        :param struct_name:
        :return: A list of tuples with pairs "attribute_name" and a list of elements.
                 Each element is a dictionary itself, which represents a hop in the design (though sets and structs).
                 The last element corresponds to the "domain_name" in the hypergraph for the attribute, which can be the same attribute or association end.
        """
        # This cannot be a dictionary with the domain attribute name as key, because two loose ends over the same class would use the same entry
        # Hence, this is a list of tuples, with the first element being an attribute name, and the second a path to it
        attribute_list = []
        loose_ends = self.get_loose_association_end_names_by_struct_name(struct_name)
        # For each element in the struct
        elem_names = self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes")
        for elem_name in elem_names:
            assert self.is_attribute(elem_name) or self.is_class_phantom(elem_name) or self.is_association_phantom(elem_name) or self.is_generalization_phantom(elem_name) or self.is_struct_phantom(elem_name) or self.is_set_phantom(elem_name), f"☠️ Some element in struct '{struct_name}' is not expected: '{elem_name}'"
            if self.is_attribute(elem_name):
                attribute_list.append((elem_name, [{"kind": "Attribute", "name": elem_name}]))
            elif self.is_class_phantom(elem_name):
                # Add the class identifier if there is not any other attribute of the same class
                class_name = self.get_edge_by_phantom_name(elem_name)
                if not self.get_outbound_class_by_name(class_name).index.get_level_values('nodes').isin(elem_names).any():
                    attribute_list.append((self.get_class_id_by_name(class_name), [{"kind": "Attribute", "name": self.get_class_id_by_name(class_name)}]))
            elif self.is_association_phantom(elem_name):
                ends = self.get_outbound_association_by_name(self.get_edge_by_phantom_name(elem_name))
                for end in ends.itertuples():
                    if end.misc_properties["End_name"] in loose_ends:
                        attribute_list.append((end.misc_properties['End_name'],
                                               [{"kind": "AssociationEnd", "name": end.misc_properties['End_name'], "id": self.get_class_id_by_name(self.get_edge_by_phantom_name(end.Index[1]))}]))
            elif self.is_struct_phantom(elem_name):
                nested_struct_name = self.get_edge_by_phantom_name(elem_name)
                for attr_name, attr_path in self.get_struct_attributes(nested_struct_name):
                    attribute_list.append((attr_name, [{"kind": "Struct", "name": nested_struct_name}]+attr_path))
            elif self.is_set_phantom(elem_name):
                nested_set_name = self.get_edge_by_phantom_name(elem_name)
                for nested_element_phantom_name in self.get_outbound_set_by_name(nested_set_name).index.get_level_values("nodes"):
                    assert self.is_class_phantom(nested_element_phantom_name) or self.is_struct_phantom(nested_element_phantom_name), f"☠️ Set '{nested_set_name}' contains '{nested_element_phantom_name}', which is neither a class nor a struct"
                    nested_element_name = self.get_edge_by_phantom_name(nested_element_phantom_name)
                    if self.is_class(nested_element_name):
                        attr_name = self.get_class_id_by_name(nested_element_name)
                        attribute_list.append((attr_name, [{"kind": "Set", "name": nested_set_name}, {"kind": "Attribute", "name": attr_name}]))
                    # If not a class, it must be a struct
                    else:
                        for attr_name, attr_path in self.get_struct_attributes(nested_element_name):
                            attribute_list.append((attr_name, [{"kind": "Set", "name": nested_set_name}] + attr_path))
        # We need to remove duplicates to avoid ids appearing twice
        attribute_list = drop_duplicates(attribute_list)
        assert len(attribute_list) == len(set(drop_duplicates([t[0] for t in attribute_list]))), f"☠️ There is some ambiguous attribute name in '{struct_name}': {attribute_list}"
        return attribute_list

    def is_consistent(self, design=False) -> bool:
        """
        This method checks all the integrity constrains of the catalog.
        It can be expensive, so just do it at the end, not for each operation.
        :param design: Whether the catalog contains a design, or just a domain (more or less ICs will be checked)
        :return: If the catalog is honors all integrity constraints
        """
        consistent = True
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
        outbounds = self.get_outbounds()
        structOutbounds = self.get_outbound_structs()
        setOutbounds = self.get_outbound_sets()

        # -------------------------------------------------------------------------------------------------- Generic ICs
        custom_progress("    Checking generic domain constraints")

        # Pre-check emptiness
        logger.info("Checking emptiness")
        if self.get_nodes().empty or self.get_edges().empty or self.get_incidences().empty:
            print(f"This is a degenerated hypergraph: {self.get_nodes().shape[0]} nodes, {self.get_edges().shape[0]} edges, and {self.get_incidences().shape[0]} incidences")
            return False

        # IC-Generic1: Names must be unique
        logger.info("Checking IC-Generic1")
        union1_1 = pd.concat([self.get_nodes()["name"], self.get_edges()["name"]], ignore_index=True)
        violations1_1 = union1_1.groupby(union1_1).size()
        if not violations1_1[violations1_1 > 1].empty:
            consistent = False
            print("🚨 IC-Generic1 violation: Some names are not unique")
            display(violations1_1[violations1_1 > 1])

        # IC-Generic2: The catalog must be connected
        logger.info("Checking IC-Generic2")
        if not self.H.is_connected(s=1):
            consistent = False
            print("🚨 IC-Generic2 violation: The catalog is not connected")

        # IC-Generic3: Every phantom belongs to one edge
        logger.info("Checking IC-Generic3")
        matches1_3 = inbounds.join(edges, on='edges', rsuffix='_edges', how='inner')
        violations1_3 = phantoms[~phantoms["name"].isin((matches1_3.reset_index(drop=False))["nodes"])]
        if not violations1_3.empty:
            consistent = False
            print("🚨 IC-Generic3 violation: There are phantoms without an edge")
            display(violations1_3)

        # IC-Generic4: Every edge has at least one inbound
        logger.info("Checking IC-Generic4")
        matches1_4 = self.get_inbounds().reset_index(level='nodes', drop=True).reset_index(drop=False)['edges']
        violations1_4 = df_difference(edges.reset_index(drop=False)['edges'], matches1_4)
        if not violations1_4.empty:
            consistent = False
            print("🚨 IC-Generic4 violation: There are edges without inbound")
            display(violations1_4)

        # IC-Generic5: Every edge has at least one outbound
        logger.info("Checking IC-Generic5")
        matches1_5 = self.get_outbounds().reset_index(level='nodes', drop=True).reset_index(drop=False)['edges']
        # Empty classes temtatively violate the constraint
        tentative_violations1_5 = df_difference(edges.reset_index(drop=False)['edges'], matches1_5)
        # Remove those violations that correspond to empty subclasses
        violations1_5 = df_difference(tentative_violations1_5, self.get_outbound_generalization_subclasses().reset_index(level='nodes', drop=False).merge(self.get_inbound_classes().reset_index(drop=False), on='nodes', how='inner')["edges"])
        if not violations1_5.empty:
            consistent = False
            print("🚨 IC-Generic5 violation: There are edges without outbound (a.k.a. attributes), and they are not specialized classes")
            display(violations1_5)

        # IC-Generic6: An edge cannot have more than one inbound
        logger.info("Checking IC-Generic6")
        violations1_6 = inbounds.groupby(inbounds.index.get_level_values('edges')).size()
        if not violations1_6[violations1_6 > 1].empty:
            consistent = False
            print("🚨 IC-Generic6 violation: There are edges with more than one inbound")
            display(violations1_6[violations1_6 > 1])

        # IC-Generic7: A hyperedge cannot be cyclic
        logger.info("Checking IC-Generic7")
        matches1_7 = pd.concat([self.get_sets(), self.get_structs()])
        violations1_7 = matches1_7[matches1_7.apply(lambda row: self.has_cycle(row["name"]), axis=1)]
        if not violations1_7.empty:
            consistent = False
            print("🚨 IC-Generic7 violation: There are cyclic hyperedges")
            display(violations1_7)

        # IC-Generic8: Unused

        # ------------------------------------------------------------------------------------------------- ICs on atoms
        custom_progress("    Checking constraints on the domain")

        # IC-Atoms2: Every ID belongs to one class which is outbound
        logger.info("Checking IC-Atoms2")
        matches2_2 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_2 = ids[~ids["name"].isin((matches2_2.reset_index(drop=False))["nodes"])]
        if not violations2_2.empty:
            consistent = False
            print("🚨 IC-Atoms2 violation: There are IDs without a class")
            display(violations2_2)

        # IC-Atoms3: Every attribute must belong at least one class which is outbound
        logger.info("Checking IC-Atoms3")
        matches2_3 = outbounds.join(classes, on='edges', rsuffix='_edges', how='inner')
        violations2_3 = attributes[~attributes["name"].isin((matches2_3.reset_index(drop=False))["nodes"])]
        if not violations2_3.empty:
            consistent = False
            print("🚨 IC-Atoms3 violation: There are attributes without a class")
            display(violations2_3)

        # IC-Atoms4: An attribute cannot belong to more than one class
        logger.info("Checking IC-Atoms4")
        matches2_4 = incidences.join(classes, on='edges', rsuffix='_edges', how='inner').join(attributes, on='nodes', rsuffix='_nodes', how='inner')
        violations2_4 = matches2_4.groupby(matches2_4.index.get_level_values('nodes')).size()
        if not violations2_4[violations2_4 > 1].empty:
            consistent = False
            print("🚨 IC-Atoms4 violation: There are attributes with more than one class")
            display(violations2_4[violations2_4 > 1])

        # IC-Atoms5_pre: Missing information provided to check consistency of cardinalities
        logger.info("Checking IC-Atoms5_pre")
        matches2_5_pre1 = outbounds.join(classes, on='edges', rsuffix='_class', how='inner')
        violations2_5_pre1 = matches2_5_pre1[matches2_5_pre1.apply(lambda r: r["misc_properties"]["DistinctVals"] is None, axis=1)]
        violations2_5_pre2 = classes[classes.apply(lambda r: r["misc_properties"]["Count"] is None, axis=1)]
        if not violations2_5_pre2.empty:
            warnings.warn(f"⚠️ IC-Atoms5_pre violation: Cardinalities are missing in classes {list(violations2_5_pre2.index)}")
        if not violations2_5_pre1.empty:
            warnings.warn(f"⚠️ IC-Atoms5_pre violation: Cardinalities are missing in attributes {list(violations2_5_pre1.index.get_level_values("nodes"))}")

        # IC-Atoms5: The number of different values of an attribute must be less or equal than the cardinality of its class
        logger.info("Checking IC-Atoms5")
        matches2_5 = outbounds.join(classes, on='edges', rsuffix='_class', how='inner')
        violations2_5 = matches2_5[matches2_5.apply(lambda r: r["misc_properties"]["DistinctVals"] is not None
                                                              and r["misc_properties_class"]["Count"] is not None
                                                              and r["misc_properties"]["DistinctVals"] > r["misc_properties_class"]["Count"], axis=1)]
        if not violations2_5.empty:
            consistent = False
            print("🚨 IC-Atoms5 violation: The number of different values of an attribute is greater than the cardinality of its class")
            display(violations2_5)

        # IC-Atoms6: Every association has one phantom
        logger.info("Checking IC-Atoms6")
        matches2_6 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
        violations2_6 = associations[~associations["name"].isin((matches2_6.reset_index(drop=False))["edges"])]
        if not violations2_6.empty:
            consistent = False
            print("🚨 IC-Atoms6 violation: There are associations without phantom")
            display(violations2_6)

        # IC-Atoms7: Every association has two ends (Definition 4)
        logger.info("Checking IC-Atoms7")
        matches2_7 = incidences.join(ids, on='nodes', rsuffix='_nodes', how='inner').join(associations, on='edges', rsuffix='_edges', how='inner').groupby(['edges']).size()
        violations2_7 = matches2_7[matches2_7 != 2]
        if not violations2_7.empty:
            consistent = False
            print("🚨 IC-Atoms7 violation: There are non-binary associations")
            display(violations2_7)

        # IC-Atoms8: The number of different values of an identifier must coincide with the cardinality of its class
        logger.info("Checking IC-Atoms8")
        matches2_8 = outbounds.join(classes, on='edges', rsuffix='_class', how='inner')
        violations2_8 = matches2_8[matches2_8.apply(lambda r: r["misc_properties"]["Identifier"] and r["misc_properties"]["DistinctVals"] != r["misc_properties_class"]["Count"], axis=1)]
        if not violations2_8.empty:
            consistent = False
            print("🚨 IC-Atoms5 violation: The number of different values of an identified must coincide with the cardinality of its class")
            display(violations2_8)

        # IC-Atoms9: One class cannot have more than one direct superclass
        logger.info("Checking IC-Atoms9")
        matches2_9 = self.get_outbound_generalization_subclasses().groupby(["nodes"]).size()
        violations2_9 = matches2_9[matches2_9 > 1]
        if not violations2_9.empty:
            consistent = False
            print("🚨 IC-Atoms9 violation: There are classes with more than one superclass")
            display(violations2_9)

        # IC-Atoms10: Every generalization outgoing of a subclass must have a discriminant
        logger.info("Checking IC-Atoms10")
        violations2_10 = self.get_outbound_generalization_subclasses()[~self.get_outbound_generalization_subclasses().apply(lambda r: "Constraint" in r["misc_properties"], axis=1)]
        if not violations2_10.empty:
            consistent = False
            print("🚨 IC-Atoms10 violation: There are generalization subclasses without discriminant constraint")
            display(violations2_10)

        # IC-Atoms11: Every generalization has disjointness and completeness constraints
        logger.info("Checking IC-Atoms11")
        matches2_11 = generalizations[generalizations.apply(lambda r: "Disjoint" in r["misc_properties"] and "Complete" in r["misc_properties"], axis=1)]
        violations2_11 = df_difference(generalizations["name"], matches2_11["name"])
        if not violations2_11.empty:
            consistent = False
            print("🚨 IC-Atoms11 violation: There are generalizations without completeness and disjointness constraints")
            display(violations2_11)

        # IC-Atoms12: Generalizations cannot have cycles
        logger.info("Checking IC-Atoms12")
        violations2_12 = classes[classes.apply(lambda r: r["name"] in self.get_superclasses_by_class_name(r["name"]), axis=1)]
        if not violations2_12.empty:
            consistent = False
            print("🚨 IC-Atoms12 violation: There are some cyclic generalizations")
            display(violations2_12)

        # IC-Atoms13: Every class has one ID or belongs to a generalization hierarchy
        logger.info("Checking IC-Atoms13")
        matches2_13 = outbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        possible_violations2_13 = classes[~classes["name"].isin((matches2_13.reset_index(drop=False))["edges"])]
        for class_name in possible_violations2_13.index:
            superclasses = self.get_superclasses_by_class_name(class_name)
            if not superclasses:
                consistent = False
                print(f"🚨 IC-Atoms13 violation: There is some class '{class_name}' without identifier (neither direct, nor inherited from a superclass)")

        # IC-Atoms14: Not two classes in a hierarchy can have ID
        logger.info("Checking IC-Atoms14")
        matches2_14 = outbounds.join(ids, on='nodes', rsuffix='_nodes', how='inner')
        possible_violations2_14 = classes[classes["name"].isin((matches2_14.reset_index(drop=False))["edges"])]
        for class_name in possible_violations2_14.index:
            superclasses = self.get_superclasses_by_class_name(class_name)
            identified_superclasses = [s for s in superclasses if s in possible_violations2_14.index]
            if identified_superclasses:
                consistent = False
                print(f"🚨 IC-Atoms14 violation: There is some class '{class_name}' with identifier in a generalization hierarchy with also identifiers '{identified_superclasses}'")

        # IC-Atoms15: The top of every hierarchy has an ID
        logger.info("Checking IC-Atoms15")
        matches2_15 = df_difference(self.get_outbound_generalization_superclasses().reset_index(drop=False)['nodes'], self.get_outbound_generalization_subclasses().reset_index(drop=False)['nodes'])
        for top_phantom in matches2_15:
            top_class = self.get_edge_by_phantom_name(top_phantom)
            if self.get_class_id_by_name(top_class) is None:
                consistent = False
                print(f"🚨 IC-Atoms15 violation: The class '{top_class}' in the top of a hierarchy should have an identifier")

        # IC-Atoms16: Every discriminant must be an attribute in one of the corresponding superclasses
        logger.info("Checking IC-Atoms16")
        matches2_16 = self.get_outbound_generalization_subclasses()[self.get_outbound_generalization_subclasses().apply(lambda r: "Constraint" in r["misc_properties"], axis=1)]
        for subclass in matches2_16.itertuples():
            superclass_names = self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(subclass.Index[1]))
            constraint = subclass.misc_properties.get('Constraint', None)
            assert constraint is not None, f"☠️ No constraint found for '{subclass}'"
            attribute_names = self.parse_predicate(constraint)
            for attribute_name in attribute_names:
                found = False
                for superclass_name in superclass_names:
                    found = found or self.H.get_cell_properties(superclass_name, attribute_name, "Kind") is not None
                if not found:
                    consistent = False
                    print(f"🚨 IC-Atoms16 violation: The attribute '{attribute_name}' used in the generalization constraint of '{subclass.Index[1]}', not found in any of its superclasses '{superclass_names}'")

        # IC-Atoms17: Every association end has name and multiplicities
        logger.info("Checking IC-Atoms17")
        matches2_17 = self.get_outbound_associations()["misc_properties"]
        for end_properties in matches2_17:
            if end_properties.get("End_name", None) is None:
                consistent = False
                print(f"🚨 IC-Atoms17 violation: Some association end does not have 'End_name' defined")
            else:
                if end_properties.get("MultiplicityMax", None) is None:
                    consistent = False
                    print(f"🚨 IC-Atoms17 violation: The association end '{end_properties.get('End_name')}' does not have 'MultiplicityMax' defined")
                if end_properties.get("MultiplicityMin", None) is None:
                    consistent = False
                    print(f"🚨 IC-Atoms17 violation: The association end '{end_properties.get('End_name')}' does not have 'MultiplicityMin' defined")
                # if end_properties.get("MultiplicityAvg", None) is None:
                #     consistent = False
                #     print(f"🚨 IC-Atoms17 violation: The association end '{end_properties.get("End_name")}' does not have MultiplicityAvg defined")

        # Not necessary to check from here on if the catalog only contains the atoms in the domain
        if design:
            # ---------------------------------------------------------------------------------------------- ICs on sets
            custom_progress("    Checking constraints on sets")

            # IC-Sets1: Every set has one phantom
            logger.info("Checking IC-Sets1")
            matches4_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations4_1 = sets[~sets["name"].isin((matches4_1.reset_index(drop=False))["edges"])]
            if not violations4_1.empty:
                consistent = False
                print("🚨 IC-Sets1 violation: There are sets without phantom")
                display(violations4_1)

            # IC-Sets2: Sets cannot be empty
            logger.info("Checking IC-Sets2")
            matches5_2 = self.get_outbound_sets().reset_index(drop=False).set_index("edges", drop=False)["edges"]
            violations5_2 = df_difference(sets["name"], matches5_2)
            if not violations5_2.empty:
                consistent = False
                print("🚨 IC-Sets2 violation: There are sets that are empty")
                display(violations5_2)

            # IC-Sets3: Sets cannot directly contain attributes
            logger.info("Checking IC-Sets3")
            violations4_3 = pd.merge(self.get_outbound_sets(), self.get_attributes(), on='nodes', suffixes=('_setOutbounds', '_attributes'),
                                     how='inner')
            if not violations4_3.empty:
                consistent = False
                print("🚨 IC-Sets3 violation: There are sets that contain attributes")
                display(violations4_3)

            # IC-Sets4: Sets cannot directly contain other sets
            logger.info("Checking IC-Sets4")
            violations4_4 = pd.merge(self.get_outbound_sets(), self.get_inbound_sets(), on='nodes', suffixes=('_setOutbounds', '_setInbounds'), how='inner')
            if not violations4_4.empty:
                consistent = False
                print("🚨 IC-Sets4 violation: There are sets that contain other sets")
                display(violations4_4)

            # IC-Sets5: Sets cannot directly contain associations
            logger.info("Checking IC-Sets5")
            violations4_5 = pd.merge(self.get_outbound_sets(), self.get_inbound_associations(), on='nodes', suffixes=('_setOutbounds', '_assocInbounds'), how='inner')
            if not violations4_5.empty:
                consistent = False
                print("🚨 IC-Sets5 violation: There are sets that contain associations")
                display(violations4_5)

            # IC-Sets6: Sets cannot directly contain generalizations
            logger.info("Checking IC-Sets6")
            violations4_6 = pd.merge(self.get_outbound_sets(), self.get_inbound_generalizations(), on='nodes', suffixes=('_setOutbounds', '_generInbounds'), how='inner')
            if not violations4_6.empty:
                consistent = False
                print("🚨 IC-Sets6 violation: There are sets that contain generalizations")
                display(violations4_6)

            # IC-Sets7: A set that contains a class, cannot contain anything else
            logger.info("Checking IC-Sets7")
            sets_with_attributes = self.get_outbound_sets().reset_index(drop=False).merge(self.get_inbound_classes(), left_on='nodes', right_on='nodes', suffixes=('_sets', '_attributes'), how='inner')
            matches4_7 = self.get_outbound_sets()[self.get_outbound_sets().index.get_level_values('edges').isin(sets_with_attributes['edges'])].groupby('edges').size()
            violations4_7 = matches4_7[matches4_7 > 1]
            if not violations4_7.empty:
                consistent = False
                print("🚨 IC-Sets5 violation: There are sets that contain a class, besides other elements")
                display(violations4_7)

            # ------------------------------------------------------------------------------------------- ICs on structs
            custom_progress("    Checking constraints on structs")

            # IC-Structs1: Every struct has one phantom
            logger.info("Checking IC-Structs1")
            matches3_1 = inbounds.join(phantoms, on='nodes', rsuffix='_nodes', how='inner')
            violations3_1 = structs[~structs["name"].isin((matches3_1.reset_index(drop=False))["edges"])]
            if not violations3_1.empty:
                consistent = False
                print("🚨 IC-Structs1 violation: There are structs without phantom")
                display(violations3_1)

            # IC-Structs2: Every struct must be inside another struct or set
            logger.info("Checking IC-Structs2")
            matches3_2 = pd.concat([self.get_outbound_sets(), self.get_outbound_structs()]).reset_index("edges", drop=True).drop('misc_properties', axis=1)
            violations3_2 = df_difference(self.get_phantom_structs().drop(['misc_properties', 'name'], axis=1), matches3_2)
            if not violations3_2.empty:
                consistent = False
                print("🚨 IC-Structs2 violation: There are structs that do not belong to any other struct or set")
                display(violations3_2)

            # IC-Structs3: Every struct has at least one anchor
            logger.info("Checking IC-Structs3")
            matches3_3 = outbounds[outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Anchor', False))].groupby('edges').size()
            violations3_3 = structs[~structs["name"].isin((matches3_3[matches3_3 > 0].reset_index(drop=False))["edges"])]
            if not violations3_3.empty:
                consistent = False
                print("🚨 IC-Structs3 violation: There are structs without exactly one anchor")
                display(violations3_3)

            # IC-Structs4: Anchors can be either classes or associations
            logger.info("Checking IC-Structs3")
            matches3_4 = outbounds[outbounds["misc_properties"].apply(lambda x: x['Kind'] == 'StructIncidence' and x.get('Anchor', False))].reset_index(drop=False)['nodes']
            violations3_4 = df_difference(matches3_4, pd.concat([self.get_phantom_classes(), self.get_phantom_associations()])["name"])
            if not violations3_4.empty:
                consistent = False
                print("🚨 IC-Structs4 violation: There are structs with an anchor which is neither class nor association")
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
                    consistent = False
                    print(f"🚨 IC-Structs-5 violation: The anchor of struct '{struct}' is not connected")

            # IC-Structs6: Elements in a struct can not contain two classes (directly or transitively) related by generalization
            #              This is just because of ambiguity generated by attributes. It could be solved using aliases
            logger.info("Checking IC-Structs6")
            inbound_classes = self.get_inbound_classes()
            inbound_classes["classname"] = inbound_classes.index.get_level_values("edges")
            struct_outbound_classes = pd.merge(structOutbounds, inbound_classes, on="nodes", how="inner")
            for elem in struct_outbound_classes["classname"]:
                for superclass in self.get_superclasses_by_class_name(elem):
                    if superclass in struct_outbound_classes["classname"]:
                        consistent = False
                        print(f"🚨 IC-Structs-6 violation: Both '{elem}' and its superclass '{superclass}' cannot belong to the same struct")

            # IC-Structs7: Loose association ends in the anchor must still be loose ends in the whole struct
            logger.info("Checking IC-Structs7")
            for struct in structs.index:
                loose_ends = self.get_loose_association_end_names_by_struct_name(struct)
                for anchor_end_name in self.get_anchor_end_names_by_struct_name(struct):
                    if not self.is_class_phantom(anchor_end_name) and anchor_end_name not in loose_ends:
                        consistent = False
                        print(f"🚨 IC-Structs-7 violation: There is an anchor point '{anchor_end_name}' in '{struct}', which is a loose end (i.e., it has not the class in the anchor, but only in its elements)")

            # IC-Structs8: A struct containing siblings by some generalization must also contain the discriminant attribute
            logger.info("Checking IC-Structs8")
            for struct_name in self.get_structs().index:
                discriminants = []
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                restricted_classes = restricted_struct.get_classes()
                # Foll all classes in the current struct
                for class_name1 in restricted_classes.index.get_level_values("edges"):
                    superclasses1 = restricted_struct.get_superclasses_by_class_name(class_name1)
                    # If it has superclasses
                    if superclasses1:
                        # Check all other classes in the struct
                        for class_name2 in restricted_classes.index.get_level_values("edges"):
                            # Get their superclasses
                            superclasses2 = restricted_struct.get_superclasses_by_class_name(class_name2)
                            # Check this is not actually itself or an ancestor
                            if class_name1 != class_name2 and class_name2 not in superclasses1 and class_name1 not in superclasses2:
                                # Check if they are siblings
                                if [s for s in superclasses1 if s in superclasses2]:
                                    # Check if the corresponding discriminant attribute is present(this works because we have single inheritance)
                                    discriminants.append(
                                        restricted_struct.get_outbound_generalization_subclasses().reset_index(
                                            level="edges", drop=True).loc[
                                            self.get_phantom_of_edge_by_name(class_name1)].misc_properties["Constraint"])
                attribute_names = drop_duplicates(self.parse_predicate(" AND ".join(discriminants)))
                for attr in attribute_names:
                    kind = self.H.get_cell_properties(struct_name, attr, "Kind")
                    if kind is None:
                        consistent = False
                        print(f"🚨 IC-Structs8 violation: The struct '{struct_name}' should have attribute '{attr}' to be used as a discriminant in a generalization")

            # IC-Structs-b: All attributes in a struct are connected to its anchor by a unique path of associations, which are all part of the struct, too (Definition 7-b)
            logger.info("Checking IC-Structs-b")
            for struct_name in structs.index:
                attribute_names = self.get_attribute_names_by_struct_name(struct_name)
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                # Check if the restricted struct is connected
                if not restricted_struct.H.is_connected(s=1):
                    consistent = False
                    print(f"🚨 IC-Structs-b violation: The struct '{struct_name}' is not connected")
                    restricted_struct.show_textual()
                anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                bipartite = restricted_struct.H.remove_edges(self.get_anchor_associations_by_struct_name(struct_name)).bipartite()
                for attr in attribute_names:
                    paths = []
                    for anchor in anchor_points:
                        paths += list(nx.all_simple_paths(bipartite, source=anchor, target=attr))
                    if len(paths) > 1:
                        consistent = False
                        print(f"🚨 IC-Structs-b violation: The struct '{struct_name}' has multiple paths '{paths}', which generates ambiguity in the meaning of some attribute")

            # IC-Structs-c: All anchors of structs inside a struct are connected to its anchor by a unique path of associations, which are all part of the struct, too (Definition 7-c)
            #               Also need to check that max multiplicity is one (otherwise, it should be a set)
            logger.info("Checking IC-Structs-c")
            for external_struct_name in self.get_structs().index:
                for elem_name in self.get_outbound_struct_by_name(external_struct_name).index.get_level_values("nodes"):
                    if self.is_phantom(elem_name):
                        edge_name = self.get_edge_by_phantom_name(elem_name)
                        if self.is_struct(edge_name):
                            internal_struct_name = edge_name
                            restricted_struct = self.get_restricted_struct_hypergraph(external_struct_name)
                            bipartite = restricted_struct.H.bipartite()
                            for internal_anchor in self.get_anchor_points_by_struct_name(internal_struct_name):
                                found = False
                                for external_anchor in self.get_anchor_points_by_struct_name(external_struct_name):
                                    paths = list(nx.all_simple_paths(bipartite, source=external_anchor, target=internal_anchor))
                                    if len(paths) > 0:
                                        found = True
                                        if len(paths) > 1:
                                            print(f"🚨 IC-Structs-c violation: The anchor point '{internal_anchor}' of struct '{internal_struct_name}' is connected to '{external_anchor}' in its parent struct '{external_struct_name}' by more than one path: '{paths}'")
                                        if not self.check_multiplicities_to_one(paths[0])[1]:
                                            print(f"🚨 IC-Structs-c violation: The anchor point '{internal_anchor}' of struct '{internal_struct_name}' is connected to '{external_anchor}' in its parent struct '{external_struct_name}' by path '{paths[0]}' with max multiplicity greater than one")
                                if not found:
                                    consistent = False
                                    print(f"🚨 IC-Structs-c violation: The anchor point '{internal_anchor}' of struct '{internal_struct_name}' is not connected to any anchor point of its parent struct '{external_struct_name}'")

            # IC-Structs-d: All sets inside a struct must contain a unique path of associations connecting the parent struct to either the class or anchor of the struct inside the set (Definition 7-d)
            #               Actually, this just check that the parent struct has an association to either the class or every element in the anchor
            logger.info("Checking IC-Structs-d")
            sets_within_struct = self.get_outbound_structs().reset_index(drop=False).merge(self.get_inbound_sets(), left_on='nodes', right_on='nodes', suffixes=('_struct', '_set'), how='inner')
            for set_struct in sets_within_struct.itertuples():
                external_struct_name = set_struct.edges
                # The content of a set can be either one single class, or several structs
                # In the case of several structs, all must share the same anchor, so anyway, taking the fist element is enough
                internal_elem_name = self.get_outbound_set_by_name(self.get_edge_by_phantom_name(set_struct.nodes)).index[0][1]
                restricted_struct = self.get_restricted_struct_hypergraph(external_struct_name)
                if self.is_class_phantom(internal_elem_name):
                    # By IC-Sets7 a set can have at most one class
                    # It may be that the association is actually inherited from a superclass
                    superclass_phantoms = [self.get_phantom_of_edge_by_name(s) for s in self.get_superclasses_by_class_name(self.get_edge_by_phantom_name(internal_elem_name))]
                    superclass_phantoms.append(internal_elem_name)
                    if all([p not in restricted_struct.get_association_ends()["nodes"].values for p in superclass_phantoms]):
                        consistent = False
                        print(f"🚨 IC-Structs-d violation: Class '{internal_elem_name}' included in set '{set_struct.nodes}' is not connected to struct '{external_struct_name}', which contains said set")
                else:
                    assert self.is_struct_phantom(internal_elem_name), f"☠️ The element '{internal_elem_name}' inside set '{set_struct.nodes}', which is not a class, should be a struct, but it is not"
                    for anchor_point in self.get_anchor_points_by_struct_name(internal_elem_name):
                        if self.get_phantom_of_edge_by_name(anchor_point) not in restricted_struct.get_nodes().index:
                            consistent = False
                            print(f"🚨 IC-Structs-d violation: Anchor point '{anchor_point}' of struct '{internal_elem_name}' and included in set '{set_struct.nodes}' is not connected to struct '{external_struct_name}', which contains said set")

            # IC-Structs-e: All associations inside a struct connect either a class or another struct (Definition 7-e)
            #               This needs to be relaxed to simply structs being connected
            logger.info("Checking IC-Structs-e (relaxed)")
            for struct_name in self.get_structs().index:
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                # Check if the restricted struct is connected
                if not restricted_struct.H.is_connected(s=1):
                    consistent = False
                    print(f"🚨 IC-Structs-e violation: The struct '{struct_name}' is not connected")
                    restricted_struct.show_textual()

            # ----------------------------------------------------------------------------------------- ICs about design
            custom_progress("    Checking generic design constraints")

            # IC-Design1: All the first levels must be sets
            logger.info("Checking IC-Design1")
            matches5_1 = self.get_inbound_firstLevel()
            violations5_1 = matches5_1[~matches5_1["misc_properties"].apply(lambda x: x['Kind'] == 'SetIncidence')]
            if not violations5_1.empty:
                consistent = False
                print("🚨 IC-Design1 violation: All first levels must be sets")
                display(violations5_1)

            # IC-Design2: All the attributes and associations in the domain are connected to the first level
            #             Classes are excluded from the check because of generalization
            logger.info("Checking IC-Design2")
            matches5_2 = []
            for set_name in self.get_inbound_firstLevel().index.get_level_values("edges"):
                matches5_2.extend(self.get_atoms_including_transitivity_by_edge_name(set_name))
            atoms5_2 = pd.concat([self.get_attributes(), self.get_phantom_associations()])
            violations5_2 = atoms5_2[~atoms5_2.index.isin(matches5_2)]
            if not violations5_2.empty:
                consistent = False
                print("🚨 IC-Design2 violation: Atoms disconnected from the first level")
                display(violations5_2)

            # IC-Design3: All domain elements must appear in some struct or set
            #             This is relaxed into just a warning, because of generalizations
            logger.info("Checking IC-Design3 (produces just warnings)")
            atoms = pd.concat([self.get_inbound_classes().reset_index(drop=False)["nodes"], self.get_inbound_associations().reset_index(drop=False)["nodes"], attributes.reset_index(drop=False)["nodes"]])
            violations5_3 = atoms[~atoms.isin(pd.concat([structOutbounds, setOutbounds]).index.get_level_values("nodes"))]
            if not violations5_3.empty:
                # consistent = False
                warnings.warn("⚠️ IC-Design3 violation: Some atoms do not belong to any struct or set")
                if config.show_warnings:
                    display(violations5_3)

            # IC-Design4: All structs in a set must have the same attributes in the anchor
            # IC-Design5: For all structs in a set, there must be a difference in a class in the anchor, which are related by generalization
            # IC-Design6: If there are different structs in a set, and two of them differ in some sibling class in the anchor, the discriminant attribute must be provided in the struct
            #             Actually, IC-Design6 checks if the discriminant attributes are in the set, but it should check only the corresponding struct
            #             All three are checked at the same time to be more precise in the message and efficient
            logger.info("Checking IC-Design4")
            logger.info("Checking IC-Design5")
            logger.info("Checking IC-Design6")
            for set_name in sets.index:
                anchor_concepts = []
                anchor_attributes = []
                set_attributes = []
                struct_phantom_list = pd.merge(self.get_outbound_set_by_name(set_name), self.get_phantom_structs(), on="nodes", how="inner").index
                for struct_phantom in struct_phantom_list:
                    struct_name = self.get_edge_by_phantom_name(struct_phantom)
                    set_attributes.extend(self.get_attribute_names_by_struct_name(struct_name))
                    attribute_list = []
                    concept_list = []
                    for key in self.get_anchor_end_names_by_struct_name(struct_name):
                        concept_list.append(key)
                        if self.is_class_phantom(key):
                            attribute_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                        # If it is not a class, it is a loose end
                        else:
                            attribute_list.append(key)
                    concept_list.sort()
                    attribute_list.sort()
                    anchor_concepts.append(concept_list)
                    anchor_attributes.append(attribute_list)
                set_attributes = drop_duplicates(set_attributes)
                # Check IC-Design4
                if len(drop_duplicates(anchor_attributes)) > 1:
                    consistent = False
                    print(f"🚨 IC-Design4 violation: Anchor attributes of structs in set '{set_name}' do not coincide: '{anchor_attributes}'")
                # Check IC-Design5
                # Not really necessary to check if they are generalization, because attributes already coincide
                elif len(drop_duplicates(anchor_concepts)) != len(struct_phantom_list):
                    consistent = False
                    print(f"🚨 IC-Design5 violation: Anchor concepts (aka classes) of structs in set '{set_name}' do exactly coincide and should not: '{anchor_concepts}'")
                # Check IC-Design6
                else:
                    # For every pair of structs in the set
                    for i in range(len(anchor_concepts)):
                        for j in range(i+1, len(anchor_concepts)):
                            if anchor_concepts[i] != anchor_concepts[j]:
                                a, b = i, j
                                for _ in range(2):
                                    # Find the different concept in the anchor (they must be in the same generalization hierarchy by ID-Design4)
                                    for phantom_name in anchor_concepts[a]:
                                        if phantom_name not in anchor_concepts[b]:
                                            class_name = self.get_edge_by_phantom_name(phantom_name)
                                            # Check if the class to be discriminated is not the top of the hierarchy
                                            if self.get_superclasses_by_class_name(class_name):
                                                # Now we need to check if the corresponding discriminant is in the table (actually, we should check in the same struct)
                                                discriminant = self.get_outbound_generalization_subclasses().reset_index(level="edges", drop=True).loc[phantom_name].misc_properties.get("Constraint", None)
                                                assert discriminant is not None, f"☠️ No discriminant for '{class_name}'"
                                                attribute_names = self.parse_predicate(discriminant)
                                                found = True
                                                for attribute_name in attribute_names:
                                                    # This is just checking if the attribute is in the table, but actually it should check if it is in the current struct
                                                    found = found and attribute_name in set_attributes
                                                if not found:
                                                    consistent = False
                                                    print(f"🚨 IC-Design6 violation: Some discriminant attribute missing in set '{set_name}' required for '{class_name}'")
                                    # Now we need to do the comparison the other way round
                                    a, b = j, i

            # IC-Design7: Any struct with a class with subclasses must contain the corresponding discriminants
            #             It is implemented as a warning, because it could be acceptable as soon as the class is not used in the queries
            logger.info("Checking IC-Design7 (produces just warnings)")
            for struct_name in self.get_structs().index:
                # Get all class names in the current struct
                class_names = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("nodes").isin(pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_inbound_classes(), on="nodes", how="inner").index)].index.get_level_values("edges")
                attribute_names = self.get_attribute_names_by_struct_name(struct_name)
                for class_name in class_names:
                    for subclass_name in self.get_subclasses_by_class_name(class_name):
                        discriminant = self.get_discriminant_by_class_name(subclass_name)
                        assert discriminant is not None, f"☠️ No discriminant for '{class_name}'"
                        if any(attr not in attribute_names for attr in self.parse_predicate(discriminant)):
                            # consistent = False
                            warnings.warn(f"⚠️ IC-Design7 violation: Some discriminant attribute missing in struct '{struct_name}' for '{subclass_name}' subclass of '{class_name}' (it is fine as soon as queries do not use this class)")

            # IC-Design8: All classes must appear linked to at least one anchor with min multiplicity one.
            #             Such anchor must have min multiplicity one internally, to guarantee that it does not miss any instance.
            #             This is relaxed to be just a warning, as above, just because of generalizations.
            logger.info("Checking IC-Design8 (produces just warnings)")
            for class_name in self.get_classes().index:
                class_phantom = self.get_phantom_of_edge_by_name(class_name)
                found = False
                for struct_name in self.get_structs().index:
                    # Check if the class is in this struct
                    if class_phantom in self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes"):
                        dont_cross = self.get_anchor_associations_by_struct_name(struct_name)
                        restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                        bipartite = restricted_struct.H.remove_edges(dont_cross).bipartite()
                        anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                        for anchor_point in anchor_points:
                            if self.is_class_phantom(anchor_point):
                                paths = list(nx.all_simple_paths(bipartite, source=class_phantom, target=anchor_point))
                                # There can be more than one path from a class to the first level, as soon as it goes through different structs, but this is not relevant here
                                for path in paths:
                                    # First position in the tuple is the min multiplicity
                                    found = self.check_multiplicities_to_one(path)[0]
                                    if found:
                                        # Check that the internal multiplicity of the anchor point in the anchor is also min to one with all other anchor points
                                        # This means all dont_cross have min multiplicity one
                                        restricted_anchor_struct = self.get_restricted_struct_hypergraph(struct_name, only_anchor=True)
                                        bipartite_anchor = restricted_anchor_struct.H.bipartite()
                                        for anchor_point2 in anchor_points:
                                            anchor_paths = list(nx.all_simple_paths(bipartite_anchor, source=anchor_point, target=anchor_point2))
                                            assert len(anchor_paths) > 0, f"☠️ No path found in the anchor of struct '{struct_name}' between points '{anchor_point}' and '{anchor_point2}'"
                                            assert len(anchor_paths) < 2, f"☠️ Multiple paths '{anchor_paths}' found in the anchor of struct '{struct_name}' between points '{anchor_point}' and '{anchor_point2}'"
                                            found = found and self.check_multiplicities_to_one(anchor_paths[0])[0]
                                        # If the problem is in the anchor, we do not need to continue checking paths anyway (any other path to the same anchor point will have the same problem)
                                        break
                                if found: break
                        if found: break
                if not found:
                    # consistent = False
                    warnings.warn(f"⚠️ IC-Design8 violation: Instances of class '{class_name}' may be lost, because it is not linked to any set at the first level with associations of minimum multiplicity one")

            # IC-Design9: All attributes in the structs in a set must have the same paths
            #             In already considers nested structs and sets, because get_struct_attributes already does
            logger.info("Checking IC-Design9")
            for set_name in self.get_sets().index:
                inner_structs_phantom_names = self.get_outbound_set_by_name(set_name).index.get_level_values("nodes")
                if len(inner_structs_phantom_names) > 1:
                    attribute_paths = []
                    for phantom_name in inner_structs_phantom_names:
                        attribute_paths.append(self.get_struct_attributes(self.get_edge_by_phantom_name(phantom_name)))
                    for i in range(len(attribute_paths)):
                        for j in range(i+1, len(attribute_paths)):
                            for pair_i in attribute_paths[i]:
                                for pair_j in attribute_paths[j]:
                                    if pair_i[0] == pair_j[0] and pair_i[1] != pair_j[1]:
                                        consistent = False
                                        print(f"🚨 IC-Design9 violation: Attribute '{pair_i[0]}' has a different path depending on the struct inside '{set_name}': {pair_i[1]} vs {pair_j[1]}")

        return consistent

    def check_basic_request_structure(self, pattern_edges: list[str], required_attributes: list[str]) -> None:
        """
        Checks if the pattern is connected and contains all the required attributes.
        :param pattern_edges:
        :param required_attributes:
        """
        # Check if the hypergraph contains all the pattern hyperedges
        non_existing_associations = df_difference(pd.DataFrame(pattern_edges), pd.concat([self.get_classes(), self.get_associations()])["name"].reset_index(drop=True))
        if not non_existing_associations.empty:
            raise ValueError(f"🚨 Some class or association in the pattern does not belong to the catalog: {non_existing_associations.values.tolist()[0]}")

        superclasses = []
        for e in pattern_edges:
            if self.is_class(e):
                superclasses.extend(self.get_superclasses_by_class_name(e))
                superclasses.extend(self.get_generalizations_by_class_name(e))
        restricted_domain = self.H.restrict_to_edges(pattern_edges+superclasses)
        # Check if the restricted domain is connected
        if not restricted_domain.is_connected(s=1):
            raise ValueError(f"🚨 Some pattern elements (i.e., classes and associations) are not connected")

        # Check if the restricted domain contains all the required attributes and association ends
        attributes = pd.merge(restricted_domain.nodes.dataframe, self.get_attributes(), left_index=True, right_index=True, how="inner")["name"]
        hop1 = pd.merge(restricted_domain.nodes.dataframe, self.get_inbound_associations().reset_index(drop=False), left_index=True, right_on="nodes", suffixes=('_associationPhantoms', '_inbounds'), how="inner")
        hop2 = pd.merge(hop1, self.get_outbound_associations().reset_index(drop=False), left_on="edges", right_on="edges", suffixes=('_inbounds', '_outbounds'), how="inner")
        association_ends = hop2.apply(lambda row: row["misc_properties"]["End_name"], axis=1)
        association_ends.name = "name"
        if attributes.empty:
            missing_attributes = df_difference(pd.DataFrame(required_attributes), association_ends)
        elif association_ends.empty:
            missing_attributes = df_difference(pd.DataFrame(required_attributes), attributes)
        else:
            missing_attributes = df_difference(pd.DataFrame(required_attributes), pd.concat([attributes, association_ends], axis=0))
        if not missing_attributes.empty:
            raise ValueError(f"🚨 Some attributes {missing_attributes.values.tolist()} in the request are not covered by the elements in the pattern {pattern_edges}")

    def check_query_structure(self, project_attributes, filter_attributes, pattern_edges, required_attributes) -> None:
        # Check if the hypergraph contains all the projected attributes
        non_existing_attributes = df_difference(pd.DataFrame(project_attributes), pd.concat([self.get_ids(), self.get_attributes(), self.get_association_ends()])["name"].reset_index(drop=True))
        if not non_existing_attributes.empty:
            raise ValueError(f"🚨 Some attribute in the projection does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        # Check if the hypergraph contains all the filter attributes
        non_existing_attributes = df_difference(pd.DataFrame(filter_attributes), pd.concat([self.get_ids(), self.get_attributes(), self.get_association_ends()])["name"].reset_index(drop=True))
        if not non_existing_attributes.empty:
            raise ValueError(f"🚨 Some attribute in the filter does not belong to the catalog: {non_existing_attributes.values.tolist()[0]}")

        self.check_basic_request_structure(pattern_edges, required_attributes)

    def parse_predicate(self, predicate) -> list[str]:
        attributes = []
        where_clause = "WHERE "+predicate
        where_parsed = sqlparse.parse(where_clause)[0].tokens[0]

        # This extracts the attribute names
        # TODO: Parenthesis are not considered by now. It will require some kind of recursion
        for atom in where_parsed.tokens:
            if atom.ttype is None:  # This is a clause in the predicate
                for token in atom.tokens:
                    if token.ttype is None:  # This is an attribute in the predicate
                        if not self.is_attribute(token.value):
                            raise ValueError(f"🚨 '{token.value}' (in a filter or constraint) is not an attribute")
                        attributes.append(token.value)
        return attributes

    def parse_query(self, query) -> tuple[list[str], list[str], list[str], list[str], str]:
        pattern_edges = query.get("pattern", [])
        if not pattern_edges:
            raise ValueError("🚨 Empty pattern is not allowed in the query")
        # Get the query and parse it
        requested_project_attributes = query.get("project", [])
        if not requested_project_attributes:
            raise ValueError("🚨 Empty projection is not allowed in a query")
        project_attributes = []
        for requested in requested_project_attributes:
            if self.is_attribute(requested) or self.is_association_end(requested):
                project_attributes.append(requested)
            elif requested == '*':
                for edge in pattern_edges:
                    if self.is_class(edge):
                        # TODO: This needs to include the attributes of the superclasses
                        project_attributes.extend(self.get_outbound_class_by_name(edge).index.get_level_values('nodes'))
                        # for attr in self.get_outbound_class_by_name(edge).itertuples():
                        #     project_attributes.append(attr.Index[1])
            elif len(requested) > 2 and requested[-1] == '*' and self.is_class(requested[:-2]):
                project_attributes.extend(self.get_outbound_class_by_name(requested[:-2]).index.get_level_values('nodes'))
                # for attr in self.get_outbound_class_by_name(requested[:-2]).itertuples():
                #     project_attributes.append(attr.Index[1])
            else:
                raise ValueError(f"🚨 Projected '{requested}' is neither an attribute, nor an association end, nor an accepted wildcard")
        identifiers = []
        for e in pattern_edges:
            if not (self.is_class(e) or self.is_association(e)):
                raise ValueError(f"🚨 Chosen edge '{e}' is neither a class nor an association")
            if self.is_class(e):
                identifiers.append(self.get_class_id_by_name(e))
        filter_clause = query.get("filter", "TRUE")
        if filter_clause == "":
            filter_clause = "TRUE"
        filter_attributes = drop_duplicates(self.parse_predicate(filter_clause))
        # Identifiers of all classes are added to guarantee that a table containing the class is used in the query
        required_attributes = list(set(project_attributes + filter_attributes + identifiers))

        self.check_query_structure(project_attributes, filter_attributes, pattern_edges, required_attributes)
        return project_attributes, filter_attributes, pattern_edges, required_attributes, filter_clause

    def parse_insert(self, insert) -> tuple[dict[str, str], list[str]]:
        # Get the query and parse it
        data = insert.get("data", {})
        if not data:
            raise ValueError("🚨 Empty data is not allowed in an insertion")
        for a in data.keys():
            if not (self.is_attribute(a) or self.is_association_end(a)):
                raise ValueError(f"🚨 Projected '{a}' is neither an attribute nor an association end")
        pattern_edges = insert.get("pattern", [])
        if not pattern_edges:
            raise ValueError("🚨 Empty pattern is not allowed in the insertion")

        self.check_basic_request_structure(pattern_edges, data.keys())
        return data, pattern_edges

    def create_bucket_combinations(self, pattern, required_attributes) -> tuple[list[list[str]], list[str], list[str]]:
        """
        For each required domain element in the pattern, create a bucket with all the tables where it can come from.
        Then, combine all these buckets to cover all elements.
        :param pattern: List of classes and associations in the query.
        :param required_attributes: List of attributes used in the query.
        :return: List of combinations of tables covering all the required elements.
        :return: List of classes required.
        :return: List of associations required.
        """
        buckets = []
        classes = []
        associations = []
        for elem in pattern:
            # Find the sets at fist level where the element belongs
            hierarchy = [elem]+self.get_superclasses_by_class_name(elem)
            first_levels = drop_duplicates(self.get_transitive_firstLevels(hierarchy))
            # Sorting the list of tables is important to drop duplicates later
            first_levels.sort()
            # Split join edges into classes and associations
            if self.is_association(elem):
                associations.append(elem)
                # If the element is an association, any table containing it is an option
                buckets.append(first_levels)
            if self.is_class(elem):
                classes.append(elem)
                current_attributes = []
                # Take the required attributes in the class that are in the current table
                for class_name in hierarchy:
                    current_attributes.extend(self.get_outbound_class_by_name(class_name)[self.get_outbound_class_by_name(class_name).index.get_level_values('nodes').isin(required_attributes)].index.get_level_values('nodes').to_list())
                # If it is a class, the id always belongs to the table, hence we add it even if not required
                if self.get_class_id_by_name(elem) not in current_attributes:
                    current_attributes.append(self.get_class_id_by_name(elem))
                # If it is a class, it may be vertically partitioned
                # We need to generate joins of these tables that cover all required attributes one by one
                # Get the tables independently for every attribute in the class
                #    First, we precompute the attributes of all sets (which is expensive) to save time
                attrs_of_set = {}
                for set_name in first_levels:
                    attrs_of_set[set_name] = self.get_atoms_including_transitivity_by_edge_name(set_name)
                for attr in current_attributes:
                    if not self.is_id(attr) or len(current_attributes) == 1:
                        firstlevels_with_attr = []
                        for set_name in first_levels:
                            if attr in attrs_of_set[set_name]:
                                firstlevels_with_attr.append(set_name)
                        if firstlevels_with_attr:
                            buckets.append(firstlevels_with_attr)
        # Generate combinations of the buckets of each element to get the minimal combinations of tables that cover all of them
        # TODO: Actually, it is not guaranteed that all of them are covered. It should be checked
        return combine_buckets(drop_duplicates(buckets)), classes, associations

    def get_aliases(self, sets_combination) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
        """
        This method generates correspondences of aliases of tables and renamings of attributes in a query.
        :param sets_combination: The set of tables in the FROM clause of a query.
        :return: Dictionary of aliases of tables.
        :return: Dictionary of projections of domain attributes.
        :return: Dictionary of joins of domain attributes.
        :return: Dictionary of table locations of domain attributes.
        """
        alias_set = {}
        proj_attr = {}
        join_attr = {}
        location_attr = {}
        # The list of tables is reversed, so that the first appearance of an attribute prevails (seems more logical)
        for index, set_name in enumerate(reversed(sets_combination)):
            # Determine the aliases of tables and required attributes
            alias_set[set_name] = self.config.prepend_table_alias + str(len(sets_combination) - index)
            for struct_name in self.get_struct_names_inside_set_name(set_name):
                custom_progress(f"--------Processing {struct_name}")
                for dom_attr_name, attr_path in tqdm(self.get_struct_attributes(struct_name), desc=f"----------Attributes in {struct_name}", leave=config.show_progress):
                    # In case of generalization, the attribute may be overwritten, but they should coincide
                    # It is fine that two classes appear in a struct, as soon as they are queried based on the corresponding association end
                    assert dom_attr_name not in location_attr or location_attr[dom_attr_name] != alias_set[set_name] or self.generate_attr_projection_clause(attr_path) == proj_attr[dom_attr_name], f"☠️ Attribute '{dom_attr_name}' ambiguous in struct '{struct_name}': '{proj_attr[dom_attr_name]}' and '{self.generate_attr_projection_clause(attr_path)}' (it should not be used in the query)"
                    location_attr[dom_attr_name] = alias_set[set_name]
                    proj_attr[dom_attr_name] = self.generate_attr_projection_clause(attr_path)
                    join_attr[dom_attr_name + "@" + set_name] = self.generate_attr_projection_clause(attr_path)
                custom_progress(f"----------Processing its association ends")
                # From here on in the loop is necessary to translate queries based on association ends, when the design actually stores the class ID
                atoms = self.get_atoms_including_transitivity_by_edge_name(struct_name)
                associations = self.get_inbound_associations()[self.get_inbound_associations().index.get_level_values("nodes").isin(atoms)]
                classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("nodes").isin(atoms)]
                association_ends = self.get_outbound_associations()[
                    (self.get_outbound_associations().index.get_level_values("edges").isin(
                        associations.index.get_level_values("edges"))) & (
                        self.get_outbound_associations().index.get_level_values("nodes").isin(
                            classes.index.get_level_values("nodes")))]
                # Set the location of all association ends that have a class in the struct (i.e., non-loose ends)
                for end in association_ends.itertuples():
                    location_attr[end.misc_properties["End_name"]] = alias_set[set_name]
                    dom_attr_name = self.get_class_id_by_name(self.get_edge_by_phantom_name(end.Index[1]))
                    assert dom_attr_name in proj_attr and dom_attr_name + "@" + set_name in join_attr, f"☠️ Attribute '{dom_attr_name}' does not exist in '{struct_name}'"
                    proj_attr[end.misc_properties["End_name"]] = proj_attr[dom_attr_name]
                    join_attr[end.misc_properties["End_name"] + "@" + set_name] = join_attr[dom_attr_name + "@" + set_name]
        return alias_set, proj_attr, join_attr, location_attr

    def get_discriminants(self, sets_combination, pattern_class_names) -> list[str]:
        """
        Based on the existence of superclasses, this method finds the corresponding discriminants.
        :param sets_combination: The set of firstlevel element that is to be accessed by a query.
        :param pattern_class_names: The set of classes in the pattern of the query.
        :return: List of discriminants necessary in the query.
        """
        # TODO: Consider what happens with nested structs, when the same discriminant can come from more than one substruct
        discriminants = []
        # For every class in the pattern
        for pattern_class_name in pattern_class_names:
            pattern_superclasses = self.get_superclasses_by_class_name(pattern_class_name)
            if pattern_superclasses:
                # For every first level set required in the query
                for set_name in sets_combination:
                    for struct_name in self.get_struct_names_inside_set_name(set_name):
                        # Get all classes in the current struct of the current table
                        table_classes = self.get_inbound_classes()[self.get_inbound_classes().index.get_level_values("nodes").isin(pd.merge(self.get_outbound_struct_by_name(struct_name), self.get_inbound_classes(), on="nodes", how="inner").index)]
                        # For all classes in the table
                        for table_class_name in table_classes.index.get_level_values("edges"):
                            # Check if they are siblings
                            if table_class_name in pattern_superclasses:
                                discriminant = self.get_discriminant_by_class_name(pattern_class_name)
                                assert discriminant is not None, f"☠️ No discriminant for '{pattern_class_name}'"
                                found = True
                                for attribute_name in self.parse_predicate(discriminant):
                                    found = found and attribute_name in self.get_attribute_names_by_struct_name(struct_name)
                                if not found:
                                    raise ValueError(f"🚨 Some discriminant attribute missing in struct '{struct_name}' of table '{set_name}' for '{pattern_class_name}' in the query (IC-Design7 should have warned about this)")
                                # Add the corresponding discriminant (this works because we have single inheritance)
                                discriminants.append(discriminant)
        # It should not be necessary to remove duplicates if design and query are sound (some extra check may be needed)
        # Right now, the same discriminant twice is useless, because attribute alias can come from only one table
        return drop_duplicates(discriminants)

    def get_insertion_alternatives(self, pattern_edges: list[str], provided_attributes: list[str]) -> list[tuple[str, dict[str,str]]]:
        """
        This function performs all required checks for an insertion to be correct, and returns a list of sets where the data needs to be inserted.
        :param pattern_edges: List of edge names defining the operation.
        :param provided_attributes: List of attribute names provided for the insertion.
        :return:
        """
        set_combinations, _, _ = self.create_bucket_combinations(pattern_edges, provided_attributes)
        # Check that the insertion has exactly one table name inside (otherwise, insertions are not allowed)
        insert_points = [combination[0] for combination in set_combinations if len(combination) == 1]
        if len(insert_points) == 0:
            raise ValueError(f"🚨 Insertions cannot be executed if the pattern {pattern_edges} does not appear in any set or requires accessing many sets")
        if len(insert_points) > 1:
            warnings.warn(f"⚠️ The insertion may be ambiguous or there is redundancy in the design, since it affects different tables: {insert_points}")
        result = []
        for set_name in insert_points:
            struct_name_list = self.get_struct_names_inside_set_name(set_name)
            # Check that all anchor points are provided
            # Get the anchor attributes of the set
            anchor_attributes = []
            # Just need to take any struct, because all share the same anchor
            for key in self.get_anchor_end_names_by_struct_name(struct_name_list[0]):
                if self.is_class_phantom(key):
                    anchor_attributes.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                # If it is not a class, it is a loose end
                else:
                    anchor_attributes.append(key)
            if any(attribute not in provided_attributes for attribute in anchor_attributes):
                raise ValueError(f"🚨 Some anchor attribute in {anchor_attributes} of structs in set '{set_name}' is not provided in the insertion with pattern {pattern_edges}")
            # Check if all mandatory information is provided
            replacements = {}
            for struct_name in struct_name_list:
                # Create a restricted struct to search for paths that do not cross the anchor
                dont_cross = self.get_anchor_associations_by_struct_name(struct_name)
                restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                bipartite = restricted_struct.H.remove_edges(dont_cross).bipartite()
                for table_attribute in self.get_attribute_names_by_struct_name(struct_name):
                    for anchor_attribute in anchor_attributes:
                        paths = list(nx.all_simple_paths(bipartite, source=anchor_attribute, target=table_attribute))
                        assert len(
                            paths) <= 1, f"☠️ Unexpected problem in '{struct_name}' on finding more than one path '{paths}' between '{anchor_attribute}' and '{table_attribute}'"
                        # It may happen that the attribute is not connected to this anchor (still should be connected to another one)
                        if len(paths) == 1:
                            # First position in the tuple of multiplicities is the min multiplicity at least one
                            if self.check_multiplicities_to_one(paths[0])[0] and table_attribute not in provided_attributes:
                                # If the attribute is an ID, -2 is its class, -3 is its phantom and -4 is the association
                                if len(paths[0]) > 3 and self.is_id(table_attribute):
                                    # If it is an association end, we take note of the replacement
                                    alternative = self.get_association_ends().query(f"edges=='{paths[0][-4]}' and nodes=='{paths[0][-3]}'").iloc[0]["name"]
                                    if alternative in provided_attributes:
                                        replacements[alternative] = table_attribute
                                else:
                                    raise ValueError(f"🚨 Mandatory attribute '{table_attribute}' of struct '{struct_name}' in set '{set_name}' is not provided in the insertion")
            result.append((set_name, replacements))
        return result
