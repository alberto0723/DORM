import logging
import warnings
import pandas as pd
from IPython.display import display
import networkx as nx
from tqdm import tqdm

from . import config
from .relational import Relational
from .tools import custom_warning, custom_progress, str_list_difference

# Library initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("FirstNormalForm")
warnings.showwarning = custom_warning


class FirstNormalForm(Relational):
    """
    This is a subclass of Relational that implements the code generation as normalized in 1NF.
    """
    def __init__(self, *args, **kwargs):
        kwargs["paradigm_name"] = "First Normal Form"
        super().__init__(*args, **kwargs)
        logger.info("Using a first normal form (1NF) traditional implementation of the schema")

    def is_consistent(self, design=False) -> bool:
        consistent = super().is_consistent(design)
        # Not worth to check anything if the more basic stuff is already not consistent
        if consistent:
            root_names = self.get_root_edges()

            # ---------------------------------------------------------------- ICs about being a First Normal Form catalog
            custom_progress("    Checking 1NF constraints")

            # IC-FirstNormalForm1: Sets can only appear as root edges
            logger.info("Checking IC-FirstNormalForm1")
            sets = self.get_sets()
            violations7_1 = str_list_difference(sets, root_names)
            if violations7_1:
                consistent = False
                print(f"🚨 IC-FirstNormalForm1 violation: Some sets are not root edges", violations7_1)

            # IC-FirstNormalForm2: Sets can only contain structs
            logger.info("Checking IC-FirstNormalForm2")
            struct_phantom_names = self.get_phantom_structs()
            violations7_2 = self.get_outbound_sets()[~self.get_outbound_sets()["nodes"].isin(struct_phantom_names)]
            if not violations7_2.empty:
                consistent = False
                print("🚨 IC-FirstNormalForm2 violation: Some sets contain elements that are not structs")
                display(violations7_2)

            # IC-FirstNormalForm3: Structs can only appear at the second level
            logger.info("Checking IC-FirstNormalForm3")
            struct_phantom_names = self.get_phantom_structs()
            violations7_3 = self.get_outbounds()[self.get_outbounds().apply(lambda row: row["edges"] not in root_names and row["nodes"] in struct_phantom_names, axis=1)]
            if not violations7_3.empty:
                consistent = False
                print("🚨 IC-FirstNormalForm3 violation: Some structs are not at the second level")
                display(violations7_3)

            # IC-FirstNormalForm4: All associations from the anchor of a class must be to one (at most)
            logger.info("Checking IC-FirstNormalForm4")
            # For each table
            for set_name in root_names:
                for struct_name in self.get_struct_names_by_set_name(set_name):
                    members = self.get_outbound_struct_by_name(struct_name)["nodes"].values.tolist()
                    anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                    dont_cross = self.get_anchor_associations_by_struct_name(struct_name)
                    restricted_struct = self.get_restricted_struct_hypergraph(struct_name)
                    bipartite = restricted_struct.H.remove_edges(dont_cross).bipartite()
                    for anchor in anchor_points:
                        for member in set(members)-set(anchor_points):
                            if self.is_class_phantom(member) or self.is_association_phantom(member):
                                paths = list(nx.all_simple_paths(bipartite, source=anchor, target=member))
                                assert len(paths) <= 1, f"☠️ Unexpected problem in '{struct_name}' on finding more than one path '{paths}' between '{anchor}' and '{member}'"
                                if len(paths) == 1:
                                    # Second position in the tuple is the max multiplicity
                                    if not self.check_multiplicities_to_one(paths[0])[1]:
                                        consistent = False
                                        print(f"🚨 IC-FirstNormalForm4 violation: A struct '{struct_name}' has an unacceptable path (not to one) '{paths[0]}'")
        return consistent

    def generate_attr_projection_clause(self, attr_path: list[dict[str, str]]) -> str:
        super().generate_attr_projection_clause(attr_path)
        assert len(attr_path) == 1, f"☠️ Incorrect length of attribute path '{attr_path}', which should be one"
        return attr_path[0].get("name")

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set which is a root (i.e., without parent).
        One or more structs are expected inside the set, but all of them should generate the same attributes.
        Inside each table, there are all the attributes in the struct, plus the IDs of the classes, plus the loose ends
        of the associations.
        :return: List of statements generated (one per table)
        """
        statements = []
        # For each table
        for table_name in tqdm(self.get_root_edges(), desc="Generating create table statements", leave=config.show_progress):
            logger.info("-- Creating table " + table_name)
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table_name + " (\n"
            # Get all the attributes in all the structs
            attr_paths = {}
            # This merges the attributes in all the structs
            for struct_name in self.get_struct_names_by_set_name(table_name):
                attr_paths |= self.get_struct_attributes(struct_name)
            assert len(set([self.generate_attr_projection_clause(path) for _, path in attr_paths.items()])) == len(attr_paths), f"☠️ Table '{table_name}' has the same attribute defined twice: {attr_paths}"
            # Add all the attributes to the CREATE TABLE sentence
            attribute_list = []
            for _, attr_path in attr_paths.items():
                attribute = self.get_attribute_by_name(self.get_domain_attribute_from_path(attr_path))
                if attribute["DataType"] == "String":
                    attribute_list.append("  " + self.generate_attr_projection_clause(attr_path) + " VarChar(" + str(int(attribute["Size"])) + ")")
                else:
                    attribute_list.append("  " + self.generate_attr_projection_clause(attr_path) + " " + attribute["DataType"])
            sentence += ",\n".join(attribute_list) + "\n  );"
            statements.append(sentence)
        return statements

    def generate_migration_insert_statement(self, table_name: str, project: list[str], pattern: list[str], source: Relational) -> str:
        '''
        Generates insert statements to migrate data from a database to another.
        :param table_name: The table to be loaded.
        :param project: List of attributes to be loaded in that table.
        :param pattern: List of domain elements that determine the content of the table.
        :param source: The source catalog to get the data from.
        :return: The SQL statement that moves the data from one schema to another.
        '''
        return (f"INSERT INTO {table_name}({', '.join(project)})\n  SELECT {', '.join(project)}\n  FROM (\n    " +
                            source.generate_query_statement({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ") AS foo;")

    def generate_values_clause(self, table_name, data_values) -> str:
        """
        Values generation depends on the concrete implementation strategy.
        :param table_name: Name of the table
        :param data_values: Dictionary with pairs attribute name and value
        :return: String representation of the values to be inserted
        """
        return table_name + "(" + ", ".join(data_values.keys()) + ") VALUES (" + ", ".join(data_values.values()) + ")"

    def generate_add_pk_statements(self) -> list[str]:
        """
        Generated the DDL to add PKs to the tables.
        The primary key of the table is composed by the IDs of the classes in the anchor of the struct, plus the loose.
        ends of the associations in the anchor.
        :return: List of statements generated (one per table)
        """
        statements = []
        # For each table
        for table_name in tqdm(self.get_root_edges(), desc="Generating primary key declaration statements", leave=config.show_progress):
            logger.info(f"-- Altering table {table_name} to add the PK")
            sentence = "ALTER TABLE " + table_name + " ADD"
            # Create the PK
            # All structs in a set must share the anchor attributes (IC-Design4), so we can take any of them
            struct_name = self.get_struct_names_by_set_name(table_name)[0]
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class(key):
                    key_list.append(self.get_class_id_by_name(key))
                # If it is not a class, it is a loose end
                else:
                    key_list.append(key)
            assert key_list, f"☠️ Table '{table_name}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined"
            sentence += " PRIMARY KEY (" + ", ".join(key_list) + ");"
            statements.append(sentence)
        return statements

    def generate_add_fk_statements(self) -> list[str]:
        """
        Generated the DDL to add FKs to the tables.
        The foreign keys of a table come from the ends of its associations or class IDs,
        which are attributes and there is another table that has their class (or corresponding superclass) as an anchor.
        :return: List of statements generated (one per table)
        """
        statements = []
        # For each table
        for table_referee_name in tqdm(self.get_root_edges(), desc="Generating foreign key declaration statements", leave=config.show_progress):
            # Get all the attributes in all the structs
            attribute_list = {}
            for struct_name in self.get_struct_names_by_set_name(table_referee_name):
                attribute_list |= self.get_struct_attributes(struct_name)
            # Check all the attributes to see if they require an FK
            for dom_attr_name, attr_path in attribute_list.items():
                attr_correspondence = self.get_domain_attribute_from_path(attr_path)
                if self.is_id(attr_correspondence):
                    # If it comes from an association
                    if dom_attr_name != attr_correspondence:
                        class_referee = self.get_class_name_by_end_name(dom_attr_name)
                        hierarchy = [class_referee] + self.get_generalizations_by_class_name(class_referee, return_superclasses=True)
                    # If the attribute comes from a class (the FK corresponds to generalization)
                    else:
                        # Get the classes in the struct that provide the ID
                        hierarchies = []
                        for struct_name in self.get_struct_names_by_set_name(table_referee_name):
                            for elem in self.get_outbound_struct_by_name(struct_name)["nodes"]:
                                if self.is_class_phantom(elem):
                                    class_name = self.get_edge_by_phantom_name(elem)
                                    if dom_attr_name == self.get_class_id_by_name(class_name):
                                        hierarchies.append([class_name]+self.get_generalizations_by_class_name(class_name, return_superclasses=True))
                        assert len(hierarchies) > 0, f"☠️ The ID '{dom_attr_name}' we are looking for should be in some struct in '{table_referee_name}'"
                        # Take the shorter hierarchy
                        hierarchy = sorted(hierarchies, key=len)[0]
                    # Follow the hierarchy bottom to top in order until a superclass is found to point to
                    found = False
                    for class_name in hierarchy:
                        for table_referred_name in self.get_root_edges():
                            # We can take any struct in the set, because all must share the anchor
                            struct_name = self.get_struct_names_by_set_name(table_referred_name)[0]
                            anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                            assert len(anchor_points) > 0, f"☠️ Struct '{struct_name}' should have at least one anchor point"
                            assert self.is_class(anchor_points[0]), f"☠️ Anchor point '{anchor_points[0]}' must be class phantoms"
                            attr_proj = self.generate_attr_projection_clause(attr_path)
                            # FKs are only generated when they point to a PK of a single attribute
                            if (len(anchor_points) == 1 and anchor_points[0] == class_name
                                    and (table_referee_name != table_referred_name or attr_proj != attr_correspondence)):
                                found = True
                                logger.info(f"-- Altering table {table_referee_name} to add the FK on '{attr_proj}'")
                                # Create the FK
                                sentence = f"ALTER TABLE {table_referee_name} ADD FOREIGN KEY ({attr_proj}) REFERENCES {table_referred_name}({attr_correspondence});"

                                statements.append(sentence)
                        if found:
                            break
        return statements
