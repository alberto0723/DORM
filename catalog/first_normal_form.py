import logging
import warnings
import pandas as pd
from IPython.display import display
import networkx as nx

from .relational import Relational
from .tools import custom_warning, drop_duplicates

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

    def is_correct(self, design=False) -> bool:
        correct = super().is_correct(design)
        # Not worth to check anything if the more basic stuff is already not correct
        if correct:
            # ---------------------------------------------------------------- ICs about being a normalized catalog
            # IC-Normalized1: All associations from the anchor of a class must be to one (at most)
            logger.info("Checking IC-Normalized1")
            firstlevels = self.get_inbound_firstLevel()
            # For each table
            for table in firstlevels.itertuples():
                for struct in self.get_outbound_set_by_name(table.Index[0]).itertuples():
                    struct_name = self.get_edge_by_phantom_name(struct.Index[1])
                    members = self.get_outbound_struct_by_name(struct_name).index.get_level_values(1).tolist()
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
                                        correct = False
                                        print(f"🚨 IC-FirstNormalForm1 violation: A struct '{struct_name}' has an unacceptable path (not to one) '{paths[0]}'")
        return correct

    def generate_attr_projection_clause(self, attr_path: list[dict[str, str]]) -> str:
        super().generate_attr_projection_clause(attr_path)
        assert len(attr_path) == 1, f"☠️ Incorrect length of attribute path '{attr_path}', which should be one"
        return attr_path[0].get("name")

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        One or more structs are expected inside the set, but all of them should generate the same attributes.
        Inside each table, there are all the attributes in the struct, plus the IDs of the classes, plus the loose ends
        of the associations.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info("-- Creating table " + table.Index[0])
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table.Index[0] + " (\n"
            # Get all the attributes in all the structs
            attr_paths = []
            for struct_name in self.get_struct_names_inside_set_name(table.Index[0]):
                attr_paths.extend(self.get_struct_attributes(struct_name))
            attr_paths = drop_duplicates(attr_paths)
            assert len(set([self.generate_attr_projection_clause(path) for _, path in attr_paths])) == len(attr_paths), f"☠️ Table '{table.Index[0]}' has the same attribute defined twice: {attr_paths}"
            # Add all the attributes to the CREATE TABLE sentence
            attribute_list = []
            for _, attr_path in attr_paths:
                attribute = self.get_attribute_by_name(self.get_domain_attribute_from_path(attr_path))
                if attribute["misc_properties"].get("DataType") == "String":
                    attribute_list.append("  " + self.generate_attr_projection_clause(attr_path) + " VarChar(" + str(attribute["misc_properties"].get("Size")) + ")")
                else:
                    attribute_list.append("  " + self.generate_attr_projection_clause(attr_path) + " " + attribute["misc_properties"].get("DataType"))
            sentence += ",\n".join(attribute_list) + "\n  );"
            statements.append(sentence)
        return statements

    def generate_migration_statements(self, migration_source) -> list[str]:
        """
        Generates insertions to migrate data from one schema to another one.
        Both must be in the same database for it to work.
        :param migration_source: Database schema to migrate the data from.
        :return: List of statements generated to migrate the data (one per struct)
        """
        statements = []
        source = FirstNormalForm(dbms=self.dbms, ip=self.ip, port=self.port, user=self.user, password=self.password, dbname=self.dbname, dbschema=migration_source)
        self.check_migration(source, migration_source)
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info(f"-- Generating data migration for table {table.Index[0]}")
            # For each struct in the table, we have to create a different extraction query
            for struct_name in self.get_struct_names_inside_set_name(table.Index[0]):
                project = [self.generate_attr_projection_clause(path) for _, path in self.get_struct_attributes(struct_name)]
                pattern = []
                for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
                    if self.is_class_phantom(incidence.Index[1]) or self.is_association_phantom(incidence.Index[1]):
                        pattern.append(self.get_edge_by_phantom_name(incidence.Index[1]))
                sentence = f"INSERT INTO {table.Index[0]}({", ".join(project)})\n" + source.generate_query_statement({"project": project, "pattern": pattern},
                                                                                                                     explicit_schema=True)[0] + ";"
                statements.append(sentence)
        return statements

    def generate_add_pk_statements(self) -> list[str]:
        """
        Generated the DDL to add PKs to the tables.
        The primary key of the table is composed by the IDs of the classes in the anchor of the struct, plus the loose.
        ends of the associations in the anchor.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info(f"-- Altering table {table.Index[0]} to add the PK")
            sentence = "ALTER TABLE " + table.Index[0] + " ADD"
            # Create the PK
            # All structs in a set must share the anchor attributes (IC-Design4), so we can take any of them
            struct_name = self.get_struct_names_inside_set_name(table.Index[0])[0]
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class_phantom(key):
                    key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                # If it is not a class, it is a loose end
                else:
                    key_list.append(key)
            assert key_list, f"☠️ Table '{table.Index[0]}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined"
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
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table_referee in firstlevels.itertuples():
            # Get all the attributes in all the structs
            attribute_list = []
            for struct_name in self.get_struct_names_inside_set_name(table_referee.Index[0]):
                attribute_list.extend(self.get_struct_attributes(struct_name))
            # Check all the attributes to see if they require an FK
            for dom_attr_name, attr_path in attribute_list:
                attr_correspondence = self.get_domain_attribute_from_path(attr_path)
                if self.is_id(attr_correspondence):
                    # If it comes from an association
                    if dom_attr_name != attr_correspondence:
                        class_referee = self.get_class_name_by_end_name(dom_attr_name)
                        hierarchy = [class_referee] + self.get_superclasses_by_class_name(class_referee)
                    # If the attribute comes from a class (the FK corresponds to generalization)
                    else:
                        # Get the classes in the struct that provide the ID
                        hierarchies = []
                        for struct_name in self.get_struct_names_inside_set_name(table_referee.Index[0]):
                            for elem in self.get_outbound_struct_by_name(struct_name).index.get_level_values("nodes"):
                                if self.is_class_phantom(elem):
                                    class_name = self.get_edge_by_phantom_name(elem)
                                    if dom_attr_name == self.get_class_id_by_name(class_name):
                                        hierarchies.append([class_name]+self.get_superclasses_by_class_name(class_name))
                        assert len(hierarchies) > 0, f"☠️ The ID '{dom_attr_name}' we are looking for should be in some struct in '{table_referee}'"
                        # Take the shorter hierarchy
                        hierarchy = sorted(hierarchies, key=len)[0]
                    # Follow the hierarchy bottom to top in order until a superclass is found to point to
                    found = False
                    for class_name in hierarchy:
                        for table_referred in firstlevels.itertuples():
                            # We can take any struct in the set, because all must share the anchor
                            struct_name = self.get_struct_names_inside_set_name(table_referred.Index[0])[0]
                            anchor_points = self.get_anchor_points_by_struct_name(struct_name)
                            assert len(anchor_points) > 0, f"☠️ Struct '{struct_name}' should have at least one anchor point"
                            assert self.is_class_phantom(anchor_points[0]), f"☠️ Anchor point '{anchor_points[0]}' must be class phantoms"
                            attr_proj = self.generate_attr_projection_clause(attr_path)
                            if (len(anchor_points) == 1 and self.get_edge_by_phantom_name(anchor_points[0]) == class_name
                                    and (table_referee != table_referred or attr_proj != attr_correspondence)):
                                found = True
                                logger.info(f"-- Altering table {table_referee.Index[0]} to add the FK on '{attr_proj}'")
                                # Create the FK
                                sentence = f"ALTER TABLE {table_referee.Index[0]} ADD FOREIGN KEY ({attr_proj}) REFERENCES {table_referred.Index[0]}({attr_correspondence});"

                                statements.append(sentence)
                        if found:
                            break
        return statements
