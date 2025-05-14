import logging
import warnings
import pandas as pd
from IPython.display import display
import networkx as nx

from .tools import drop_duplicates
from .relational import Relational

# Library initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("NonFirstNormalFormJSON")


class NonFirstNormalFormJSON(Relational):
    """
    This is a subclass of Relational that implements the code generation as normalized in 1NF.
    """
    def __init__(self, *args, **kwargs):
        kwargs["paradigm_name"] = "Non First Normal Form with JSON"
        super().__init__(*args, **kwargs)
        logger.info("Using a non-first normal form (NF2) implementation of the schema using JSON")

    def is_consistent(self, design=False) -> bool:
        consistent = super().is_consistent(design)
        # Not worth to check anything if the more basic stuff is already not consistent
        if consistent:
            pass
        return consistent

    def generate_attr_projection_clause(self, attr_path: list[dict[str, str]]) -> str:
        super().generate_attr_projection_clause(attr_path)
        path = "value"
        for hop in attr_path[:-1]:
            path += "->'" + hop.get("name") + "'"
        path += "->>'" + attr_path[-1].get("name") + "'"
        return path

    def generate_insert_statement(self, table_name: str, project: list[str], pattern: list[str], source: Relational) -> str:
        '''
        Generates insert statements to migrate data from a database to another.
        :param table_name: The table to be loaded.
        :param project: List of attributes to be loaded in that table.
        :param pattern: List of domain elements that determine the content of the table.
        :param source: The source catalog to get the data from.
        :return: The SQL statement that moves the data from one schema to another.
        '''
        attr_paths = []
        for struct_name in self.get_struct_names_inside_set_name(table_name):
            attr_paths.extend(self.get_struct_attributes(struct_name))
        attr_paths = drop_duplicates(attr_paths)
        assert len(attr_paths) == len(project), f"Mismatch in the number of attributes of the table {table_name} ({len(attr_paths)}) and those required for the migration ({len(project)})"
        assert all([attr in project for attr, _ in attr_paths]), f"Some attribute in the paths '{attr_paths}' of table '{table_name}' is not in the projection of the migration table {project}"
        # TODO: This needs to be properly implemented to consider nested elements
        formatted_pairs = ["'" + attr_path[-1].get("name") + "', " + dom_attr_name for dom_attr_name, attr_path in attr_paths]
        return (f"INSERT INTO {table_name}(value)\n  SELECT jsonb_build_object({", ".join(formatted_pairs)})\n  FROM (\n    " +
                            source.generate_query_statement({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ") AS foo;")

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        The same table is generated irrespectively of the attributes: one numerical key and one JSON value that will contain all the data.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info("-- Creating table " + table.Index[0])
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table.Index[0] + " (\n  key SERIAL PRIMARY KEY,\n  value JSONB\n  );"
            statements.append(sentence)
        return statements

    def generate_add_pk_statements(self) -> list[str]:
        """
        Generated the DDL to add PKs to the tables.
        Actually, in the case of NF2, it just adds uniqueness to the corresponding attributes inside the JSON, for the
        IDs of the classes in the anchor of the struct, plus the loose ends of the associations in the anchor.
        :return: List of statements generated (one per table)
        """
        statements = []
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            logger.info(f"-- Altering table {table.Index[0]} to add the PK (actually just uniqueness)")
            sentence = "CREATE UNIQUE INDEX pk_" + table.Index[0] + " ON " + table.Index[0]
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
            # This is not considering that an anchor of a struct can be in a nested struct (only at first level)
            sentence += "((" + "), (".join(["value->>'" + k + "'" for k in key_list]) + "));"
            statements.append(sentence)
        return statements

    def generate_add_fk_statements(self) -> list[str]:
        """
        FKs cannot be generated over JSONB attributes in PostgreSQL.
        :return: List of statements generated (one per table)
        """
        warnings.warn("⚠️ Foreign keys cannot be defined over PostgreSQL JSONB attributes (hence, not implemented in NonFirstNormalFormJSON class)")
        return []
