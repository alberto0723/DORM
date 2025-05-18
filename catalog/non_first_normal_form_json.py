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
    This is a subclass of Relational that implements the code generation as denormalized inside a JSON attribute.
    An autoincrement key is also added to each table as PK for simplicity.
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

    def generate_jsonb_object_clause(self, attr_paths: list[tuple[str, list[dict[str, str]]]]) -> str:
        """
        This generates the correspondence between attribute names in a struct and their corresponding attribute.
        It is necessary to do it to consider loose ends (i.e., associations without class), which generate foreign keys.
        :param struct_name:
        :param attr_paths: A list of tuples with pairs "attribute_name" and a list of elements.
                 Each element is a dictionary itself, which represents a hop in the design (though sets and structs).
                 The last element corresponds to the "domain_name" in the hypergraph for the attribute, which can be the same attribute or association end.
        :returns: A string containing a jsonb-encoded object
        """
        clause = "jsonb_build_object("
        ignore_structs = []
        formatted_elems = []
        for dom_attr_name, path in attr_paths:
            elem_name = path[0].get("name", None)
            assert elem_name is not None, f"☠️ Path element without name in {path}"
            if path[0].get("kind") == "Struct":
                if elem_name not in ignore_structs:
                    ignore_structs.append(elem_name)
                    temp_attr_paths = [(a, p[1:]) for a, p in attr_paths if p[0].get("name") == elem_name]
                    formatted_elems.append("'" + elem_name + "', " + self.generate_jsonb_object_clause(temp_attr_paths))
            # elif paths[0].get("kind") == "Set":
                # TODO: Implement the management of sets
            else:
                formatted_elems.append("'" + path[0].get("name") + "', " + dom_attr_name)
        clause += ",\n".join(formatted_elems) + ")"
        return clause

    def generate_insert_statement(self, table_name: str, project: list[str], pattern: list[str], source: Relational) -> str:
        """
        Generates insert statements to migrate data from a database to another.
        :param table_name: The table to be loaded.
        :param project: List of attributes to be loaded in that table.
        :param pattern: List of domain elements that determine the content of the table.
        :param source: The source catalog to get the data from.
        :return: The SQL statement that moves the data from one schema to another.
        """
        logger.info("-- Migrating table " + table_name)
        attr_paths = []
        for struct_name in self.get_struct_names_inside_set_name(table_name):
            attr_paths.extend(self.get_struct_attributes(struct_name))
        attr_paths = drop_duplicates(attr_paths)
        assert len(attr_paths) == len(project), f"Mismatch in the number of attributes of the table {table_name} ({len(attr_paths)}) and those required for the migration ({len(project)})"
        assert all([attr in project for attr, _ in attr_paths]), f"Some attribute in the paths '{attr_paths}' of table '{table_name}' is not in the projection of the migration table {project}"
        sentence = (f"INSERT INTO {table_name}(value)\n  SELECT {self.generate_jsonb_object_clause(attr_paths)}\n  FROM (\n    " +
                    source.generate_query_statement({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ") AS foo;")
        return sentence

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        The same table is generated irrespectively of the attributes: one numerical key and one JSON value that will contain all the data.
        :return: List of statements generated (one per table)
        """
        statements = []
        # For each table
        for table_name in self.get_inbound_firstLevel().index.get_level_values("edges"):
            logger.info("-- Creating table " + table_name)
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table_name + " (\n  key SERIAL PRIMARY KEY,\n  value JSONB\n  );"
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
        # For each table
        for table_name in self.get_inbound_firstLevel().index.get_level_values("edges"):
            logger.info(f"-- Altering table {table_name} to add the PK (actually just uniqueness)")
            sentence = "CREATE UNIQUE INDEX pk_" + table_name + " ON " + table_name
            # Create the PK
            # All structs in a set must share the anchor attributes (IC-Design4), so we can take any of them
            struct_name = self.get_struct_names_inside_set_name(table_name)[0]
            key_list = []
            for key in self.get_anchor_end_names_by_struct_name(struct_name):
                if self.is_class_phantom(key):
                    key_list.append(self.get_class_id_by_name(self.get_edge_by_phantom_name(key)))
                # If it is not a class, it is a loose end
                else:
                    key_list.append(key)
            assert key_list, f"☠️ Table '{table_name}' does not have a primary key (a.k.a. anchor in the corresponding struct) defined"
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
