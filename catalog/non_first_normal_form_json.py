import logging
import warnings
import pandas as pd
from IPython.display import display
from tqdm import tqdm

from . import config
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
            # if show_progress:
            #    print("    Checking NF2_JSON constraints")
            pass
        return consistent

    def generate_attr_projection_clause(self, attr_path: list[dict[str, str]]) -> str:
        super().generate_attr_projection_clause(attr_path)
        path = "value"
        for hop in attr_path[:-1]:
            if hop["kind"] == "Set":
                path = "jsonb_array_elements(" + path + "->'" + hop.get("name") + "')"
            else:
                path += "->'" + hop.get("name") + "'"
        path += "->>'" + attr_path[-1].get("name") + "'"
        return path

    def build_jsonb_object(self, attr_paths: list[tuple[str, list[dict[str, str]]]]) -> [str, list[str]]:
        # TODO: Generalize this to any number of nested sets
        #       The limitation is the multiple grouping sets and nested 'jsonb_agg', which PostgreSQL that does not allow
        formatted_pairs = []
        pending_attributes = {}
        tmp_grouping = []
        final_grouping = []
        for dom_attr_name, attr_path in attr_paths:
            current_name = attr_path[0].get("name")
            if len(attr_path) == 1:
                formatted_pairs.append("('" + current_name + "', to_jsonb(" + dom_attr_name + "))")
                tmp_grouping.append(dom_attr_name)
            else:
                if current_name in pending_attributes:
                    pending_attributes[current_name] = pending_attributes[current_name]+[(dom_attr_name, attr_path[1:])]
                else:
                    pending_attributes[current_name] = [(dom_attr_name, attr_path[1:])]
        for key, paths in pending_attributes.items():
            assert self.is_struct(key) or self.is_set(key), f"☠️ On creating a nested attribute in a JSONB object, '{key}' should be either a struct or a set"
            nested_object, nested_grouping = self.build_jsonb_object(paths)
            if self.is_struct(key):
                formatted_pairs.append("('" + key + "', to_jsonb(" + nested_object + "))")
                final_grouping = nested_grouping
            else:
                assert not nested_grouping, f"☠️ There is a limitation of PostgreSQL that does not allow to nest 'jsonb_agg', hence, nested sets are not allowed as in '{key}'"
                formatted_pairs.append("('" + key + "', jsonb_agg(DISTINCT " + nested_object + "))")
                final_grouping = tmp_grouping
#        return f"jsonb_build_object({', '.join(formatted_pairs)})", final_grouping
        # TODO: Needs to check what happens when we have lists inside a document, and even if nesting lists into lists is now possible
        return f"(SELECT jsonb_object_agg(k,v) FROM (VALUES {', '.join(formatted_pairs)}) AS __kv__(k,v))", final_grouping

    def generate_migration_insert_statement(self, table_name: str, project: list[str], pattern: list[str], source: Relational) -> str:
        """
        Generates insert statements to migrate data from a database to another.
        :param table_name: The table to be loaded.
        :param project: List of attributes to be loaded in that table.
        :param pattern: List of domain elements that determine the content of the table.
        :param source: The source catalog to get the data from.
        :return: The SQL statement that moves the data from one schema to another.
        """
        # This is more complex than the 1NF, because we have to generate the paths of attributes inside the JSON
        attr_paths = []
        for struct_name in self.get_struct_names_inside_set_name(table_name):
            attr_paths.extend(self.get_struct_attributes(struct_name))
        attr_paths = drop_duplicates(attr_paths)
        mismatch = [attr for attr in project if attr not in [attr2 for attr2, _ in attr_paths]]
        assert not mismatch, f"Attributes '{mismatch}' found in the required projection of the migration table '{table_name}' are not found in the paths of table"
        # Remove unnecessary paths, whose attributes are actually not being migrated (this would be unnecessary if the struct name would be known)
        attr_paths = [(attr, paths) for attr, paths in attr_paths if attr in project]
        obj, grouping = self.build_jsonb_object(attr_paths)
        if grouping:
            return (f"INSERT INTO {table_name}(value)\n  SELECT {obj}\n  FROM (\n    " +
                                    source.generate_query_statement({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ") AS foo" +
                                    "\nGROUP BY " + ", ".join(grouping) + ";")
        else:
            return (f"INSERT INTO {table_name}(value)\n  SELECT {obj}\n  FROM (\n    " +
                                    source.generate_query_statement({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ") AS foo;")

    def generate_values_clause(self, table_name, data_values) -> str:
        """
        Values generation depends on the concrete implementation strategy.
        :param table_name: Name of the table
        :param data_values: Dictionary with pairs attribute name and value
        :return: String representation of the values to be inserted
        """
        attr_paths = []
        for struct_name in self.get_struct_names_inside_set_name(table_name):
            attr_paths.extend(self.get_struct_attributes(struct_name))
        attr_paths = drop_duplicates(attr_paths)
        obj, grouping = self.build_jsonb_object(attr_paths)
        if grouping:
            assert False, f"☠️ Unexpected grouping '{grouping}' in the insertion of '{data_values}' into '{table_name}' (insertions are not allowed in the presence of nested sets)"
        for k, v in data_values.items():
            obj = obj.replace("', " + k, "', " + v)
        return table_name + "(value) VALUES (" + obj + ")"

    def generate_create_table_statements(self) -> list[str]:
        """
        Generated the DDL for the tables in the design. One table is created for every set in the first level (i.e., without parent).
        The same table is generated irrespectively of the attributes: one numerical key and one JSON value that will contain all the data.
        :return: List of statements generated (one per table)
        """
        statements = []
        # For each table
        for table_name in tqdm(self.get_inbound_firstLevel().index.get_level_values("edges"), desc="Generating create table statements", leave=config.show_progress):
            logger.info("-- Creating table " + table_name)
            # sentence = "DROP TABLE IF EXISTS " + table.Index[0] +" CASCADE;\n"
            sentence = "CREATE TABLE " + table_name + " (\n  key SERIAL,\n  value JSONB\n  );"
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
        for table_name in tqdm(self.get_inbound_firstLevel().index.get_level_values("edges"), desc="Generating primary key declaration statements", leave=config.show_progress):
            logger.info(f"-- Altering table {table_name} to add the surrogate PK and a UNIQUE index for the true PK")
            statements.append(f"ALTER TABLE {table_name} ADD PRIMARY KEY (key);")
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
