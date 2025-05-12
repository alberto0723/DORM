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
        super().__init__(*args, **kwargs)
        logger.info("Using a non-first normal form (NF2) implementation of the schema using JSON")
        # This print is just to avoid silly mistakes while testing, can eventually be removed
        print("*********************** NonFirstNormalFormJSON ***********************")

    def is_correct(self, design=False) -> bool:
        correct = super().is_correct(design)
        # Not worth to check anything if the more basic stuff is already not correct
        if correct:
            pass
        return correct

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

    def generate_migration_statements(self, migration_source) -> list[str]:
        """
        Generates insertions to migrate data from one schema to another one.
        Both must be in the same database for it to work.
        :param migration_source: Database schema to migrate the data from.
        :return: List of statements generated to migrate the data (one per struct)
        """
        statements = []
        source = NonFirstNormalFormJSON(dbms=self.dbms, ip=self.ip, port=self.port, user=self.user, password=self.password, dbname=self.dbname, dbschema=migration_source)
        self.check_migration(source, migration_source)
        firstlevels = self.get_inbound_firstLevel()
        # TODO: Create the insertions with JSON format
        # For each table
        # for table in firstlevels.itertuples():
        #     logger.info(f"-- Generating data migration for table {table.Index[0]}")
        #     # For each struct in the table, we have to create a different extraction query
        #     for struct_name in self.get_struct_names_inside_set_name(table.Index[0]):
        #         project = list(self.get_struct_attributes(struct_name).keys())
        #         pattern = []
        #         for incidence in self.get_outbound_struct_by_name(struct_name).itertuples():
        #             if self.is_class_phantom(incidence.Index[1]) or self.is_association_phantom(incidence.Index[1]):
        #                 pattern.append(self.get_edge_by_phantom_name(incidence.Index[1]))
        #         sentence = f"INSERT INTO {table.Index[0]}({", ".join(project)})\n" + source.generate_sql({"project": project, "pattern": pattern}, explicit_schema=True)[0] + ";"
        #         statements.append(sentence)
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
