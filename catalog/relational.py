from abc import ABC, abstractmethod
import logging
from IPython.display import display
import pandas as pd
import sqlalchemy
import hypernetx as hnx
import json
import re

from .catalog import Catalog

# Libraries initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("Relational")


class Relational(Catalog, ABC):
    """
    This is a subclass of Catalog that implements the constraints specific for relational databases,
    as well as the management of the connection to the DBMS and corresponding interaction.
    It uses SQLAlchemy (https://www.sqlalchemy.org)
    """
    # This contains the connection to the database to store the catalog
    engine = None
    dbms = None
    ip = None
    port = None
    user = None
    password = None
    dbname = None
    dbschema = None
    TABLE_NODES = '__dorm_catalog_nodes'
    TABLE_EDGES = '__dorm_catalog_edges'
    TABLE_INCIDENCES = '__dorm_catalog_incidences'

    def __init__(self, file_path=None, dbms=None, ip=None, port=None, user=None, password=None, dbname=None, dbschema=None, supersede=False):
        if user is None or password is None:
            super().__init__(file_path=file_path)
        else:
            self.dbms = dbms
            self.ip = ip
            self.port = port
            self.user = user
            self.password = password
            self.dbname = dbname
            self.dbschema = dbschema
            self.engine = self.get_engine(dbschema)
            with self.engine.connect() as conn:
                if supersede:
                    logger.info(f"Creating schema '{dbschema}'")
                    conn.execute(sqlalchemy.text(f"DROP SCHEMA IF EXISTS {dbschema} CASCADE;"))
                    conn.execute(sqlalchemy.text(f"CREATE SCHEMA {dbschema};"))
                    conn.execute(sqlalchemy.text(f"COMMENT ON SCHEMA {dbschema} IS '"+"{}';"))
                    conn.commit()
                    # This creates either an empty hypergraph or reads it from a file
                    super().__init__(file_path=file_path)
                else:
                    catalog_tables = [self.TABLE_NODES, self.TABLE_EDGES, self.TABLE_INCIDENCES]
                    assert all(table in sqlalchemy.inspect(self.engine).get_table_names() for table in catalog_tables), f"☠️ Missing required tables '{catalog_tables}' in the database"
                    logger.info(f"Loading the catalog from the database connection")
                    df_nodes = pd.read_sql_table(self.TABLE_NODES, con=self.engine)
                    df_edges = pd.read_sql_table(self.TABLE_EDGES, con=self.engine)
                    df_incidences = pd.read_sql_table(self.TABLE_INCIDENCES, con=self.engine)
                    # There is a bug in the library, and the name of the property column for both nodes and edges is taken from "misc_properties_col"
                    H = hnx.Hypergraph(df_incidences, edge_col="edges", node_col="nodes", cell_weight_col="weight", misc_cell_properties_col="misc_properties",
                                       node_properties=df_nodes, node_weight_prop_col="weight", misc_properties_col="misc_properties",
                                       edge_properties=df_edges, edge_weight_prop_col="weight")
                    super().__init__(hypergraph=H)
                    # Get domain and design
                    result = conn.execute(sqlalchemy.text("SELECT n.nspname AS schema_name, d.description AS comment FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid WHERE n.nspname=:schema;"), {"schema": dbschema})
                    row = result.fetchone()
                    assert row, "☠️ No metadata (in the form of a comment) found in the schema of the database (necessary to check domain and design origin)"
                    self.metadata = json.loads(row.comment)

    def get_engine(self, dbschema) -> sqlalchemy.engine.Engine:
        assert self.dbms is not None and self.ip is not None and self.port is not None and self.user is not None and self.password is not None and self.dbname is not None and dbschema is not None, "☠️ Missing required parameters to create connection: dbms, ip, port, user, password, dbname, dbschema"
        url = f"{self.dbms}://{self.user}:{self.password}@{self.ip}:{self.port}/{self.dbname}"
        logger.info(f"Creating database connection to '{dbschema}' at '{url}'")
        return sqlalchemy.create_engine(url, connect_args={"options": f"-csearch_path={dbschema}"})

    def save(self, file_path=None, migration_source=None, show_sql=False, show_warnings=True) -> None:
        if file_path is not None:
            super().save(file_path)
        elif self.engine is not None:
            logger.info("Checking the catalog before saving it in the database")
            if self.is_correct(design="design" in self.metadata, show_warnings=show_warnings):
                logger.info("Saving the catalog in the database")
                df_nodes = self.H.nodes.dataframe.copy()
                df_nodes['misc_properties'] = df_nodes['misc_properties'].apply(json.dumps)
                df_nodes.to_sql(self.TABLE_NODES, self.engine, if_exists='replace', index=True)
                df_edges = self.H.edges.dataframe.copy()
                df_edges['misc_properties'] = df_edges['misc_properties'].apply(json.dumps)
                df_edges.to_sql(self.TABLE_EDGES, self.engine, if_exists='replace', index=True)
                df_incidences = self.H.incidences.dataframe.copy()
                df_incidences['misc_properties'] = df_incidences['misc_properties'].apply(json.dumps)
                df_incidences.to_sql(self.TABLE_INCIDENCES, self.engine, if_exists='replace', index=True)
                self.create_schema(migration_source=migration_source, show_sql=show_sql)
                self.metadata["tables_created"] = "design" in self.metadata
                if migration_source is not None:
                    self.metadata["data_migrated"] = True
                with (self.engine.connect() as conn):
                    statement = f"COMMENT ON SCHEMA {self.dbschema} IS '{json.dumps(self.metadata)}';"
                    conn.execute(sqlalchemy.text(statement))
                    conn.commit()
        else:
           raise ValueError("🚨 No connection to the database or file provided")

    def is_correct(self, design=False, show_warnings=True) -> bool:
        correct = super().is_correct(design, show_warnings=show_warnings)
        # Only needs to run further checks if the basic one succeeded
        if correct:
            structs = self.get_structs()
            sets = self.get_sets()
            # --------------------------------------------------------------------- ICs about being a relational catalog
            # IC-Relational1: All sets are first level
            logger.info("Checking IC-Relational1")
            matches6_1 = self.get_inbound_firstLevel().reset_index(drop=False)
            violations6_1 = sets[~sets["name"].isin(matches6_1["edges"])]
            if violations6_1.shape[0] > 0:
                correct = False
                print("🚨 IC-Relational1 violation: Some sets are not at the first level")
                display(violations6_1)

            # IC-Relational2: All second level are structs
            logger.info("Checking IC-Relational2")
            matches6_2 = self.get_inbound_firstLevel().merge(
                            self.get_outbounds().reset_index(drop=False), on="edges", how="inner", suffixes=(None, "_firsthop")).merge(
                            self.get_inbounds().reset_index(drop=False), on="nodes", how="inner", suffixes=(None, "_secondhop"))
            violations6_2 = matches6_2[~matches6_2["misc_properties_secondhop"].apply(lambda x: x['Kind'] == 'StructIncidence')]
            if violations6_2.shape[0] > 0:
                correct = False
                print("🚨 IC-Relational2 violation: Some second level are not structs")
                display(violations6_2)

            # IC-Relational3: All structs are at second level
            logger.info("Checking IC-Relational3")
            violations6_3 = structs[~structs["name"].isin(matches6_2["edges_secondhop"])]
            if violations6_3.shape[0] > 0:
                correct = False
                print("🚨 IC-Relational3 violation: Some structs are not at the second level")
                display(violations6_1)
        return correct

    def create_schema(self, migration_source=None, show_sql=False, show_warnings=True) -> None:
        """
        Creates the tables according to the design.
        :param migration_source: Name of the database schema to migrate the data from.
        :param show_sql: Whether to print SQL statements or not.
        :param show_warnings: Whether to print warnings or not.
        """
        logger.info("Creating schema")
        statements = self.generate_create_table_statements(show_sql=show_sql)
        if migration_source is not None:
            statements.extend(self.generate_migration_statements(migration_source, show_sql=show_sql, show_warnings=show_warnings))
        statements.extend(self.generate_add_pk_statements(show_sql=show_sql))
        statements.extend(self.generate_add_fk_statements(show_sql=show_sql))
        if self.engine is not None:
            with self.engine.connect() as conn:
                for statement in statements:
                    conn.execute(sqlalchemy.text(statement))
                conn.commit()

    @abstractmethod
    def generate_create_table_statements(self, show_sql=False) -> list[str]:
        """
        Table creation depends on the concrete implementation strategy.
        :param show_sql: Indicates if the DDL should be printed
        :return: List of statements generated (one per table)
        """
        pass

    @abstractmethod
    def generate_migration_statements(self, migration_source, show_sql=False, show_warnings=True) -> list[str]:
        """
        Migration generation depends on the concrete implementation strategy.
        :param migration_source: Database schema to migrate the data from.
        :param show_sql: Whether to print SQL statements or not.
        :param show_warnings: Whether to print warnings statements or not.
        :return: List of statements generated to migrate the data (one per struct)
        """
        pass

    @abstractmethod
    def generate_add_pk_statements(self, show_sql=False) -> list[str]:
        """
        PK generation depends on the concrete implementation strategy.
        :param show_sql: Whether to print SQL statements or not.
        :return: List of statements generated (one per table)
        """
        pass

    @abstractmethod
    def generate_add_fk_statements(self, show_sql=False) -> list[str]:
        """
        FK generation depends on the concrete implementation strategy.
        :param show_sql: Whether to print SQL statements or not.
        :return: List of statements generated (one per FK)
        """
        pass

    @abstractmethod
    def generate_sql(self, spec, show_warnings=True) -> list[str]:
        """
        SQL generation depends on the concrete implementation strategy.
        :param spec: Specification of a query.
        :param show_warnings: Whether to print warnings or not.
        :return: List of SQL statements corresponding to the specification in the current design.
        """
        pass

    def execute(self, query) -> sqlalchemy.Sequence[sqlalchemy.Row]:
        """
        Executes a query in the engine associated to the catalog.
        :param query: SQL query to be executed.
        :return: Set of rows resulting from the query execution.
        """
        if self.engine is None:
            raise ValueError("🚨 Queries cannot be executed without a connection to the DBMS")
        with self.engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query)).fetchall()
        return result

    def get_cost(self, query) -> float:
        """
        Estimates the cost of a query in the engine associated to the catalog.
        :param query: SQL query to be executed.
        :return: Unitless estimated cost.
        """
        if self.engine is None:
            raise ValueError("🚨 Query cost cannot be estimated without a connection to the DBMS")
        with self.engine.connect() as conn:
            first_row = conn.execute(sqlalchemy.text("EXPLAIN " + query)).fetchone()
        assert first_row is not None, "☠️ Empty access plan"
        # Extract the float (e.g., from "Execution Time: 0.456 ms")
        match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', str(first_row[0]))
        assert match is not None, f"☠️ Cost not found in the access plan of the query '{first_row}'"
        try:
            # match.group(1) gives you just the number (whatever in the parenthesis in the pattern)
            return float(match.group(1))
        except ValueError:
            raise ValueError(f"🚨 Cost parsing failed in the access plan of the query '{first_row}'")

    def get_time(self, query) -> float:
        """
        Estimates the execution time of a query in the engine associated to the catalog (requires true execution!!!).
        :param query: SQL query to be executed.
        :return: Estimated time in milliseconds.
        """
        if self.engine is None:
            raise ValueError("🚨 Query cost cannot be estimated without a connection to the DBMS")
        with self.engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"EXPLAIN (ANALYZE TRUE, SUMMARY TRUE) " + query)).fetchall()
        assert len(result) > 0, "☠️ Empty access plan"
        last_row = result[-1]
        # Extract the float (e.g., from "Execution Time: 0.456 ms")
        match = re.search(r'([\d.]+)\s*ms', str(last_row[0]))
        assert match is not None, f"☠️ Time not found in the access plan of the query '{last_row}'"
        try:
            # match.group(1) gives you just the number, without the " ms" suffix  (whatever in the parenthesis in the pattern)
            return float(match.group(1))
        except ValueError:
            raise ValueError(f"🚨 Cost parsing failed in the access plan of the query '{last_row}'")
