import logging
from IPython.display import display
import pandas as pd
import sqlalchemy  # https://www.sqlalchemy.org
import hypernetx as hnx
import json

from .catalog import Catalog

# Libraries initialization
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = logging.getLogger("Relational")


class Relational(Catalog):
    """This is a subclass of Catalog that implements the constraints for relational databases
    """
    # This contains the connection to the database to store the catalog
    engine = None
    dbschema = None
    TABLE_NODES = 'dorm_catalog_nodes'
    TABLE_EDGES = 'dorm_catalog_edges'
    TABLE_INCIDENCES = 'dorm_catalog_incidences'

    def __init__(self, file_path=None, dbms=None, ip=None, port=None, user=None, password=None, dbname=None, dbschema=None, supersede=False):
        if user is None or password is None:
            super().__init__(file_path=file_path)
        else:
            if dbms is None or ip is None or port is None or dbname is None or dbschema is None:
                ValueError("Missing required parameters: dbms, ip, port, user, password, dbname, dbschema")
            url = f"{dbms}://{user}:{password}@{ip}:{port}/{dbname}"
            logger.info(f"Creating database connection to '{url}'")
            self.engine = sqlalchemy.create_engine(url, connect_args={"options": f"-csearch_path={dbschema}"})
            with self.engine.connect() as conn:
                if supersede:
                    logger.info(f"Creating schema '{dbschema}'")
                    conn.execute(sqlalchemy.text(f"DROP SCHEMA IF EXISTS {dbschema} CASCADE;"))
                    conn.execute(sqlalchemy.text(f"CREATE SCHEMA {dbschema};"))
                    conn.execute(sqlalchemy.text(f"COMMENT ON SCHEMA {dbschema} IS '"+"{}';"))
                    conn.commit()
            if supersede:
                super().__init__(file_path=file_path)
            else:
                catalog_tables = [self.TABLE_NODES, self.TABLE_EDGES, self.TABLE_INCIDENCES]
                if all(table in sqlalchemy.inspect(self.engine).get_table_names() for table in catalog_tables):
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
                    with self.engine.connect() as conn:
                        result = conn.execute(sqlalchemy.text("SELECT n.nspname AS schema_name, d.description AS comment FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid WHERE n.nspname=:schema;"), {"schema": dbschema})
                        row = result.fetchone()
                    if row:
                        self.origin = json.loads(row.comment)
                    else:
                        ValueError("No comment found in the schema of the database (necessary to check domain and design origin)")
                else:
                    ValueError(f"Missing required tables '{catalog_tables}' in the database")
            self.dbschema = dbschema

    def save(self, file_path=None):
        if file_path is not None:
            super().save(file_path)
        elif self.engine is not None:
            logger.info("Checking the catalog before saving it in the database")
            if self.is_correct("design" in self.origin):
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
                with (self.engine.connect() as conn):
                    statement = f"COMMENT ON SCHEMA {self.dbschema} IS '{json.dumps(self.origin)}';"
                    conn.execute(sqlalchemy.text(statement))
                    conn.commit()
        else:
            ValueError("No connection to the database or file provided")

    def is_correct(self, design=False, verbose=True):
        correct = super().is_correct(design, verbose)
        structs = self.get_structs()
        sets = self.get_sets()
        if correct:
            # --------------------------------------------------------------------- ICs about being a relational catalog
            # IC-Relational1: All sets are first level
            logger.info("Checking IC-Relational1")
            matches6_1 = self.get_inbound_firstLevel().reset_index(drop=False)
            violations6_1 = sets[~sets["name"].isin(matches6_1["edges"])]
            if violations6_1.shape[0] > 0:
                correct = False
                print("IC-Relational1 violation: Some sets are not at the first level")
                display(violations6_1)

            # IC-Relational2: All second level are structs
            logger.info("Checking IC-Relational2")
            matches6_2 = self.get_inbound_firstLevel().merge(
                            self.get_outbounds().reset_index(drop=False), on="edges", how="inner", suffixes=[None, "_firsthop"]).merge(
                            self.get_inbounds().reset_index(drop=False), on="nodes", how="inner", suffixes=[None, "_secondhop"])
            violations6_2 = matches6_2[~matches6_2["misc_properties_secondhop"].apply(lambda x: x['Kind'] == 'StructIncidence')]
            if violations6_2.shape[0] > 0:
                correct = False
                print("IC-Relational2 violation: Some second level are not structs")
                display(violations6_2)

            # IC-Relational3: All structs are at second level
            logger.info("Checking IC-Relational3")
            violations6_3 = structs[~structs["name"].isin(matches6_2["edges_secondhop"])]
            if violations6_3.shape[0] > 0:
                correct = False
                print("IC-Relational3 violation: Some structs are not at the second level")
                display(violations6_1)

        return correct
