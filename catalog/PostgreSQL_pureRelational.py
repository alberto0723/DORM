import logging
from IPython.display import display
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

from .relational import Relational

class PostgreSQL(Relational):
    """This is a subclass of Relational that implements the code generation in PostgreSQL
    """
    def __init__(self, file=None):
        super().__init__(file)

    def is_correct(self, design=False):
        correct = super().is_correct(design)
        if correct:
            # ---------------------------------------------------------------- ICs about being a pure relational catalog
            # IC-PureRelational1: All relationships from the root of a struct must be to one (or less)
            logging.info("Checking IC-PureRelational1 -> To Be Implemented")

        return correct

    def create_tables(self):
        logging.info("Creating tables")
        firstlevels = self.get_inbound_firstLevel()
        # For each table
        for table in firstlevels.itertuples():
            clause_PK = None
            logging.info("-- Creating table " + table.Index[0])
            sentence = "CREATE TABLE IF NOT EXISTS " + table.Index[0] + " (\n"
            struct_phantom = self.get_outbound_sets().query('edges == "'+table.Index[0]+'"')
            struct = self.get_inbound_structs().query('nodes == "'+struct_phantom.index[0][1]+'"')
            elements = self.get_outbound_structs().query('edges == "'+struct.index[0][0]+'"')
            # For each element in the table
            for elem in elements.itertuples():
                # If it is an attribute or class id
                attribute = pd.concat([self.get_ids(), self.get_attributes()]).query('nodes == "'+elem.Index[1]+'"')
                if attribute.shape[0] != 0:
                    sentence += "  " + attribute.iloc[0]["name"]
                    if attribute.iloc[0]["misc_properties"].get("DataType") == "String":
                        sentence += " VarChar(" + str(attribute.iloc[0]["misc_properties"].get("Size")) + "),\n"
                    else:
                        sentence += " " + attribute.iloc[0]["misc_properties"].get("DataType") + ",\n"
                    if elem.misc_properties.get("Root"):
                        clause_PK = "  PRIMARY KEY ("+elem.Index[1]+")\n"
                # If it is a relationship
                else:
                    relationship = self.get_inbound_relationships().query('nodes == "'+elem.Index[1]+'"')
                    legs = self.get_outbound_relationships().query('edges == "'+relationship.index[0][0]+'"')
                    leg_names = []
                    for leg in legs.itertuples():
                        leg_names.append(leg.Index[1])
                        attribute = self.get_ids().query('nodes == "' + leg.Index[1] + '"')
                        if attribute.shape[0] != 0:
                            sentence += "  " + attribute.iloc[0]["name"] + " " + attribute.iloc[0][
                                "misc_properties"].get("DataType") + ",\n"
                    if elem.misc_properties.get("Root"):
                        clause_PK = "  PRIMARY KEY ("+",".join(leg_names)+")\n"
            if clause_PK is None:
                raise ValueError(f"Table '{table.Index[0]}' does not have a primary key (a.k.a. root in the corresponding struct) defined")
            sentence += clause_PK + "  );"
            print(sentence)
