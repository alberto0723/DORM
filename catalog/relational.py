import logging
from IPython.display import display
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

from .catalog import Catalog

class Relational(Catalog):
    """This is a subclass of Catalog that implements the constraints for relational databases
    """
    def __init__(self, file=None):
        super().__init__(file)

    def is_correct(self, design=False):
        correct = super().is_correct(design)
        structs = self.get_structs()
        sets = self.get_sets()
        if correct:
            # --------------------------------------------------------------------- ICs about being a relational catalog
            # IC-Relational1: All sets are first level
            logging.info("Checking IC-Relational1")
            matches6_1 = self.get_inbound_firstLevel().reset_index(drop=False)
            violations6_1 = sets[~sets["name"].isin(matches6_1["edges"])]
            if violations6_1.shape[0] > 0:
                correct = False
                print("IC-Relational1 violation: Some sets are not at the first level")
                display(violations6_1)

            # IC-Relational2: All second level are structs
            logging.info("Checking IC-Relational2")
            matches6_2 = self.get_inbound_firstLevel().merge(
                            self.get_outbounds().reset_index(drop=False), on="edges", how="inner", suffixes=[None, "_firsthop"]).merge(
                            self.get_inbounds().reset_index(drop=False), on="nodes", how="inner", suffixes=[None, "_secondhop"])
            violations6_2 = matches6_2[~matches6_2["misc_properties_secondhop"].apply(lambda x: x['Kind'] == 'StructIncidence')]
            if violations6_2.shape[0] > 0:
                correct = False
                print("IC-Relational2 violation: Some second level are not structs")
                display(violations6_2)

            # IC-Relational3: All are structs are second level
            logging.info("Checking IC-Relational3")
            violations6_3 = structs[~structs["name"].isin(matches6_2["edges_secondhop"])]
            if violations6_3.shape[0] > 0:
                correct = False
                print("IC-Relational3 violation: Some structs are not at the second level")
                display(violations6_1)

        return correct
