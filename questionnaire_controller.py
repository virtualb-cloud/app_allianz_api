# ALLIANZ V2_0

import numpy as np
import pandas as pd


class Type_Null_controller:

    def __init__(self, columns_expected_type:dict) -> tuple:
        
        pass

    def control_keys(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        for column in self.columns_expected_type.keys():

            if not column in local_df.columns: 
                flag = False
                break

        return flag, column

    def control_values_mifid1(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        for column in self.columns_expected_type.keys():

            if not column in local_df.columns: 
                flag = False
                break

        return flag, column
