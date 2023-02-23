# Allianz v2.0

import json
import numpy as np
import pandas as pd
from scipy.stats import beta
from pipelines_ingestion.questionnaire_contents import Contenter
from pipelines_ingestion.questionnaire_observations import Observer
from pipelines_ingestion.ingestion_utils import Type_Null_controller


class Customers:

    def __init__(self, external_df:pd.DataFrame) -> None:

        # copy
        self.external_df = external_df

        # questionnaire modules initialization
        self.questionnaire_versions = ["MIFID_1_0", "MIFID_2_0", "MIFID_3_0"]
        self.version_column = "TIPO_MIFID"
        self.contenter = Contenter()
        self.observer = Observer()

        # columns



# synthetic variables

    def _ordinal_subjective_risk_index_synthetic(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df

        column = "ISR"

        # pre-fitted beta distribution
        a=9
        b=0.3
        for idx, row in temp_df.iterrows():
            distribution = beta.ppf(temp_df.loc[idx, column], a, b)
            temp_df.loc[idx, column] = np.round(distribution, 4)

        return temp_df[column]

    def _ordinal_financial_experience_index(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df
    
        column = "ESPERIENZA_DES"

        # considering mid-bins
        bin_size = 3
        bins = [0.167, 0.500, 0.833]

        replacements = {
            'ALTA' : bins[0],
            'MEDIA' : bins[1],
            'BASSA' : bins[2]
            }

        return temp_df[column].map(replacements)

    def _ordinal_subjective_risk_index_categorical(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df
   
        column = "PROFILO_SINT_RICHIED_N"

        # considering mid-bins
        bin_size = 5
        bins = [0.1, 0.3, 0.5, 0.7, 0.9]

        replacements = {
            'CONSERVATIVO' : 0,
            'PRUDENTE' : bins[0],
            'EQUILIBRATO' : bins[1],
            'EVOLUTO' : bins[2],
            'DINAMICO' : bins[3],
            }

        return temp_df[column].map(replacements)

    def _ordinal_synthetic_time_horizon(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df

        column = "OTS"

        return temp_df[column]
