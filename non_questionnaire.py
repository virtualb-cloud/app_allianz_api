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
        self.mifid1_columns_type = {
            "SOGGETTO" : "float", 
            'ETA' : "float", 
            "ISR" : "float", 
            'OTS' : "float", 
            'VAL_DOMANDA_S4_14_1' : "float", 
            'VAL_DOMANDA_S4_14_2' : "float", 
            'VAL_DOMANDA_S4_14_3' : "float", 
            'VAL_DOMANDA_S4_14_4' : "float",
            'SESSO_B' : "string", 
            'PROV_T' : "string", 
            'PROFILO_SINT_RICHIED_N' : "string", 
            'PROFESSIONE_S' : "string",
            'TAE_T' : "string", 
            'ESPERIENZA_DES' : "string",
            'TIPO_MIFID' : "string", 
            'VAL_DOMANDA_S1_1' : "string", 
            'VAL_DOMANDA_S1_2' : "string",
            'VAL_DOMANDA_S1_3' : "string", 
            'VAL_DOMANDA_S1_4' : "string", 
            'VAL_DOMANDA_S2_5A_1' : "string", 
            'VAL_DOMANDA_S2_5A_2' : "string",
            'VAL_DOMANDA_S2_5A_3' : "string", 
            'VAL_DOMANDA_S2_5A_4' : "string", 
            'VAL_DOMANDA_S2_5A_5' : "string",
            'VAL_DOMANDA_S2_5A_6' : "string", 
            'VAL_DOMANDA_S2_5A_7' : "string",
            'VAL_DOMANDA_S2_5A_8' : "string", 
            'VAL_DOMANDA_S2_5A_9' : "string", 
            'VAL_DOMANDA_S2_5A_10' : "string", 
            'VAL_DOMANDA_S2_5A_11' : "string", 
            'VAL_DOMANDA_S2_5A_12' : "string", 
            'VAL_DOMANDA_S2_5B_1' : "string", 
            'VAL_DOMANDA_S2_5B_2' : "string", 
            'VAL_DOMANDA_S2_5B_3' : "string", 
            'VAL_DOMANDA_S2_5B_4' : "string",
            'VAL_DOMANDA_S2_5B_5' : "string", 
            'VAL_DOMANDA_S2_5B_6' : "string", 
            'VAL_DOMANDA_S2_5B_7' : "string", 
            'VAL_DOMANDA_S2_5B_8' : "string", 
            'VAL_DOMANDA_S2_5B_9' : "string", 
            'VAL_DOMANDA_S2_5B_10' : "string", 
            'VAL_DOMANDA_S2_5B_11' : "string", 
            'VAL_DOMANDA_S2_5B_12' : "string", 
            'VAL_DOMANDA_S2_6_1' : "string", 
            'VAL_DOMANDA_S2_6_2' : "string", 
            'VAL_DOMANDA_S2_6_3' : "string", 
            'VAL_DOMANDA_S2_6_4' : "string", 
            'VAL_DOMANDA_S2_7' : "string", 
            'VAL_DOMANDA_S2_8' : "string",
            'VAL_DOMANDA_S3_9' : "string", 
            'VAL_DOMANDA_S3_10' : "string", 
            'VAL_DOMANDA_S3_11' : "string",
            'VAL_DOMANDA_S3_12' : "string", 
            'VAL_DOMANDA_S4_13_1' : "string", 
            'VAL_DOMANDA_S4_13_2' : "string",
            'VAL_DOMANDA_S4_13_3' : "string", 
            'VAL_DOMANDA_S4_15' : "string", 
            "DATA_INSE_CLI" : "string", 
            "DATA_CENSIMENTO" : "string"
            }

        self.mifid2_columns_type = {
            'NASCITA_FIGLIO_1_MU20' : "integer", 
            'NASCITA_FIGLIO_2_MU20' : "integer",
            'NASCITA_FIGLIO_3_MU20' : "integer", 
            'NASCITA_FIGLIO_4_MU20' : "integer",
            'NASCITA_FIGLIO_5_MU20' : "integer", 
            'NASCITA_FIGLIO_6_MU20' : "integer",
            'VAL_DOMANDA_S2_7_MU20' : "string", 
            'VAL_DOMANDA_S2_8_MU20' : "string", 
            'VAL_DOMANDA_S2_9_MU20' : "string",
            'VAL_DOMANDA_S2_10_MU20' : "string", 
            'VAL_DOMANDA_S4_17_4_MU20' : "string",
            'VAL_DOMANDA_S4_17_5_MU20' : "string", 
            'VAL_DOMANDA_S4_18_MU20' : "string"
            }

        self.mifid3_columns_type = {
            'VAL_DOMANDA_S5_21_MU22' : "string",
            'VAL_DOMANDA_S5_22_MU22' : "string", 
            'VAL_DOMANDA_S5_23_MU22' : "string"
            }

        


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
