import pandas as pd

class Ingestion_controller:

    def __init__(self) -> None:
        
        # columns
        self.expected_columns = {
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
            "DATA_CENSIMENTO" : "string",
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
            'VAL_DOMANDA_S4_18_MU20' : "string",
            'VAL_DOMANDA_S5_21_MU22' : "string",
            'VAL_DOMANDA_S5_22_MU22' : "string", 
            'VAL_DOMANDA_S5_23_MU22' : "string"
            }


    def run(self, df:pd.DataFrame):
        
        columns = df.columns
        
        flag = True
        errors = ""

        for column in columns:

            if not column in self.expected_columns.keys():
                flag = False
                errors += f"column {column} is not accepted. "

        for column in self.expected_columns.keys():
            if not column in columns:
                flag = False
                errors += f"column {column} is not in the data. "

        return flag, errors

        