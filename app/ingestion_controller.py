import pandas as pd


class Ingestion_controller:

    def __init__(self) -> None:
        
        # columns
        self.expected_columns = {
            "FAMIGLIA" : "string",
            "MACRO_PRODOTTO" : "string",
            "GRUPPO_PRODOTTO" : "string",
            "PRODOTTO" : "string",
            "SOTTO_PRODOTTO" : "string",
            "COD_PROD" : "string",
            "COD_SOTTOPROD" : "string",
            "Tipologia" : "string",
            "Sottotipologia" : "string",
            
            "COD_ISIN" : "string",
            "ESPERIENZA_T" : "string",
            "CONOSCENZA_MIFID" : "string",
            "Sottotipologia" : "string",
            "Bisogno" : "string",
            "ISR_PRODOTTO" : "float",
            "CODICE_AA" : "string",

            "PESO" : "float",
            "data_riferimento" : "string",
            #"ART_6_8_9" : "string"
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

        