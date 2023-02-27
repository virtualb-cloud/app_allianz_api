import pandas as pd

class Ingestion_controller:

    def __init__(self) -> None:
        
        # columns
        self.expected_columns = {
            "SOGGETTO" : "float",
            "PROMOTORE" : "float",
            "FAMIGLIA" : "string",
            "MACRO_PRODOTTO" : "string",
            "GRUPPO_PRODOTTO" : "string",
            "PRODOTTO" : "string",
            "COD_PROD" : "string",
            "SOTTO_PRODOTTO" : "string",
            "COD_SOTTOPROD" : "string",
            "AUM" : "float",
            "DATA_APERTURA_D" : "string",
            "DATA_CHIUSURA_D" : "string",
            "DATA_RIFERIMENTO" : "string",
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

        