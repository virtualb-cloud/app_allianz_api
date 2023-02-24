import pandas as pd


class Ingestion_controller:

    def __init__(self) -> None:
        
        # columns
        self.expected_columns = {
            "PROMOTORE" : "float",
            "ETA" : "integer",
            "SESSO_B" : "string",
            "coret" : "string",
            "FLAG_AAA" : "integer",
            "FLAG_WEALTH" : "integer",
            "FLAG_PRIVATE" : "integer",
            "QUALIFICA" : "string",
            "D_INIZIO_RAPPORTO" : "string",
            "D_FINE_RAPPORTO" : "string",
            "DATA_INIZIO_PRIVATE" : "string",
            "DATA_FINE_PRIVATE" : "string"
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

        