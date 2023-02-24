# Allianz v2.0

import json
import requests
import numpy as np
import pandas as pd
from ingestion_utils import Type_Null_controller

class Promoters:

    def __init__(self, external_df:pd.DataFrame) -> None:

        # copy
        self.external_df = external_df

        # columns
        self.columns_expected_type = {
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

        # initialization
        initializer = Type_Null_controller(self.columns_expected_type)
        response = initializer.run(self.external_df)
        if response[0] == False: print(response)
        else: self.initialized_df = response[1]

    def insert_db(self, df:pd.DataFrame):

        advisors = []
        for idx, row in df.iterrows():

            advisor = {
                "id": f"advisor_{idx}",
                "description": {
                    "classification_index": df.loc[idx, "classification_index"],
                    "qualification_index": df.loc[idx, "qualification_index"]
                }
            }
            advisors.append(advisor)

        body = json.dumps(advisors)

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.put(
            url="https://advisors-dialogue.herokuapp.com/insert_advisors",
            data=body,
            headers=headers
            )

        return response.text
    
    def run(self):

        # 1) calculate qualification index

        self.initialized_df["qualification_index"] = self.initialized_df["QUALIFICA"].apply(lambda x: 1 if x == "PROMOTORE" else 0)
        
        # 2) calculate classification index

        bin_size = 3
        bins = [0.167, 0.500, 0.833]

        for idx, row in self.initialized_df.iterrows():
            
            if row["FLAG_AAA"] == 1: self.initialized_df.loc[idx, "classification_index"] = bins[2]

            elif row["FLAG_WEALTH"] == 1: self.initialized_df.loc[idx, "classification_index"] = bins[1]

            elif row["FLAG_PRIVATE"] == 1: self.initialized_df.loc[idx, "classification_index"] = bins[0]
            
            else: self.initialized_df.loc[idx, "classification_index"] = 0

        try:
            response = self.insert_db(df=self.initialized_df)
            print(response)
            return True
        except:
            return False

