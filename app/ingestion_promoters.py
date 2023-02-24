# Allianz v2.0

import json
import numpy as np
import pandas as pd
from pipelines_ingestion.ingestion_utils import Type_Null_controller

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

        # renaming
        promoters_final_naming = {
            'PROMOTORE' : "promoter_deprecated_id", 
            'ETA' : "age", 
            'SESSO_B' : "gender",
            'coret' : "class_coret", 
            'FLAG_AAA' : "class_aaa", 
            'FLAG_WEALTH' : "class_wealth", 
            'FLAG_PRIVATE' : "class_private",
            'QUALIFICA' : "qualification",
            "D_INIZIO_RAPPORTO" : "start_date",
            "D_FINE_RAPPORTO" : "end_date"
        }
        self.ready_df = self.initialized_df.rename(columns=promoters_final_naming)

# final tables

    def hub_promoters(self, last_primary_key:int):

        # copy
        temp_df = self.ready_df

        # drop duplicates on identity
        promoters_identity = [
            'promoter_deprecated_id', 'age', 'gender',
            'class_coret', 'class_aaa', 'class_wealth', 'class_private',
            'qualification', "start_date", "end_date"
        ]
        temp_df = temp_df.drop_duplicates(subset=promoters_identity)
        
        # create primary key
        temp_df = temp_df.reset_index()
        temp_df["promoter_id"] = temp_df.index + last_primary_key + 1

        ##### write the map
        promoters_list = temp_df.to_dict(orient="records")

        mappa = dict()

        # build
        for promoter in promoters_list:
            
            key = tuple()

            for identity in promoter.keys():
                if identity == "promoter_deprecated_id":
                    key = (*key, promoter[identity])

            mappa[str(key)] = promoter["promoter_id"]
        
        # save
        with open("pipelines_ingestion/maps/map_promoter.json", "w") as write_file:
            json.dump(mappa, write_file, indent=4)
        
        # choose the desired columns
        temp_df = temp_df[["promoter_id"] + promoters_identity]

        # set
        temp_df.set_index("promoter_id", inplace=True)
        
        return temp_df
