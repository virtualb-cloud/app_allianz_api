# Allianz v2.0

import numpy as np
import pandas as pd
import json
from ingestion_utils import Type_Null_controller

class Positions:

    def __init__(self, external_df:pd.DataFrame) -> None:

        # copy
        self.external_df = external_df
        
        # columns
        self.columns_expected_type = {
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
    
        # initialization
        initializer = Type_Null_controller(self.columns_expected_type)
        response = initializer.run(self.external_df)
        if response[0] == False: print(response)
        else: self.initialized_df = response[1]


        # renaming
        positions_final_naming = {
            "SOGGETTO" : "customer_deprecated_id",
            "PROMOTORE" : "promoter_deprecated_id",
            "FAMIGLIA" : "product_family",
            "MACRO_PRODOTTO" : "product_macro",
            "GRUPPO_PRODOTTO" : "product_group",
            "PRODOTTO" : "product_name",
            "COD_PROD" : "product_code",
            "SOTTO_PRODOTTO" : "subproduct_name",
            "COD_SOTTOPROD" : "subproduct_code",
            "AUM" : "aum",
            "DATA_APERTURA_D" : "opening_date",
            "DATA_CHIUSURA_D" : "closing_date",
            "DATA_RIFERIMENTO" : "aum_reference_date"
            }
        self.ready_df = self.initialized_df.rename(columns=positions_final_naming)

# final tables

    def hub_portfolio(self, last_primary_key:int) -> dict:
        
        # copy
        temp_df = self.ready_df
        
        # prepare
        portfolios_identity = [
            "customer_deprecated_id", 'promoter_deprecated_id'
        ]
        temp_df = temp_df.drop_duplicates(portfolios_identity)
        
        # create primary key
        temp_df = temp_df.reset_index()
        temp_df["portfolio_id"] = temp_df.index + last_primary_key + 1

        # create foreign keys
        mappa_customer = self.read_map_customer()
        mappa_promoter = self.read_map_promoter()

        for idx, row in temp_df.iterrows():
            
            key_customer = tuple()
            key_promoter = tuple()

            # make the key
            key_customer = (*key_customer, row["customer_deprecated_id"])
            key_promoter = (*key_promoter, row["promoter_deprecated_id"])

            # ask the id
            temp_df.loc[idx, "customer_id"] = mappa_customer[str(key_customer)] if str(key_customer) in mappa_customer.keys() else np.nan
            temp_df.loc[idx, "promoter_id"] = mappa_promoter[str(key_promoter)] if str(key_promoter) in mappa_promoter.keys() else np.nan

        # to dictionary
        portfolios_list = temp_df.to_dict(orient="records")

        # write map
        mappa = dict()
        for portfolio in portfolios_list:
            
            key = tuple()

            for identity in portfolio.keys():
                if identity in ["customer_id", "promoter_id"]:
                    key = (*key, portfolio[identity])

            mappa[str(key)] = portfolio["portfolio_id"]
        
        # save the map
        json.dump(mappa, open("pipelines_ingestion/maps/map_portfolio.json", "w"), indent=4)

        # choose the desired columns
        temp_df = temp_df[["portfolio_id", "customer_id", "promoter_id"]]

        temp_df.set_index("portfolio_id", inplace=True)

        return temp_df

    def hub_positions(self, last_primary_key:int):

        # copy
        temp_df = self.ready_df

        # prepare
        positions_identity = [
            "customer_deprecated_id", 'promoter_deprecated_id', 'product_family', 'product_macro', 'product_group',
            'product_name', 'product_code', 'subproduct_name', 'subproduct_code', 'aum', 
            'opening_date', 'closing_date', "aum_reference_date"
        ]
        temp_df = temp_df.drop_duplicates(subset=positions_identity)
        
        # create primary key
        temp_df = temp_df.reset_index()
        temp_df["position_id"] = temp_df.index + last_primary_key + 1
        
        # create foreign keys
        mappa_product = self.read_map_product()
        mappa_portfolio = self.read_map_portfolio()
        mappa_customer = self.read_map_customer()
        mappa_promoter = self.read_map_promoter()

        for idx, row in temp_df.iterrows():
            
        # customer
        
            customer_key = tuple()
            customer_key = (*customer_key, row["customer_deprecated_id"])
            temp_df.loc[idx, "customer_id"] = mappa_customer[str(customer_key)] if str(customer_key) in mappa_customer.keys() else np.nan

        # promoter

            promoter_key = tuple()
            promoter_key = (*promoter_key, row["promoter_deprecated_id"])
            temp_df.loc[idx, "promoter_id"] = mappa_promoter[str(promoter_key)] if str(promoter_key) in mappa_promoter.keys() else np.nan

        # product

            product_key = tuple()
            identity = ['product_family', 'product_macro', 'product_group',
                    'product_name', 'product_code', 'subproduct_name', 'subproduct_code']
            for col in identity:
                product_key = (*product_key, row[col])
            temp_df.loc[idx, "product_id"] = mappa_product[str(product_key)] if str(product_key) in mappa_product.keys() else np.nan


        for idx, row in temp_df.iterrows():
        
        # portfolio

            portfolio_key = tuple()
            identity = ["customer_id", 'promoter_id']
            for col in identity:
                portfolio_key = (*portfolio_key, row[col])     
            temp_df.loc[idx, "portfolio_id"] = mappa_portfolio[str(portfolio_key)] if str(portfolio_key) in mappa_portfolio.keys() else np.nan


        ##### write map
        positions_list = temp_df.to_dict(orient="records")

        mappa = dict()

        ##### build
        for position in positions_list:
            
            key = tuple()

            for identity in position.keys():
                if identity in ["customer_id", "product_id", "opening_date", "closing_date"]:
                    key = (*key, position[identity])

            mappa[str(key)] = position["position_id"]
        
        # save
        json.dump(mappa, open("pipelines_ingestion/maps/map_position.json", "w"), indent=4)
        

        # choose the desired columns
        temp_df = temp_df[["position_id", "portfolio_id", "product_id", "opening_date", "closing_date", "aum", "aum_reference_date"]]

        # set
        temp_df.set_index("position_id", inplace=True)

        return temp_df
