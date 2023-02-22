# Allianz v2.0

import pandas as pd
import json
import requests
import json

class Products:

    def __init__(self) -> None:

        pd.options.mode.chained_assignment = None 
    
    def filter_by_date(self, df:pd.DataFrame):

        column = "data_riferimento"
        df[column] = pd.to_datetime(df[column], dayfirst=True, infer_datetime_format=True)
        ultimate_date = df[column].max()
        df = df[ df[column] == ultimate_date ]

        return df

    def filter_product_identity(self, df:pd.DataFrame):

        columns = ["COD_PROD" , "COD_SOTTOPROD"]

        for column in columns:
            df[column] = df[column].fillna("n")

        df = df.drop_duplicates(subset=columns)
        df.reset_index(inplace=True)

        return df

    def risk_propensity_index(self, df:pd.DataFrame):

        column = "ISR_PRODOTTO"
        df[column] = df[column].apply(lambda x: str(x).replace('.',''))
        df[column] = df[column].apply(lambda x: str(x).replace(',','.'))
        df[column] = df[column].astype(float)
        df[column] = df[column].apply(lambda x: 500 if x > 500 else x)
        df[column] = df[column].apply(lambda x: round(x/500, 2))

        return df

    def financial_litteracy_index(self, df:pd.DataFrame):

        column = "CONOSCENZA_MIFID"
        bins = [0.03, 0.10, 0.17, 0.24, 0.31, 0.38, 0.45, 0.52, 0.59, 0.66, 0.73, 0.80, 0.87, 0.94]

        values = {
            "Q.1" : bins[0],
            "Q.2" : bins[1],
            "Q.3" : bins[2],
            "Q.4" : bins[3],
            "Q.5" : bins[4],
            "Q.6" : bins[5],
            "Q.7" : bins[6],
            "Q.8" : bins[7],
            "Q.9" : bins[8],
            "Q.10" : bins[9],
            "Q.11" : bins[10],
            "Q.12" : bins[11],
            "Q.13" : bins[12],
            "Q.14" : bins[13]
        }
        df[column] = df[column].map(values).fillna(0)

        return df
    
    def financial_experience_index(self, df:pd.DataFrame):

        column = "ESPERIENZA_T"
        bins = [0.167, 0.500, 0.833]

        values = {
            "B" : bins[0],
            "M" : bins[1],
            "A" : bins[2]
        }
        df[column] = df[column].map(values).fillna(0)

        return df

    def needs(self, df:pd.DataFrame):

        column = "Bisogno"
        mappa = {
            "Liquidità" : ["liquidity_investment_need"], 
            "Multiramo (no PRV)" : [], 
            "Accumulo" : ["capital_accumulation_investment_need"],
            "Income" : ["income_investment_need"], 
            "Protezione capitale" : ["capital_protection_investment_need"],
            "Previdenza" : ["retirement_investment_need"],
            "Protezione persona" : ["health_insurance_need", "home_insurance_need"], 
            "LTC" : ["longterm_care_insurance_need"],
            'Income - Liquidità' : ["income_investment_need", "liquidity_investment_need"],
            '0' : []
        }
        our_needs = [
            "capital_accumulation_investment_need",
            "capital_protection_investment_need",
            "liquidity_investment_need",
            "income_investment_need",
            "retirement_investment_need",
            "home_insurance_need",
            "health_insurance_need",
            "longterm_care_insurance_need"
        ]

        # create
        for need in our_needs:
            df[need] = 0.0

        # build
        for idx, row in df.iterrows():
            
            their_element = df.loc[idx, column]

            if their_element in mappa.keys():
                    
                for our_element in mappa[their_element]:

                    df.loc[idx, our_element] = 1
        
        return df

    def insert_db(self, df:pd.DataFrame):

        products = []
        for idx, row in df.iterrows():

            product = {
                "id": f"product_{idx}",
                "description": {
                    "name": df.loc[idx, "COD_SOTTOPROD"],
                    "bloomberg_id": df.loc[idx, "COD_PROD"],
                    "isin_code": df.loc[idx, "COD_ISIN"]
                },
                "cultures": {
                    "financial_experience_index": df.loc[idx, "ESPERIENZA_T"],
                    "financial_litteracy_index": df.loc[idx, "CONOSCENZA_MIFID"],
                    "risk_propensity_index": df.loc[idx, "ISR_PRODOTTO"]
                },
                "needs": {
                    "capital_accumulation_investment_need": df.loc[idx, "capital_accumulation_investment_need"],
                    "capital_protection_investment_need": df.loc[idx, "capital_protection_investment_need"],
                    "liquidity_investment_need": df.loc[idx, "liquidity_investment_need"],
                    "income_investment_need": df.loc[idx, "income_investment_need"],
                    "retirement_investment_need": df.loc[idx, "retirement_investment_need"],
                    "home_insurance_need": df.loc[idx, "home_insurance_need"],
                    "health_insurance_need": df.loc[idx, "health_insurance_need"],
                    "longterm_care_insurance_need": df.loc[idx, "longterm_care_insurance_need"]   
                }
            }
            products.append(product)

        body = json.dumps(products)

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.put(
            url="https://products-dialogue.herokuapp.com/insert_products",
            data=body,
            headers=headers
            )

        return response.text


    def run(self, df:pd.DataFrame):

        df = self.filter_by_date(df=df)
        df = self.filter_product_identity(df=df)
        print("filter done")
        
        df = self.financial_litteracy_index(df=df)
        df = self.financial_experience_index(df=df)
        df = self.risk_propensity_index(df=df)
        print("index done")

        df = self.needs(df=df)
        print("needs done")

        try:
            response = self.insert_db(df=df)
            print(response.text)
            return True
        except:
            return False

       