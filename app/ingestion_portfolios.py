# Allianz v2.0

import pandas as pd
import json
import requests


class Positions:

    def __init__(self) -> None:
        
        headers = {
            "Content-Type": "application/json"
        }
        body = {
            "ids" : [],
            "categories" : ["description"]
        }
        body = json.dumps(body)
        response = requests.put(
            url="https://products-dialogue.herokuapp.com/read_products",
            data=body,
            headers=headers
            )
        
        products = json.loads(response.text)
        
        self.ready_products = {}

        for product in products:
            
            description = {
                "COD_PROD" : product["description"]["name"],
                "COD_SOTTOPROD" : product["description"]["bloomberg_id"]
            }
            self.ready_products[description] = product["id"]

    def links(self, df:pd.DataFrame) -> tuple:
        
        # copy
        df = df.drop_duplicates(subset=["SOGGETTO", "PROMOTORE", "COD_PROD", "COD_SOTTOPROD"])
        df.reset_index(inplace=True)

        portfolios = {}
        positions = {}

        for idx, row in df.iterrows():

            portf = {
                "customer_id" : df.loc[idx, "SOGGETTO"],
                "advisor_id" : df.loc[idx, "PROMOTORE"]
            }

            if not portf in portfolios.keys():
                
                id = f'portfolio_{len(portfolios) + 1}'

                portfolios[portf] = id

            else:

                id = portfolios[portf]
            
            prod = {
                "COD_PROD" : df.loc[idx, "COD_PROD"],
                "COD_SOTTOPROD" : df.loc[idx, "COD_SOTTOPROD"]
            }
            posit = {
                "portfolio_id" : id,
                "product_id" : self.ready_products[prod]
            }

            positions[f'position_{idx + 1}'] = posit

        # change the format
        new_portfolios = []

        for key, value in portfolios:
            new_portfolio = {}
            new_portfolio["id"] = key
            new_portfolio["description"] = value
            new_portfolios.append(new_portfolio)

        new_positions = []

        for key, value in positions:
            new_position = {}
            new_position["id"] = key
            new_position["description"] = value
            new_positions.append(new_position)

        return new_portfolios, new_positions 
            
    def insert_db(self, portfolios:dict, positions:dict):

        # portfolios
        body = json.dumps(portfolios)

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.put(
            url="https://portfolios-dialogue.herokuapp.com/insert_portfolios",
            data=body,
            headers=headers
            )
        print(response.text)

        # positions
        body = json.dumps(positions)

        headers = {
            "Content-Type": "application/json"
        }

        while response.status_code == 422:
            
            response = requests.put(
                url="https://positions-dialogue.herokuapp.com/insert_positions",
                data=body,
                headers=headers
                )
            print(response.text)

        return True
    

    def run(self, df:pd.DataFrame):

        portfolios, positions = self.links(df=df)

        self.insert_db(portfolios=portfolios, positions=positions)

        return True
        