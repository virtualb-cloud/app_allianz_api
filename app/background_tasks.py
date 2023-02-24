import pandas as pd
from app.ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    l1 = round(len(df)/10)

    init = 0
    while (init + l1) < len(df):
        
        ingestor = Customers(external_df=df.loc[init:init+l1, :])
        response = ingestor.run()
        if response: flag =  True
        else: flag = False

        init = init+l1

    ingestor = Customers(external_df=df.loc[init:, :])
    response = ingestor.run()
    if response: flag =  True
    else: flag = False
