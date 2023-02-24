import pandas as pd
from app.ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    l1 = round(len(df)/10)

    init = 0
    fin = init + l1

    while fin < len(df):
        
        ingestor = Customers(external_df=df.loc[init:fin, :])
        response = ingestor.run()
        if response: flag =  True
        else: flag = False

        init = fin + 1

    ingestor = Customers(external_df=df.loc[init:, :])
    response = ingestor.run()
    if response: flag =  True
    else: flag = False
