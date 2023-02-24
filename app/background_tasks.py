import pandas as pd
from app.ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    l1 = round(df.shape[0]/2)
    ingestor = Customers(external_df=df.loc[0:l1, :])
    response = ingestor.run()
    if response: flag =  True
    else: flag = False

    ingestor = Customers(external_df=df.loc[l1:, :])
    response = ingestor.run()
    if response: flag = True
    else: flag = False

