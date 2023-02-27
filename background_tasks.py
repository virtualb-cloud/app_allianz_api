import pandas as pd
from app.ingestion_portfolios import portfolios
import time

def ingestor(df:pd.DataFrame):

    l1 = round(len(df)/20)

    init = 0
    fin = init + l1

    while fin < len(df):
        
        ingestor = portfolios(external_df=df.loc[init:fin, :])
        response = ingestor.run()
        if response: flag =  True
        else: flag = False

        init = fin + 1
        fin = init + l1
        time.sleep(5)

    ingestor = portfolios(external_df=df.loc[init:, :])
    response = ingestor.run()
    if response: flag =  True
    else: flag = False
