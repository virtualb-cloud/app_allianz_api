import pandas as pd
from app.ingestion_portfolios import Positions
import time

def ingestor(df:pd.DataFrame):

    l1 = round(len(df)/20)

    init = 0
    fin = init + l1

    while fin < len(df):
        
        ingestor = Positions()
        response = ingestor.run(df=df.loc[init:fin, :])
        if response: flag =  True
        else: flag = False

        init = fin + 1
        fin = init + l1
        time.sleep(5)

    ingestor = Positions()
    response = ingestor.run(df=df.loc[init:, :])
    if response: flag =  True
    else: flag = False
