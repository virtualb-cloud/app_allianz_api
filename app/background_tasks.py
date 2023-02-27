import pandas as pd
from app.ingestion_portfolios import Positions
import time

def ingestor(df:pd.DataFrame):

    ingestor = Positions()
    response = ingestor.run(df=df)
    if response: flag =  True
    else: flag = False
