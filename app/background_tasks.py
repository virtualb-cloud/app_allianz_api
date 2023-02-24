import pandas as pd
from app.ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    ingestor = Customers()
    response = ingestor.run(df=df)
    if response: return True
    else: return False

