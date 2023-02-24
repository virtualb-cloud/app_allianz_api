import pandas as pd
from ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    ingestor = Customers()
    response = ingestor.run(df=df)
    if response: return True
    else: return False

