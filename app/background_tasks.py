import pandas as pd
from app.ingestion_customers import Customers


def ingestor(df:pd.DataFrame):

    ingestor = Customers(external_df=df)
    response = ingestor.run()
    if response: return True
    else: return False

