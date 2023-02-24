import pandas as pd
from ingestion_product import Products


def ingestor(df:pd.DataFrame):

    ingestor = Products()
    response = ingestor.run(df=df)
    if response: return True
    else: return False

