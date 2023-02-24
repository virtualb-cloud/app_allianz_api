import pandas as pd
from app.ingestion_advisors import Advisors


def ingestor(df:pd.DataFrame):

    ingestor = Advisors()
    response = ingestor.run(df=df)
    if response: return True
    else: return False

