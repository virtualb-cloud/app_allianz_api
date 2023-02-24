import pandas as pd
from app.ingestion_advisors import Advisors


def ingestor(df:pd.DataFrame):

    ingestor = Advisors(external_df=df)
    response = ingestor.run()
    if response: return True
    else: return False

