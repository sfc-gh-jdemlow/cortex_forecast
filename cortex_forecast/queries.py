from cortex_forecast.connection import SnowparkConnection

def run_query(query):
    session = SnowparkConnection().get_session()
    df = session.sql(query).to_pandas() if session else None
    return df

def run_command(query):
    session = SnowparkConnection().get_session()
    df = session.sql(query).collect() if session else None
    return df
