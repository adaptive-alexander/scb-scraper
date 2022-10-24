import base64
import numpy as np
import pyscbwrapper
import pandas as pd
import calendar
import sqlalchemy
from datetime import datetime as dt
from time import sleep
from google.cloud import pubsub_v1


def get_table_variables(scb: pyscbwrapper.SCB) -> list[dict]:
    """
    Returns table variables at current SCB location
    :param scb:
    :return: list[dict]
    """
    vars = []
    # Iterate variables at current SCB object API location
    for var in scb.info()['variables']:
        # Append variables to return
        vars.append(
            {
                "desc": var['text'],
                "valueTexts": var['valueTexts'],
            })
    return vars


def create_query(vars: list[dict]) -> dict:
    """
    Creates query from variables for API call
    :param vars:
    :return: dict
    """
    query = {}
    # Append variable valueTexts used to query API
    for desc in vars:
        query[f"{desc['desc']}"] = desc['valueTexts']
    return query


def extract_cardinal_key(query: dict) -> (str, list):
    """
    Extract the highest cardinality field from query variables
    :param query:
    :return: (str, list)
    """
    longest = 0
    key = ""
    # Iterate queries to find highest number of unique valueTexts
    for k in query:
        if len(query[k]) > longest:
            longest = len(query[k])
            key = k
    # Return the highest cardinality key valueTexts
    card_list = query[key]
    # Delete from queries
    del query[key]
    return key, card_list


def chunk_card_list(query: dict, card_list: list) -> list[list[str]]:
    """
    Chunk the highest cardinality variable to return a maximum number of datapoints
    :param query:
    :param card_list:
    :return: list[str]
    """
    datapoints = 1
    # Calculate number of datapoints returned without the highest cardinality variable
    for k in query:
        datapoints *= len(query[k])

    # Number of the highest cardinality field items to chunk per query
    n = int(70000 / datapoints)

    # If more datapoints gathered per call
    if n == 0:
        print(f"{datapoints} datapoints without card_key")
        n = 1

    # Chunk card_list to use for download iterations
    chunks =  np.array_split(card_list, len(card_list) // n)

    print(f"{datapoints * len(chunks[0])} datapoints per query.")

    return chunks


def get_table(id_path: str) -> list[dict]:
    """
    Download table from API
    :param id_path:
    :return: list[dict]
    """

    print(f'Started downloading {id_path}')
    scb = pyscbwrapper.SCB('sv')
    # Navigate to path
    for id in id_path.split("."):
        scb.go_down(id)

    # Get variables for path
    vars = get_table_variables(scb)

    # Create query from variables
    query = create_query(vars)

    # Extract the highest cardinality field from query
    card_key, card_list = extract_cardinal_key(query)

    # Dynamically set nr of card_key to use at once
    chunks = chunk_card_list(query, card_list)

    # Iterate card_key to limit data request size
    data = []

    print(f"Processing {len(chunks)} chunks.")
    for i, c in enumerate(chunks):
        if (i % 10 == 0) and (i > 0):
            # At around 10 iterations SCB's rate limiter sets in. Therefore, sleep 15s after 10 iterations
            print("Primitive rate limiter active (every 10 chunks)")
            sleep(5)
        inp_c = {card_key: c}  # Put back card_key and v to single kwarg dict
        scb.set_query(**query, **inp_c)
        data.append(scb.get_data())

    print(f"Successfully downloaded {id_path}")

    # Return download status and list of data
    return data


def try_df_to_dt(c: pd.Series, dts: list[str]) -> pd.Series:
    """
    Attempt to parse dates
    :param c:
    :param dts:
    :return: pd.Series
    """
    # If column does not have SCB type t
    if not c.name in dts:
        return c
    # If specifically "månad"
    if c.name == "månad":
        splits = c.str.split(pat='M')
        splits = splits.apply(lambda x: [int(x[0]), int(x[1])])
        splits = splits.apply(lambda x: dt(x[0], x[1], calendar.monthrange(x[0], x[1])[1]))
        return pd.Series(splits)

    # If specifically "år"
    elif "år" in c.name:
        years = [dt(int(y), 12, calendar.monthrange(y, 12)[1]) for y in c]
        return pd.Series(years)

    # Attempted catch-all
    else:
        try:
            return pd.to_datetime(c)
        except:
            return c


def fix_col_name(s: str):
    return s.replace(" ", "_").replace("/", "_").lower().replace(".", "_").replace(",", "_")


def data_to_df(data: list[dict]) -> (pd.DataFrame, list):
    """
    Transform data into pd.DataFrame
    :param data:
    :return: pd.DataFrame
    """
    if len(data) == 0:
        return None

    # Extract data for later use
    # Extract columns
    cols = [col_li['text'] for col_li in data[0]['columns']]
    # Extract PK
    keys = [fix_col_name(col_li['text']) for col_li in data[0]['columns'] if col_li['type'] != "c"]
    # Extract datetime columns
    dts = [col_li['text'] for col_li in data[0]['columns'] if col_li['type'] == "t"]
    # Extract data dict
    dat = [d['data'] for d in data]

    concat_data = []
    for l in dat:
        for d in l:
            # Concatenate keys and values
            concat_data.append(d['key'] + d['values'])
    df = pd.DataFrame(concat_data, columns=cols)  # Create DataFrame

    # Fix dataframe column names
    df.columns = [fix_col_name(c) for c in df.columns.tolist()]

    # Try automatically fixing data types
    df = (
        df.replace({"..": None})  # Fix ".." as None
        .apply(lambda c: pd.to_numeric(c, errors='ignore') if c.name != "region" else c)  # Try numeric conversion
        .apply(lambda c: try_df_to_dt(c, dts))  # Fix strange month notation to datetime
    )
    return df, keys


def create_sqlalchemy_con(con_params: dict) -> sqlalchemy.engine.Engine:
    """
    Returns sqlalchemy connection
    :param con_params:
    :return: sqlalchemy.engine.Engine
    """
    # Create connection string from con_params dict
    connect = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
        con_params['user'],
        con_params['password'],
        con_params['host'],
        con_params['port'],
        con_params['database']
    )

    # Initialize sqlalchemy engine
    return sqlalchemy.create_engine(connect)


def upload_df(con, df: pd.DataFrame, table: str):
    """
    Upload using sqlalchemy
    :param con:
    :param df:
    :param table:
    :return:
    """

    # Upload dataframe using pandas df.to_sql()
    df.to_sql(table, con, if_exists='append', index=False)


def create_table(df: pd.DataFrame, keys: list[str], table_name: str, con: sqlalchemy.engine.Engine):
    # Manipulate SQL string
    schema = pd.io.sql.get_schema(df, table_name, con=con)
    schema = schema[:14] + "IF NOT EXISTS " + schema[14:-3] + ','
    schema = schema + f"PRIMARY KEY ({', '.join(keys)})" + "\n);"

    # Execute table creation if not exists
    con.execute(schema)


def table_etl(node: str):
    data = []
    # Get table data
    # Two retries
    for i in range(3):
        try:
            data = get_table(node)
            break
        except Exception as e:
            print(f'Failed downloading {node} due to: {e}.')
            sleep((i + 1) ** 2)
            if i < 2:
                print(f'Retrying ({i + 1}/2)')
            else:
                print(f'Could not download {node}.')
                return None

    print("Transforming data")
    dfk = data_to_df(data)
    if dfk is None:
        return None

    # Postgres login dict
    # ONLY DEV, REMAKE TO .ENV/KUBERNETES SECRET FOR PRODUCTION
    param_dic = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "glacial",
        "port": "5432",
    }

    # Initialize sqlalchemy engine
    con = create_sqlalchemy_con(param_dic)

    # Deconstruct DataFrame and keys
    df, keys = dfk

    # Clean table name from node path
    table_name = node.replace(".", "_")

    # Create table if not exist
    print("Create table if not exists")
    create_table(df, keys, table_name, con)

    # Comparison table
    cdf = pd.read_sql(
        f"SELECT * "
        f"FROM \"{table_name}\";",
        con
    )

    # Check which values are not yet in database
    print("Check for duplicates")
    indicator = df.merge(cdf, on=keys, how="left", indicator=True)['_merge']
    df = df[indicator != "both"]
    df = df.drop_duplicates(subset=keys)


    # Upload table
    print("Uploading table data")
    upload_df(con, df, table_name)

    # Upsert metadata (last_updated)
    con.execute(
        f"UPDATE scb_ref "
        f"SET last_update = localtimestamp, "
        f"next_update = localtimestamp + interval '30 day' "
        f"WHERE scb_ref.full_nav_path = '{node}';"
    )
    return "Success"


def sub() -> None:
    """Receives messages from a Pub/Sub subscription."""
    # Initialize a Subscriber client
    subscriber_client = pubsub_v1.SubscriberClient()
    # Create identifier `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber_client.subscription_path("adaptive-alex-cloud", "scb-table-download-sub")

    def callback(message):
        print(f'Processing {message.message_id}.')
        ret = table_etl(base64.b64decode(message.data).decode())

        # Acknowledge the message. Unack'ed messages will be redelivered.
        if ret is None:
            print(
                "Error: Failed downloading table. Acknowledging process to retry at a later time without inserting last_update.")
        message.ack()
        print(f"Acknowledged {message.message_id}.")

    streaming_pull_future = subscriber_client.subscribe(
        subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    try:
        streaming_pull_future.result(timeout=None)
    except Exception as e:
        print(f"Shutting down due to: {e}")
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

    subscriber_client.close()


if __name__ == "__main__":
    sub()
