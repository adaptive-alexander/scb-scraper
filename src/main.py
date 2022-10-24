import argparse
import sys
import pandas as pd
import pyscbwrapper
import sqlalchemy.engine
from pyscbwrapper import SCB
from datetime import datetime as dt
from time import sleep
import calendar
from sqlalchemy import create_engine


def find_nodes(id_path: str, scb: pyscbwrapper.SCB, res_list: list[dict]):
    """
    Recursively find API leafs
    :param id_path: starting point, full_nav_path
    :param scb: SCB API object
    :param res_list: Result list passed through stack frames
    :return: Returns nothing but by 'reference' alters res_list in place.
    """
    # Go down "l" node (last id in path)
    scb.go_down(id_path.split(".")[-1])
    # Get "l" node info
    info = scb.info()
    ids = []
    for i in info:
        # Add "t" nodes to result list
        if i['type'] == 't':
            res_list.append(
                {
                    "full_nav_path": id_path + "." + i['id'],
                    "description": i["text"]
                })

        # Add additional "l" nodes to iterate
        if i['type'] == 'l':
            ids.append(id_path + "." + i['id'])
    for id_p in ids:
        # Recursively search additional "l" nodes
        find_nodes(id_p, scb, res_list)
    # Go up if node is completed
    scb.go_up()


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


def chunk_card_list(query: dict, card_list: list) -> list[str]:
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

    # Number of the highest cardinality fields to chunk per query
    n = int(70000 / datapoints)

    # Chunk card_list to use for download iterations
    return [card_list[i * n:(i + 1) * n] for i in range((len(card_list) + n - 1) // n)]


def get_table(id_path: str) -> list[dict]:
    """
    Download table from API
    :param id_path:
    :return: list[dict]
    """

    print(f'Started downloading {id_path}')
    scb = SCB('sv')
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

    for i, c in enumerate(chunks):
        if (i % 10 == 0) and (i > 0):
            # At around 10 iterations SCB's rate limiter sets in. Therefore, sleep 15s after 10 iterations
            print("Primitive rate limiter active (every 10 chunks)")
            sleep(15)
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
    elif c.name == "år":
        years = [dt(int(y), 12, calendar.monthrange(y, 12)[1]) for y in c]
        return pd.Series(years)

    # Attempted catch-all
    else:
        try:
            return pd.to_datetime(c)
        except:
            return c


def fix_col_name(s: str):
    return s.replace(" ", "_").replace("/", "_").lower()


def data_to_df(data: list[dict]) -> (pd.DataFrame, list):
    """
    Transform data into pd.DataFrame
    :param data:
    :return: pd.DataFrame
    """
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
    df.columns = [fix_col_name(c) for c in df.columns.tolist()]
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
    return create_engine(connect)


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


def main(start_node: str):
    # Inits
    nodes = []
    scb = SCB('sv')

    # Recursively find leafs in API nodes
    find_nodes(start_node, scb, nodes)

    # Init data list
    data = []

    # Get table data
    for node in nodes:
        # Two retries
        for i in range(3):
            try:
                data.append(get_table(node['full_nav_path']))
                break
            except Exception as e:
                print(f'Failed downloading {node} due to: {e}.')
                sleep(2)
                if i < 2:
                    print(f'Retrying ({i + 1})')
                else:
                    print(f'Could not download {node["full_nav_path"]}.')

    ldf = []
    # Transform table data into [(pd.DataFrame, keys)]
    for dat in data:
        ldf.append(data_to_df(dat))

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

    for i, dfk in enumerate(ldf):
        # Deconstruct DataFrame and keys
        df, keys = dfk

        # Clean table name from node path
        table_name = nodes[i]['full_nav_path'].replace(".", "_")

        # Create table if not exist
        create_table(df, keys, table_name, con)

        # Comparison table
        cdf = pd.read_sql(
            f"SELECT * "
            f"FROM \"{table_name}\";",
            con
        )

        # Check which values are not yet in database
        indicator = df.merge(cdf, on=keys, how="left", indicator=True)['_merge']
        df = df[indicator != "both"]

        # Upload table
        upload_df(con, df, table_name)

        # Upsert metadata (last_updated)
        con.execute(
            f"INSERT INTO metadata (table_name, last_updated) "
            f"VALUES('{table_name}', localtimestamp) "
            f"ON CONFLICT (table_name) "
            f"DO UPDATE SET "
            f"last_updated = localtimestamp "
            f"WHERE metadata.table_name = '{table_name}';"
        )


if __name__ == "__main__":
    # setting up args
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("start_node", help="Path of base node to search")
    args = parser.parse_args()

    if args.start_node == "":
        print(f"Argument start_node cannot be empty.")
        sys.exit(1)

    # Run main
    main(args.start_node)
