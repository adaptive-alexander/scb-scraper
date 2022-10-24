import sqlalchemy
import pyscbwrapper
import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime as dt


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


def find_nodes(id_path: str, scb: pyscbwrapper.SCB, res_list: list[dict]):
    """
    Recursively find API leafs
    :param id_path: starting point, full_nav_path
    :param scb: SCB API object
    :param res_list: Result list passed through stack frames
    :return: Returns nothing but by 'reference' alters res_list in place.
    Breaks best practice for more efficiency with a large number of stack frames.
    """
    # Go down "l" node (last id in path)
    scb.go_down(id_path.split(".")[-1])
    # Get "l" node info
    info = scb.info()
    if "error" in info:
        print(f"Error in recursive calls: {info['error']}")
        print(f"Primitive retry (sleep and call again)")
        sleep(10)
        info = scb.info()
    ids = []
    sleep(.1)
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


def try_create_table(con: sqlalchemy.engine.Engine):
    con.execute(
        f"CREATE TABLE IF NOT EXISTS scb_ref ( "
        f"full_nav_path varchar(50) PRIMARY KEY, "
        f"description text not null , "
        f"last_update timestamp null, "
        f"next_update timestamp null"
        f");"
    )


def filter_new_nodes(con: sqlalchemy.engine.Engine, node_df: pd.DataFrame) -> pd.DataFrame:
    current_nodes = pd.read_sql(
        f"SELECT full_nav_path "
        f"FROM scb_ref;",
        con
    )

    # Check which values are not yet in database
    indicator = node_df.merge(current_nodes, on=["full_nav_path"], how="left", indicator=True)['_merge']
    return node_df[indicator != "both"]


def main():
    # Inits
    nodes = []
    scb = pyscbwrapper.SCB('sv')

    # Top nodes from SCB API wrapper
    start_nodes = [d['id'] for d in scb.info()]

    # Recursively find leafs in API nodes
    for start_node in start_nodes:
        print(f"Processing node {start_node}")
        find_nodes(start_node, scb, nodes)
        sleep(10)

    # Cast to pd.DataFrame
    node_df = pd.DataFrame(nodes)

    # Postgres login dict
    # ONLY DEV, REMAKE TO .ENV/KUBERNETES SECRET FOR PRODUCTION
    param_dic = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "glacial",
        "port": "5432",
    }

    # Create sqlalchemy engine
    con = create_sqlalchemy_con(param_dic)

    # Create table if not exists
    try_create_table(con)

    # Offload checking duplicates from database
    print("Checking for duplicates")
    node_df = filter_new_nodes(con, node_df)
    node_df["next_update"] = np.full(node_df.shape[0],dt.utcnow())
    node_df["last_update"] = np.full(node_df.shape[0],dt(1900,1,1))

    # Try uploading node_df
    try:
        print("Uploading new nodes")
        # Append dataframe to table if exists
        node_df.to_sql("scb_ref", con, if_exists="append", index=False)
    except Exception as e:
        # Escalate error
        print(f"Failed due to: {e}")


if __name__ == "__main__":
    main()
