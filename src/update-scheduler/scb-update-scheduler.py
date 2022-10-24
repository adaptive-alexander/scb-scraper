import base64
import numpy as np
import pandas as pd
import sqlalchemy
from google.cloud import pubsub_v1
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


def get_update_list() -> np.ndarray:
    # Postgres login dict
    # ONLY DEV, REMAKE TO .ENV/KUBERNETES SECRET FOR PRODUCTION
    param_dic = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "glacial",
        "port": "5432",
    }
    con = create_sqlalchemy_con(param_dic)

    df = pd.read_sql(
        f"SELECT full_nav_path "
        f"FROM scb_ref "
        f"WHERE scb_ref.next_update < NOW() "
        f"AND scb_ref.next_update > scb_ref.last_update "
        f"LIMIT 100;",
        con
    )

    return df.full_nav_path.values


def pub(message: str) -> None:
    """Publishes a message to a Pub/Sub topic."""
    # Initialize a Publisher client.
    client = pubsub_v1.PublisherClient()
    # Create identifier `projects/{project_id}/topics/{topic_id}`
    topic_path = client.topic_path("adaptive-alex-cloud", "scb-table-download")

    # Data sent to Cloud Pub/Sub
    data = base64.b64encode(message.encode())

    # Returns a future.
    api_future = client.publish(topic_path, data)
    message_id = api_future.result()

    print(f"Published {message} to {topic_path}: {message_id}")


def main():
    updates = get_update_list()
    for updt in updates[5:6]:
        pub(updt)


if __name__ == "__main__":
    main()
