from contextlib import contextmanager

import requests
import sqlalchemy as sa
from pgfire.conf import conf

TEST_DB_NAME = 'test_rest_pgfire'


def get_tainted_db_settings():
    conf['db']['db'] = TEST_DB_NAME
    return conf['db']


@contextmanager
def db_connection(db_name=TEST_DB_NAME):
    # init module variables
    DB_PROPS = get_tainted_db_settings()
    DB_HOST = DB_PROPS.get("host")
    DB_PORT = DB_PROPS.get("port")
    DB_USER = DB_PROPS.get("username")
    DB_PASSWORD = DB_PROPS.get("password")

    connection_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(DB_USER,
                                                                      DB_PASSWORD,
                                                                      DB_HOST,
                                                                      DB_PORT,
                                                                      db_name)

    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    yield conn
    conn.close()
    engine.dispose()


def setup_module(module):
    with db_connection('') as conn:
        conn = conn.execution_options(autocommit=False)
        conn.execute("ROLLBACK")
        try:
            conn.execute("DROP DATABASE %s" % TEST_DB_NAME)
        except sa.exc.ProgrammingError as e:
            # Could not drop the database, probably does not exist
            conn.execute("ROLLBACK")
        except sa.exc.OperationalError as e:
            # Could not drop database because it's being accessed by other users (psql prompt open?)
            conn.execute("ROLLBACK")

        conn.execute("CREATE DATABASE %s" % TEST_DB_NAME)


def teardown_module(module):
    pass


def test_create_json_db():
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": "a_json_db"
    }
    response = requests.post(url=url, json=data)
    assert response.status_code == 204
    # create the same app again
    response = requests.post(url=url, json=data)
    assert response.status_code == 400
    assert response.json().get('reason') == "db with the same name already exists"


def test_get_put_post_delete_from_app():
    # create json db
    json_db_name = "a_json_db_1"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok()

    path = "rest/saving-data/fireblog/users"
    data = {
        "alanisawesome": {
            "name": "Alan Turing",
            "birthday": "June 23, 1912"
        }
    }

    # put data
    url = 'http://localhost:8666/database/%s/%s'
    response = requests.put(url=url % (json_db_name, path), json=data)
    assert response.ok()
    assert response.json() == data

    # get data
    response = requests.get(url=url % (json_db_name, "rest/saving-data/fireblog"))
    assert response.ok()
    assert response.json() == {"users": {"alanisawesome": {"name": "Alan Turing", "birthday": "June 23, 1312"}}}

    # patch data
    path = "rest/saving-data/fireblog/users/alanisawesome"
    data = {"nickname": "Alan The Machine"}
    response = requests.patch(url=url % (json_db_name, path), json=data)
    assert response.ok()
    assert response.json() == data

    # post data
    path = "rest/saving-data/fireblog/posts"
    data = {"author": "gracehopper", "title": "The nano-stick"}
    response = requests.post(url=url % (json_db_name, path), json=data)
    assert response.ok()
    assert "name" in response.json()

    # delete data
    path = "rest/saving-data/fireblog/users/alanisawesome"
    response = requests.delete(url=url % (json_db_name, path))
    assert response.ok()

    response = requests.get(url=url % (json_db_name, path))
    assert response.ok()
    assert response.json() == {}


def test_delete_json_db():
    # create json db
    json_db_name = "a_json_db_2"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok()
    url = 'http://localhost:8666/deletedb'
    response = requests.delete(url=url, json=data)
    assert response.ok()


data_received_count1 = 0


def test_eventsource_api():
    # create json db
    json_db_name = "a_json_db_2"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok()

    import threading

    url = 'http://localhost:8666/database/%s/%s'
    post_data1 = {"t": 1}
    post_data2 = {"t": 2}
    listen_path = 'rest/saving-data/fireblog1/posts'

    def message_listener():
        global data_received_count1
        from sseclient import SSEClient
        sse = SSEClient(url % (json_db_name, listen_path))
        for msg in sse:
            data_received_count1 += 1
            print(msg)

    thr = threading.Thread(target=message_listener)
    thr.setDaemon(True)
    thr.start()

    # write data
    import time
    requests.post(url=url % (json_db_name, listen_path), json=post_data1)
    requests.post(url=url % (json_db_name, listen_path), json=post_data2)

    time.sleep(1)

    assert data_received_count1 == 2
