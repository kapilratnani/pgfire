from contextlib import contextmanager

import requests
import sqlalchemy as sa
from sqlalchemy import exc

TEST_DB_NAME = 'test_rest_pgfire'


def get_test_config():
    return {
        "db": {
            "db": TEST_DB_NAME,
            "username": "postgres",
            "port": 5432,
            "password": "123456",
            "host": "localhost"
        }
    }


@contextmanager
def db_connection():
    db_props = get_test_config()
    db_host = db_props.get("db").get("host")
    db_port = db_props.get("db").get("port")
    db_user = db_props.get("db").get("username")
    db_password = db_props.get("db").get("password")
    db_name = ''

    connection_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_user,
                                                                      db_password,
                                                                      db_host,
                                                                      db_port,
                                                                      db_name)

    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    yield conn
    conn.close()
    engine.dispose()


def setup_module(module):
    with db_connection() as conn:
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
    start_app()


process = None


def teardown_module(module):
    stop_app()


def stop_app():
    global process
    process.terminate()
    process.join()


def __start_app():
    from app import prepare_app
    from aiohttp import web
    app = prepare_app()
    # override test config
    app['config'] = get_test_config()
    web.run_app(app, host="localhost", port=8666)


def start_app():
    global process
    from multiprocessing import Process
    import time

    process = Process(target=__start_app)
    process.start()
    time.sleep(2)


def test_create_json_db():
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": "a_json_db"
    }
    response = requests.post(url=url, json=data)
    assert response.ok
    # # create the same db again
    # response = requests.post(url=url, json=data)
    # assert response.status_code == 400


def test_get_put_post_delete_from_app():
    # create json db
    json_db_name = "a_json_db_1"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok

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
    assert response.ok
    assert response.json() == data

    # get data
    response = requests.get(url=url % (json_db_name, "rest/saving-data/fireblog"))
    assert response.ok
    d = response.json()
    assert response.json() == {"users": {"alanisawesome": {"name": "Alan Turing", "birthday": "June 23, 1912"}}}

    # patch data
    path = "rest/saving-data/fireblog/users/alanisawesome"
    data = {"nickname": "Alan The Machine"}
    response = requests.patch(url=url % (json_db_name, path), json=data)
    assert response.ok
    assert response.json() == data

    # post data
    path = "rest/saving-data/fireblog/posts"
    data = {"author": "gracehopper", "title": "The nano-stick"}
    response = requests.post(url=url % (json_db_name, path), json=data)
    assert response.ok
    posted_data = response.json()

    assert requests.get(url=url % (
        json_db_name,
        "rest/saving-data/fireblog/posts/%s" % list(posted_data.keys())[0])
                        ).json() == data

    # delete data
    path = "rest/saving-data/fireblog/users/alanisawesome"
    response = requests.delete(url=url % (json_db_name, path))
    assert response.ok

    response = requests.get(url=url % (json_db_name, path))
    assert response.ok


def test_delete_json_db():
    # create json db
    json_db_name = "a_json_db_2"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok
    url = 'http://localhost:8666/deletedb'
    response = requests.delete(url=url, json=data)
    assert response.ok


data_received_count1 = 0


def test_eventsource_api():
    # create json db
    json_db_name = "a_json_db_3"
    url = 'http://localhost:8666/createdb'
    data = {
        "db_name": json_db_name
    }

    response = requests.post(url=url, json=data)
    assert response.ok

    import threading

    url_event = 'http://localhost:8666/database_events/%s/%s'
    url = 'http://localhost:8666/database/%s/%s'

    post_data1 = {"t": 1}
    post_data2 = {"t": 2}
    post_data3 = {"t": 3}

    listen_path = 'rest/saving-data/fireblog1/posts'

    requests.post(url=url % (json_db_name, listen_path), json=post_data1)

    from sseclient import SSEClient
    sse = SSEClient(url_event % (json_db_name, listen_path))

    def message_listener():
        global data_received_count1
        for msg in sse:
            data_received_count1 += 1
            print("SSE:" + str(msg))

    thr = threading.Thread(target=message_listener)
    thr.setDaemon(True)
    thr.start()

    # write data
    import time
    time.sleep(5)
    requests.post(url=url % (json_db_name, listen_path), json=post_data2)
    requests.post(url=url % (json_db_name, listen_path), json=post_data3)

    time.sleep(5)
    assert data_received_count1 == 3
