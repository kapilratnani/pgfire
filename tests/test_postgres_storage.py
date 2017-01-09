import threading
from contextlib import contextmanager

import sqlalchemy as sa
from pgfire.engine.storage.postgres import PostgresJsonStorage, BaseJsonDb

TEST_DB_NAME = 'test_pgfire'


def get_tainted_db_settings():
    return {
        "db": TEST_DB_NAME,
        "username": "postgres",
        "port": 5432,
        "password": "123456",
        "host": "localhost"
    }


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


def test_pgstorage_init():
    """
    After storage init, the db should have a table named storage_meta
    :return:
    """
    meta_table_name = "storage_meta"
    db_settings = get_tainted_db_settings()
    pgstorage = PostgresJsonStorage(db_settings)
    data = None
    with db_connection() as con:
        result = con.execute("select * from information_schema.tables where table_name='%s'"
                             % meta_table_name)
        data = result.fetchone()
        result.close()
    pgstorage.close()
    assert data


def test_create_db():
    """
    storage will create a table for every Json db
    :return:
    """
    test_table_name = "test_db1"
    db_settings = get_tainted_db_settings()
    pgstorage = PostgresJsonStorage(db_settings)
    jsonDb = pgstorage.create_db(test_table_name)
    assert jsonDb
    assert isinstance(jsonDb, BaseJsonDb)

    data = None
    with db_connection() as con:
        # TODO also check for functions patch_json_data_notify and update_json_data_notify
        result = con.execute("select * from information_schema.tables where table_name='%s'"
                             % test_table_name)
        data = result.fetchone()
        result.close()
    pgstorage.close()
    assert data


def test_delete_db():
    """
    delete the db with name
    :return:
    """
    test_table_name = "test_db2"
    db_settings = get_tainted_db_settings()
    pgstorage = PostgresJsonStorage(db_settings)
    jsonDb = pgstorage.create_db(test_table_name)
    assert jsonDb and isinstance(jsonDb, BaseJsonDb)
    with db_connection() as con:
        result = con.execute("select * from information_schema.tables where table_name='%s'"
                             % test_table_name)
        db_existed = True if result.fetchone() else False
        result.close()

        delete_return = pgstorage.delete_db(test_table_name)
        result = con.execute("select * from information_schema.tables where table_name='%s'"
                             % test_table_name)
        db_deleted = True if result.fetchone() is None else False
        result.close()

    pgstorage.close()
    assert delete_return
    assert db_existed
    assert db_deleted


def test_get_all_dbs():
    """
    return all created dbs
    :return:
    """
    test_table_name1 = "test_db3"
    test_table_name2 = "test_db4"
    db_settings = get_tainted_db_settings()
    pgstorage = PostgresJsonStorage(db_settings)
    with pgstorage:
        jsonDb1 = pgstorage.create_db(test_table_name1)
        assert jsonDb1 and isinstance(jsonDb1, BaseJsonDb)

        jsonDb2 = pgstorage.create_db(test_table_name2)
        assert jsonDb2 and isinstance(jsonDb2, BaseJsonDb)
        dbs = pgstorage.get_all_dbs()
        assert jsonDb1.db_name in dbs
        assert jsonDb2.db_name in dbs


def test_simple_get_put_data_at_path():
    """
    basic data manipulation
    :return:
    """
    test_table_name1 = "test_db5"
    db_settings = get_tainted_db_settings()
    pgstorage = PostgresJsonStorage(db_settings)
    with pgstorage:
        jsonDb1 = pgstorage.create_db(test_table_name1)
        # {"a":{"b":{"c":{"d":1}}}}
        jsonDb1.put('a/b/c', {'d': 1})
        assert jsonDb1.get('a/b/c') == {'d': 1}
        assert jsonDb1.get('a/b') == {'c': {'d': 1}}

        # {"d": 1}
        jsonDb1.put("d", 1)
        assert jsonDb1.get("d") == 1

        # {"e": True}
        jsonDb1.put("e", True)
        assert jsonDb1.get("e")

        # {"f":0.01}
        jsonDb1.put("f", 0.01)
        assert jsonDb1.get("f") == 0.01

        # {"f":{"b":{"c":1.05}}}
        jsonDb1.put("f/b/c", 1.05)
        assert jsonDb1.get("f/b") == {"c": 1.05}

        # {"f":{"b":{"c":1.05}, "d":1.05}}
        jsonDb1.put("f/d", 1.05)
        assert jsonDb1.get("f/d") == 1.05

        # {"f":{"b":1.05, "d":1.05}}
        jsonDb1.put("f/b", 1.05)
        assert jsonDb1.get("f/b") == 1.05
        assert jsonDb1.get("f/d") == 1.05


def test_get_put_post_patch_delete():
    test_db_name = "test_db_fb"
    db_settings = get_tainted_db_settings()
    with PostgresJsonStorage(db_settings) as pg_storage:
        jsonDb = pg_storage.create_db(test_db_name)
        jsonDb.put("rest/saving-data/fireblog/users", {
            "alanisawesome": {
                "name": "Alan Turing",
                "birthday": "June 23, 1912"
            }
        })

        assert jsonDb.get("rest/saving-data/fireblog/users/alanisawesome") == {
            "name": "Alan Turing", "birthday": "June 23, 1912"}

        jsonDb.patch("rest/saving-data/fireblog/users/alanisawesome", {"nickname": "Alan The Machine"})
        assert jsonDb.get("rest/saving-data/fireblog/users/alanisawesome") == {
            "name": "Alan Turing", "birthday": "June 23, 1912", "nickname": "Alan The Machine"}

        posted_data = jsonDb.post("rest/saving-data/fireblog/posts", {"author": "alanisawesome",
                                                                      "title": "The Turing Machine"})

        assert jsonDb.get("rest/saving-data/fireblog/posts/%s" % list(posted_data.keys())[0]) == {
            "author": "alanisawesome",
            "title": "The Turing Machine"
        }

        posted_data = jsonDb.post("rest/saving-data/fireblog/posts", {"author": "gracehopper",
                                                                      "title": "The nano-stick"})
        assert jsonDb.get("rest/saving-data/fireblog/posts/%s" % list(posted_data.keys())[0]) == {
            "author": "gracehopper",
            "title": "The nano-stick"
        }

        assert jsonDb.delete("rest/saving-data/fireblog/users/alanisawesome")
        assert jsonDb.get("rest/saving-data/fireblog/users/alanisawesome") is None


def test_get_db():
    test_db_name = "test_db_fb"
    db_settings = get_tainted_db_settings()
    with PostgresJsonStorage(db_settings) as pg_storage:
        # db exists
        assert pg_storage.get_db(test_db_name)
        # db does not exists
        assert pg_storage.get_db("doesnot_exists") is None


data_received_count1 = 0
data_received_count2 = 0


def test_change_notification():
    """
    set a change notifier at a path and expect a notification on change
    :return:
    """
    test_db_name = "test_db_fb"
    db_settings = get_tainted_db_settings()
    with PostgresJsonStorage(db_settings) as pg_storage:
        notifier = pg_storage.get_notifier(test_db_name)
        message_stream = notifier.listen('rest/saving-data/fireblog1/posts')

        post_data1 = {"t": 1}
        post_data2 = {"t": 2}

        def message_listener():
            global data_received_count1
            for data in message_stream:
                data_received_count1 += 1
                assert data['event'] == 'put'
                assert data['path'].startswith("rest/saving-data/fireblog1/posts")
                assert data['data'] == post_data1 or data['data'] == post_data2

        thr = threading.Thread(target=message_listener)
        thr.setDaemon(True)
        thr.start()

        json_db = pg_storage.get_db(test_db_name)
        import time
        json_db.post('rest/saving-data/fireblog1/posts', post_data1)
        json_db.post('rest/saving-data/fireblog1/posts', post_data2)
        time.sleep(1)

        notifier.hangup('rest/saving-data/fireblog1/posts')
        notifier.cleanup()

        assert data_received_count1 == 2


def test_change_notification2():
    """
    set a change notifier at a path and expect a notification on change
    :return:
    """
    test_db_name = "test_db_fb"
    db_settings = get_tainted_db_settings()
    with PostgresJsonStorage(db_settings) as pg_storage:
        notifier = pg_storage.get_notifier(test_db_name)
        message_stream = notifier.listen('rest/saving-data/fireblog2/posts')

        post_data1 = {"t": 1}
        post_data2 = {"t": 2}

        def message_listener():
            global data_received_count2
            for data in message_stream:
                data_received_count2 += 1
                assert data['event'] == 'put'
                assert data['path'].startswith("rest/saving-data/fireblog2/posts")
                assert data['data'] == post_data1

        thr = threading.Thread(target=message_listener)
        thr.setDaemon(True)
        thr.start()

        import time
        json_db = pg_storage.get_db(test_db_name)
        json_db.post('rest/saving-data/fireblog2/posts', post_data1)
        json_db.post('rest/saving-data/fireblog2/messages', post_data2)

        time.sleep(1)

        notifier.hangup('rest/saving-data/fireblog2/posts')
        notifier.cleanup()

        assert data_received_count2 == 1


def test_create_index():
    """
    create an index on a path in json document, for faster access on those paths.
    :return:
    """
    pass
