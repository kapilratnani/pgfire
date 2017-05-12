import json
import os
import queue

import psycopg2
import sqlalchemy
from sqlalchemy import exists
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import DDL

from .models import *
from ..base import *
from ..utils import read_file

UPSERT_FUNCTION_FILE = os.path.join(os.path.dirname(__file__), 'upsert_json_data_notify.sql')
PATCH_FUNCTION_FILE = os.path.join(os.path.dirname(__file__), "patch_json_data_notify.sql")
JSONB_DEEP_SET_FUNCTION_FILE = os.path.join(os.path.dirname(__file__), "jsonb_set_deep.sql")


def _build_path_query(path):
    """
    converts path in url to path required in query.
    :param path: like 'a/b/c/d'
    :return: ('a', ('a','b','c','d'), '{a,b,c,d}')
    """
    if not path:
        return None

    sp = path.split('/')
    if len(sp) == 1:
        return sp[0], sp[0], '{' + ','.join(sp) + '}'
    else:
        return sp[0], tuple(sp), '{' + ','.join(sp) + '}'


def _construct_data(path, value):
    d = {}
    if len(path) == 1:
        d[path[0]] = value
        return d
    elif len(path) > 1:
        d[path[0]] = _construct_data(path[1:], value)
        return d
    else:
        return value


class PostgresJsonStorage(BaseJsonStorage):
    vendor = "postgresql"

    def __init__(self, storage_settings: dict):
        super().__init__(storage_settings)
        self.closed = False
        self.json_db_instance_cache = {}
        self.__db_init()
        self.notifiers = []

    def __check_closed(self):
        if self.closed:
            raise ValueError('Storage already closed')

    def __db_init(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        settings = self.storage_settings
        db_name = settings.get("db")
        db_host = settings.get("host")
        port = settings.get("port")
        user = settings.get("username")
        password = settings.get("password")

        connection_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user,
                                                                          password,
                                                                          db_host,
                                                                          port,
                                                                          db_name)

        engine = create_engine(connection_string)
        self.engine = engine
        session_maker = sessionmaker()
        session_maker.configure(bind=engine)

        sqlalchemy.event.listen(
            Base.metadata,
            'after_create',
            DDL(read_file(JSONB_DEEP_SET_FUNCTION_FILE))
        )

        sqlalchemy.event.listen(
            Base.metadata,
            'after_create',
            DDL(read_file(UPSERT_FUNCTION_FILE))
        )

        sqlalchemy.event.listen(
            Base.metadata,
            'after_create',
            DDL(read_file(PATCH_FUNCTION_FILE))
        )

        Base.metadata.create_all(engine)

        self.session = session_maker()  # type: Session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self

    def set_at_path(self, db_name: str,
                    path: str,
                    value: JSON_PRIMITIVES, op_type: str = 'put') -> JSON_PRIMITIVES:
        self.__check_closed()
        session = self.session
        l1_key, path_query, write_path = _build_path_query(path)

        if op_type == 'patch':
            func_call = sqlalchemy.func.patch_json_data_notify(
                db_name,
                l1_key,
                json.dumps(_construct_data(path.split('/'), value)),
                write_path,
                json.dumps(value)
            )
        else:
            func_call = sqlalchemy.func.upsert_json_data_notify(
                db_name,
                l1_key,
                json.dumps(_construct_data(path.split('/'), value)),
                write_path,
                json.dumps(value)
            )

        session.execute(func_call)
        session.commit()
        return value

    def __check_db_exists(self, db_name: str) -> bool:
        return self.session.query(exists().where(StorageMeta.db_name == db_name)).scalar()

    def get_db(self, db_name: str) -> BaseJsonDb:
        self.__check_closed()
        if db_name in self.json_db_instance_cache:
            return self.json_db_instance_cache[db_name]
        elif self.__check_db_exists(db_name):
            db = BaseJsonDb(db_name, self)
            self.json_db_instance_cache[db_name] = db
            return db

    def get_from_path(self, db_name: str, path: str) -> JSON_PRIMITIVES:
        """
        1. get the app class from models
        2. extract base_key or l1_key from path and create path query
        :param db_name:
        :param path:
        :return:
        """
        self.__check_closed()
        session = self.session
        cls = get_json_db_cls(db_name)

        if path is None:
            # return all data
            return self.__get_all_data(cls)

        l1_key, path_query, _ = _build_path_query(path)
        if not path_query:
            raise ValueError("Invalid path")

        return session.query(cls.data[path_query]).filter(cls.l1_key == l1_key).scalar()

    def __get_all_data(self, cls):
        all_data = {}
        for row in self.session.query(cls.data).all():
            all_data.update(row[0])
        return all_data

    def delete_db(self, db_name: str) -> bool:
        self.__check_closed()
        remove_json_db_table(db_name, self.session)
        return True

    def create_db(self, db_name: str) -> BaseJsonDb:
        self.__check_closed()
        create_json_db_table(db_name, self.session)
        db = BaseJsonDb(db_name, self)
        self.json_db_instance_cache[db_name] = db
        return db

    def delete_at_path(self, db_name: str, path: str) -> bool:
        self.__check_closed()
        self.set_at_path(db_name, path, None)
        return True

    def get_all_dbs(self) -> List[str]:
        self.__check_closed()
        all_dbs = self.session.query(StorageMeta).all()
        return [it.db_name for it in all_dbs]

    def __new_pg_connection(self):
        import psycopg2
        return psycopg2.connect(database=self.storage_settings['db'],
                                user=self.storage_settings['username'],
                                password=self.storage_settings['password'],
                                host=self.storage_settings['host'],
                                port=self.storage_settings['port']
                                )

    def get_notifier(self, db_name: str, path: str) -> BaseJsonChangeNotifier:
        notifier = ThreadSafeJsonChangeNotifier(db_name, path, self.__new_pg_connection())
        self.notifiers.append(notifier)
        return notifier

    def create_index(self, db_name, path):
        pass

    def optimize(self, db_name: str):
        pass

    def close(self):
        map(lambda x: x.cleanup(), self.notifiers)
        self.session.close()
        self.closed = True


def queue_to_generator(q):
    while True:
        try:
            data = q.get_nowait()
            if data is None:
                raise StopIteration()
            q.task_done()
            yield data
        except queue.Empty:
            yield None


def path_filter(path, gen):
    for payload in gen:
        if payload:
            if payload['path'].startswith(path):
                yield payload
            else:
                continue
        else:
            yield payload


class ThreadSafeJsonChangeNotifier(BaseJsonChangeNotifier):
    def __init__(self, db_name: str, path: str, conn):
        super().__init__(db_name, path)
        self.conn = conn
        self.listen_thread = None  # type : threading.Thread
        self.__thread_kill = False
        self.message_queue = queue.Queue()

    def __prepare_listen_thread(self):
        import threading
        if self.listen_thread is None:
            self.__thread_kill = False
            self.listen_thread = threading.Thread(target=self.__listen)
            self.listen_thread.start()

    def __listen(self):
        import select

        conn = self.conn
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("LISTEN %s;" % self.db)
        try:
            while not self.__thread_kill:
                if select.select([conn], [], [], 1) == ([], [], []):
                    continue
                else:
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop()
                        print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                        payload = json.loads(notify.payload)
                        payload['path'] = '/'.join(payload['path'])
                        self.message_queue.put(payload)
        finally:
            if cursor:
                cursor.close()
        self.message_queue.put(None)

    def __close_listen_thread(self):
        if not self.__thread_kill:
            self.__thread_kill = True
            self.listen_thread.join()

    def listen(self):
        print("start notification thread for path:%s" % self.path)
        self.__prepare_listen_thread()
        print("Thread id:%s" % self.listen_thread.ident)
        return path_filter(self.path, queue_to_generator(self.message_queue))

    def cleanup(self):
        print("stop notification thread for path:%s" % self.path)
        print("Thread id:%s" % self.listen_thread.ident)
        self.__close_listen_thread()
        self.conn.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
