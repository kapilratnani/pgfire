from typing import Union, List

from ..utils import PushID

post_push_id = PushID()

JSON_PRIMITIVES = Union[int, float, bool, dict, str, None]


class BaseJsonDb(object):
    """
        Helper class to get and set data from underlying json storage
    """

    def __init__(self, db_name: str, storage: 'BaseJsonStorage'):
        self.db_name = db_name
        self.storage = storage

    def get(self, path: str = None) -> JSON_PRIMITIVES:
        return self.storage.get_from_path(self.db_name, path)

    def put(self, path: str, value: JSON_PRIMITIVES) -> JSON_PRIMITIVES:
        return self.storage.put_at_path(self.db_name, path, value)

    def post(self, path: str, value: JSON_PRIMITIVES) -> JSON_PRIMITIVES:
        return self.storage.post_at_path(self.db_name, path, value)

    def patch(self, path: str, value: JSON_PRIMITIVES) -> JSON_PRIMITIVES:
        return self.storage.patch_at_path(self.db_name, path, value)

    def delete(self, path: str) -> bool:
        return self.storage.delete_at_path(self.db_name, path)


class BaseJsonChangeNotifier(object):
    """
        Represents the notification infra, implemented by underlying storage
    """

    def __init__(self, db_name: str, path: str):
        self.db = db_name
        self.path = path

    def __enter__(self):
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    async def __aenter__(self):
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    def listen(self):
        raise NotImplementedError()

    def cleanup(self):
        raise NotImplementedError()


class BaseJsonStorage(object):
    """
        Base class for all Json Storages
    """
    vendor = 'unknown'

    def __init__(self, storage_settings: dict):
        self.storage_settings = storage_settings

    def get_from_path(self, db_name: str, path: str) -> JSON_PRIMITIVES:
        raise NotImplementedError()

    def set_at_path(self, db_name: str,
                    path: str,
                    value: JSON_PRIMITIVES,
                    op_type: str = 'put'  # 'put', 'post', or 'patch'
                    ) -> JSON_PRIMITIVES:
        raise NotImplementedError()

    def put_at_path(self, db_name: str, path: str,
                    value: JSON_PRIMITIVES,
                    ) -> JSON_PRIMITIVES:
        return self.set_at_path(db_name, path, value, 'put')

    def post_at_path(self, db_name: str, path: str,
                     value: JSON_PRIMITIVES) -> JSON_PRIMITIVES:
        # attach push_id to path
        posted_data = {}
        push_id = post_push_id.next_id()
        new_path = "%s/%s" % (path, push_id)
        posted_data[push_id] = self.set_at_path(db_name, new_path, value, 'post')
        return posted_data

    def patch_at_path(self, db_name: str, path: str,
                      value: JSON_PRIMITIVES) -> JSON_PRIMITIVES:
        return self.set_at_path(db_name, path, value, 'patch')

    def delete_at_path(self, db_name: str, path: str) -> bool:
        raise NotImplementedError()

    def optimize(self, db_name: str):
        raise NotImplementedError()

    def create_db(self, db_name: str) -> BaseJsonDb:
        raise NotImplementedError()

    def delete_db(self, db_name: str) -> bool:
        raise NotImplementedError()

    def get_db(self, db_name: str) -> BaseJsonDb:
        raise NotImplementedError()

    def get_notifier(self, db_name: str, path: str) -> BaseJsonChangeNotifier:
        raise NotImplementedError()

    def get_all_dbs(self) -> List[str]:
        raise NotImplementedError()

    def create_index(self, db_name, path):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()
