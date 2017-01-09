from sqlalchemy import Column, DateTime, String, Integer, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from ..utils import session_scope

JSON_DB_CLS = {}

__all__ = ["Base",
           "StorageMeta", "create_json_db_table", "get_json_db_cls", "remove_json_db_table"]

Base = declarative_base()


class StorageMeta(Base):
    """
        Holds metadata or stats of each json db
    """
    __tablename__ = "storage_meta"
    id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(255), unique=True, nullable=False)
    created = Column(DateTime, default=func.now())

    def __repr__(self):
        return '({}, {})'.format(self.id, self.db_name)


class BaseJsonDbTable(Base):
    """
        {
            "a":1,
            "b":{"u":"c"},
            "d":{"a":true}
        }
        The above will map to db as follows
        l1_key     data
        a          {"a":1}
        b          {"b":{"u":"c"}}
        d          {"d":{"a":true}}

        the l1_key is for quick access to first level siblings.
        When asked for full data, individual JSONs will be merged
    """
    __abstract__ = True
    # top level key
    l1_key = Column(String(255), primary_key=True)
    # all data will go here
    data = Column(JSONB)

    created = Column(DateTime, default=func.now())
    last_modified = Column(DateTime, default=func.now(), onupdate=func.now())


def get_json_db_cls(db_name: str) -> BaseJsonDbTable:
    if db_name in JSON_DB_CLS:
        return JSON_DB_CLS[db_name]

    class_name = db_name
    table_name = db_name
    cls = type(class_name, (BaseJsonDbTable,), {"__tablename__": table_name})
    JSON_DB_CLS[db_name] = cls
    return cls


def create_json_db_table(db_name: str, sa: Session) -> BaseJsonDbTable:
    cls = get_json_db_cls(db_name)
    with session_scope(sa) as session:
        cls.metadata.create_all(session.bind)
        add_json_db_entry_to_meta(db_name, session)
    return cls


def remove_json_db_table(db_name: str, sa: Session):
    cls = get_json_db_cls(db_name)
    with session_scope(sa) as session:
        remove_json_db_entry_from_meta(db_name, session)
        cls.__table__.drop(bind=session.bind)


def add_json_db_entry_to_meta(db_name: str, sa: Session):
    meta_entry = StorageMeta(db_name=db_name)
    sa.add(meta_entry)


def remove_json_db_entry_from_meta(db_name: str, sa: Session):
    sa.delete(sa.query(StorageMeta).filter(StorageMeta.db_name == db_name).one())
