"""
    Initialize the db here
"""
from pgfire.conf import conf
import psycopg2

COLLECTION_TABLE_NAME = "pgfire_collection"

COLLECTION_TABLE_CREATE = """
create table if not exists %s (
    collection varchar(255) primary key,
    data jsonb,
    created timestamp default current_timestamp,
    last_modified timestamp
)
""" % COLLECTION_TABLE_NAME

LAST_MODIFIED_UPDATE_TRIGGER_FN = """
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';
"""

CREATE_LAST_MODIFIED_UPDATE_TRIGGER = """
DO
$$
BEGIN
IF NOT EXISTS(
        SELECT *
        FROM information_schema.triggers
        WHERE event_object_table = '%s'
        AND trigger_name = 'update_collection_last_modified'
    ) THEN
    CREATE TRIGGER update_collection_last_modified
    BEFORE UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();
    END IF ;
END;
$$
""" % (COLLECTION_TABLE_NAME, COLLECTION_TABLE_NAME)


# init module variables
DB = conf.get("db", {})
COLLECTION_DB = DB.get("db")
DB_HOST = DB.get("host")
DB_PORT = DB.get("port")
DB_USER = DB.get("username")
DB_PASSWORD = DB.get("password")


def create_table_if_not_exists():
    with get_db_connection() as dbcon:
        cursor = dbcon.cursor()
        cursor.execute(COLLECTION_TABLE_CREATE)
        cursor.execute(LAST_MODIFIED_UPDATE_TRIGGER_FN)
        cursor.execute(CREATE_LAST_MODIFIED_UPDATE_TRIGGER)


def drop_db():
    pass


def drop_table():
    pass


def get_db_connection():
    db_con = psycopg2.connect(database=COLLECTION_DB,
                            host=DB_HOST,
                            port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
    db_con.set_session(autocommit=True)
    return db_con
