def test_get_connection():
    from pgfire.engine import dbops
    con = dbops.get_db_connection()
    assert con
    con.close()


def test_create_db():
    from pgfire.engine import dbops
    dbops.create_table_if_not_exists()
    con = dbops.get_db_connection()
    try:
        cursor = con.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'"
                                    % dbops.COLLECTION_TABLE_NAME)
        assert cursor.fetchone()
    finally:
        if not con:
            con.close()
