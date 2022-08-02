from pyg_sql import sql_table, sql_cursor, get_engine
import sqlalchemy as sa
import pytest
server = db = schema = None

def drop_table(table, schema = None, db = None, server = None):
    try:
        sql_table(table = table, server = server, db = db, schema = schema).drop()
    except Exception:
        pass

def test_sql_table_base():
    drop_table('test_table', db = 'test_db')
    t = sql_table('test_table', db = 'test_db', nullable= dict(a=int, b=str))
    assert len(t) == 0
    t = t.insert(dict(a=1, b='a'))
    assert len(t) == 1
    t = t.insert(dict(a=1, b='a'))
    assert len(t) == 2
    with pytest.raises(ValueError):
        t = t.update_one(dict(a=1, b='a'))
    t.drop()
