from pyg_sql import sql_table, sql_cursor, get_engine
import sqlalchemy as sa
import pytest
from functools import partial
from pyg_base import *
from pyg_encoders import * 
from pyg_sql import * 
server = db = schema = None
import pandas as pd
import pickle

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


def test_doc_store_save_and_read_dictable():
    drop_table('test_table', db = 'test_db')
    db = partial(sql_table, table = 'test_table', db = 'test_db', schema = 'test', pk = 'key', doc = True)
    t = db()   
    doc = Dict(function = passthru, data = dictable(a = [1,2,3], b = 'b'), key = 'dictable', db = db)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    
    assert len(t) == 1
    assert eq(t[0], doc)
    assert eq(t.deleted[0] - 'deleted' , doc)
    assert len(t.deleted) > 1

    t.deleted.drop()
    t.drop()
    with pytest.raises(sa.exc.DBAPIError):
        print(t)


def test_writable_doc_store_save_and_read():
    drop_table('test_table', db = 'test_db')
    db = partial(sql_table, table = 'test_table', db = 'test_db', schema = 'test', pk = 'key', doc = True, writer = '/test_db/test/test_data/%key.sql')
    t = db()
    doc = Dict(function = passthru, data = dictable(a = [1,2,3], b = 'b'), key = 'dictable', db = db)
    ## read_write without actually writing...
    # wdoc = t._write_doc(t._dock(doc))
    # decode(wdoc['doc'])


    _ = t.update_one(doc)
    _ = t.update_one(doc)
    _ = t.update_one(doc)

    ## access of stored data directly...
    store = sql_binary_store('/test_db/test/test_data/%key.sql').cursor
    stored = store.inc(key = 'dictable/data.dictable')[0]
    assert stored['key'] == 'dictable/data.dictable'
    assert eq(dictable_decode(pickle.loads(stored['data'])), doc.data)

    ## access via original table
    saved = t[0]
    t.read(0, False)

    ## saving a dataframe object
    df_doc = Dict(function = passthru, data = pd.DataFrame(dict(a = [1,2,3], b = 'b')), key = 'df', db = db)
    _ = t.update_one(df_doc)
    _ = t.update_one(df_doc)
    _ = t.update_one(df_doc)

    assert len(t.inc(key = 'df')) == 1
    saved_df = t.inc(key = 'df')[0]
    assert eq(saved_df.data, df_doc.data)

    t.deleted.drop()
    t.drop()
