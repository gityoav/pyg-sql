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
    db = partial(sql_table, table = 'test_table', db = 'test_db', schema = 'test', pk = 'key', doc = True)
    t = db()   
    t = t.delete()
    doc = Dict(function = passthru, data = dictable(a = [1,2,3], b = 'b'), key = 'dictable', db = db)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    
    assert len(t) == 1
    assert eq(t[0]-'db', doc-'db')
    assert eq(t.deleted[0] - 'deleted' - 'db' , doc -'db')
    assert len(t.deleted) > 1

    t.deleted.drop()
    t.drop()
    with pytest.raises(sa.exc.DBAPIError):
        print(t)


def test_writable_doc_store_save_and_read():
    drop_table('test_table', db = 'test_db')
    db = partial(sql_table, table = 'test_table', db = 'test_db', schema = 'test', pk = 'key', doc = True, writer = '/test_db/test/test_data/%key.sql')
    t = db()
    df = pd.DataFrame([1,2,3])
    docs = [Dict(function = passthru, data = dictable(a = [1,2,3], b = 'b'), key = 'dictable', db = db), 
            Dict(function = passthru, data = pd.DataFrame(dict(a = [1,2,3], b = 'b')), key = 'df', db = db),
            Dict(function = passthru, data = dictable(a = [1,df*2,df], b = 'b'), key = 'dictable2', db = db), 
            ]

    for doc in docs:
        ## read_write without actually writing the document to the database
        wdoc = t._write_doc(t._dock(doc))
        assert eq(decode(wdoc['doc']).data , doc.data)
    
        _ = t.update_one(doc)
        _ = t.update_one(doc)
        _ = t.update_one(doc)
    
        ## access via original table
        saved = t.inc(key = doc.key)[0]
        assert eq(saved.data, doc.data)

    ## access of binary stored data directly...
    store = sql_binary_store('/test_db/test/test_data/%key.sql').cursor
    assert 'dictable2/data/a/1' in store.distinct('key')
    stored = store.inc(key = 'dictable/data.dictable')[0]
    assert stored['key'] == 'dictable/data.dictable'
    assert eq(dictable_decode(pickle.loads(stored['data'])), docs[0].data)
    stored = store.inc(key = 'dictable2/data/a/1')[0]
    assert eq(pickle.loads(stored['data']), 2*df)

    ## acess of archived data directly
    
    key = docs[0].key
    assert len(t.deleted.inc(key = key))>1
    doc = t.deleted.inc(key = key)[0]
    eq(doc - 'deleted', docs[0])

sql_table('test_table', server = True, db = 'test_db', schema = 'archived_test')[0]['doc']

t.deleted.reset
doc.data
docs[0].data
    t.deleted.drop()
    t.drop()
