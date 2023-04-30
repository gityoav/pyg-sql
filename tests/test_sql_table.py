from pyg_sql import sql_table, sql_cursor, get_engine, create_schema, sql_has_table
from pyg import eq, dictable, Dict, passthru, sql_binary_store, decode, encode, dictable_decode, db_cell
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.engine.base import Engine
import pytest
from functools import partial
server = db = schema = None
import pandas as pd
import pickle
import pytest
from pyg import timer

def drop_table(table, schema = None, db = None, server = None):
    try:
        sql_table(table = table, server = server, db = db, schema = schema).drop()
    except Exception:
        pass

e = get_engine(db = 'test_db', schema = 'dbo', create = True)



def test_create_parameters():   
    with pytest.raises(ValueError):
        sql_table('test_table', db = 'should_fail_we_did_not_mandate', nullable= dict(a=int, b=str))
    with pytest.raises(ValueError):
        get_engine(db = 'should_fail_we_did_not_mandate', schema = 'dbo')
    

def test_sql_table_base():
    table = 'test_sql_table_base'
    drop_table(table, db = 'test_db')
    t = self = sql_table(table, db = 'test_db', nullable= dict(a=int, b=str), _id = {'i': int})
    assert len(self) == 0
    t = t.insert(dict(a=1, b='a'))
    assert len(t) == 1
    t[0]
    t = t.insert(dict(a=1, b='a'))
    assert len(t) == 2
    t.df()
    with pytest.raises(ValueError):
        t = t.update_one(dict(a=1, b='a'))
    t.dry_run = False
    rs = dictable(a = range(10000), b = 4)
    _ = timer(lambda rs: t.insert_many(rs))(rs)
    t = t.rollback()

    t = t.commit()
    len(t)
    _ = timer(lambda rs: t.insert_many(rs, max_workers = 0))(rs)
    t.delete()
    t.df()
    t.commit()
    t.rollback()
    _ = timer(lambda rs: t.insert_many(rs, max_workers = 0))(rs)
    _ = timer(lambda rs: [t.insert_one(doc, max_workers = 0) for doc in rs])(rs)
    t.drop()




def test_doc_store_save_and_read_dictable():
    table = 'test_doc_store_save_and_read_dictable'
    db = partial(sql_table, table = table, db = 'test_db', schema = 'test', pk = 'key', doc = True)
    t = db()   
    t = t.delete()
    doc = Dict(function = passthru, data = dictable(a = [1,2,3], b = 'b'), key = 'dictable', db = db)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    _ = t.update_one(doc)
    
    assert len(t) == 1
    assert eq(t[0]-'db', doc-'db')
    assert eq(t.archived()[0] - 'deleted' - 'db' , doc -'db')
    assert len(t.archived()) > 1

    t.archived().drop()
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
    assert len(t.archived().inc(key = key))>1
    doc = t.archived().inc(key = key)[0]
    assert eq(doc - 'deleted', docs[0])


def test_archive_does_not_update():
    table = sql_table(db = 'test_db', schema = 'dbo', table = 'test_archive_does_not_update', pk = 'item', doc = True,
                      writer = '/test_db/dbo/datas/%item.sql'
                      )
    table.delete()
    table.update_one(db_cell(item = 'a', ts = pd.Series([1,2,3], [4,5,6]), b = 1))
    table.update_one(db_cell(item = 'a', ts = pd.Series([1,2,4], [4,5,6]), b = 2))
    table.update_one(db_cell(item = 'a', ts = pd.Series([0,0,0], [4,5,6]), b = 3))
    table.archived()[1]
    table.delete()
    table.drop(True)


def test_saving_binary_works():
    table = sql_table(db = 'test_db', schema = 'dbo', table = 'test_saving_binary_works', pk = 'item', nullable = dict(bin = bin), doc = False)
    table.insert(dict(item = 'a', bin = pd.Series([1,2,3])))   
    table.insert(dict(item = 'b', bin = pd.Series([4,5,6])))   
    assert len(table)>=2
    assert table.item == ['a', 'b']
    table.drop()


def test_getting_engine_from_session():
    session = Session(e)
    assert isinstance(get_engine(session = session), Engine)

def test_running_with_external_session():
    table = 'test_running_with_external_session'
    t = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False)
    t = t.delete()
    assert len(t) == 0
    assert t.session is None
    assert t.connection() is not None
    with Session(e) as session:
        tbl = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False, session = session, dry_run = True)
        tbl.insert_one(dict(a = 'a', b = 'b', item = 'item'))
        session.rollback()
    assert len(t) == 0
    with Session(e) as session:
        tbl = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False, session = session, dry_run = True)
        tbl.insert_one(dict(a = 'a', b = 'b', item = 'item'))
        session.commit()
    assert len(t) == 1
    t.drop()


def test_running_with_context():
    table = 'test_running_with_context'
    t = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False)
    t = t.delete()
    assert len(t) == 0
    with t.connect(dry_run = True) as tbl:
        tbl.insert_one(dict(a = 'a', b = 'b', item = 'item'))
    assert len(t) == 0
    with t as tbl:
        tbl.insert_one(dict(a = 'a', b = 'b', item = 'item'))
    assert len(t) == 0
    with t.connect(dry_run = False) as tbl:
        tbl.insert_one(dict(a = 'a', b = 'b', item = 'item'))
    assert len(t) == 1
    t.drop(True)
    
def test_running_with_no_context():
    table = 'test_running_with_no_context'
    t = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False)
    t = t.delete()
    assert len(t) == 0
    t.insert_one(dict(a = 'a', b = 'b', item = 'item'))
    assert len(t) == 1
    t.drop(True)
    
def test_encoding_cells_with_connections():
    table = 'test_encoding_cells_with_connections'
    t = sql_table(db = 'test_db', schema = 'dbo', table = table, pk = 'item', nullable = ['a', 'b'], doc = False)
    db = 'test_db'; schema = 'dbo'; doc = False    
    session = Session(e)    
    dbt = partial(sql_table, db = db, schema = schema, doc = doc, session = session)
    encode(dbt)
    

