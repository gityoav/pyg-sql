import sqlalchemy as sa
from sqlalchemy_utils.functions import create_database
from pyg_base import cache, cfg_read, as_list, dictable, lower, loop, replace, Dict, is_dict, is_dictable, is_strs, is_str, is_int, is_date, dt2str, ulist, try_back, unique, is_primitive
from pyg_encoders import as_reader, as_writer, dumps, loads, encode, executor_pool
from sqlalchemy import Table, Column, Integer, String, MetaData, Identity, Float, DATE, DATETIME, TIME, select, func, not_, desc, asc
from sqlalchemy.orm import Session
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import NUMERIC, FLOAT, INT, Boolean, DECIMAL
import datetime
from copy import copy
from pyg_base import logger, is_regex, dt
from functools import partial
import pandas as pd
import numpy as np
import re
import pyodbc

if hasattr(sa.engine.row, 'LegacyRow'): ## SQL ALCHEMY 1.4 and below
    ROW_TYPE = (sa.engine.row.LegacyRow, sa.engine.row.Row)
else:
    ROW_TYPE = sa.engine.row.Row

@loop(list, tuple, dict)
def impartial(v):
    if isinstance(v, partial):
        args, keywords = impartial((v.args, v.keywords))
        return v.func(*args, **keywords)
    else:
        return v
       

SESSIONS = {}
_SCHEMAS = {}
_TABLES = {}
_CHUNK = 100
_MAX_WORKERS = 0


_id = '_id'
_doc = 'doc'
_root = 'root'
_deleted = 'deleted'
_pd_is_old = pd.__version__.startswith('0')
_index = 'index'

_is_sql_writer = re.compile('.(sq|pd)[a-z]{1}$')

_funcs = {np.max : func.max, len: func.count, np.min: func.min, np.sum: func.sum, np.mean: func.avg}

@loop(dict)
def _func(f):
    if isinstance(f, str):
        return getattr(func, f)
    elif isinstance(func.max, sa.sql.functions._FunctionGenerator): ## f = sqlalchemy.func.max
        return f
    elif f in _funcs:
        return _funcs[f]
    elif callable(f): ## f = max
        return getattr(func, f.__name__)
    else:
        raise ValueError(f'dont know what function is this {f}')



NVARCHAR = sa.NVARCHAR(450) 
VARCHAR = sa.VARCHAR(450)

_types = {str: String, 'str' : String, 's': String,
          int : Integer, 'int' : Integer, 'i' : Integer,
          float: Float, 'float': Float, 'f': Float,
          bool : Boolean, 'bool' : Boolean, 'b' : Boolean,
          np.int64 : sa.BigInteger, 'bigint' : sa.BigInteger, 'g' : sa.BigInteger,
          'nvarchar' : sa.NVARCHAR, 'n' : sa.NVARCHAR,
          'varchar' : sa.VARCHAR, 'v' : sa.VARCHAR,
          'decimal' : DECIMAL,
          'dec' : DECIMAL,
          '0' : DECIMAL(0),  '1' : DECIMAL(1), '2' : DECIMAL(2), '3' : DECIMAL(3), '4' : DECIMAL(4),
          '5' : DECIMAL(5), '6' : DECIMAL(6), '7' : DECIMAL(7), '8' : DECIMAL(8), '9' : DECIMAL(9),
          datetime.date: DATE, 'date' : DATE, 'd' : DATETIME,
          datetime.datetime : DATETIME, 'datetime' : DATETIME, 'dt' : DATETIME, dt : DATETIME, 'e' : DATETIME,
          datetime.time: TIME, 'time' : TIME, 't' : TIME,
          bin : sa.VARBINARY, 'y': sa.VARBINARY}

_type_codes = {String : 's', Integer : 'i', Float : 'f', Boolean: 'b', sa.BigInteger : 'g',
               NVARCHAR : 'n', VARCHAR : 'v', DATE : 'd', TIME: 't', DATETIME : 'e', sa.VARBINARY: 'y',
               DECIMAL(0): '0', DECIMAL(1): '1', DECIMAL(2): '2', DECIMAL(3): '3', DECIMAL(4): '4',
               DECIMAL(5): '5', DECIMAL(6): '6', DECIMAL(7): '7', DECIMAL(8): '8', DECIMAL(9): '9'}


## This is what is used for keys that are strings and are part of the primary keys to ensure they can be indexed
_pk_types = _types | {str : NVARCHAR, 'str' : NVARCHAR} 


def session_execute(session, statement, args = None, kwargs = None, commit = False):
    """
    This allows multiple statements to be executed at once
    """
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    if isinstance(statement, list):
        res = []
        n = len(statement)
        assert len(args) == 0 or len(args) == n
        assert len(kwargs) == 0 or len(kwargs) == n
        for i in range(n):
            statement_ = statement[i]
            args_ = args[i] if len(args) else ()
            kwargs_ = kwargs[i] if len(kwargs) else {}
            if args_ is None:
                args_ = ()
            if kwargs_ is None:
                kwargs_ = {}
            res_ = session.execute(statement_, *args_, **kwargs_)
            res.append(res_)
    else:    
        res = session.execute(statement, *args, **kwargs)
    if commit:
        session.commit()
    return res





def _as_type(t, types):
    if isinstance(t, str):
        t = t.lower()
        strs = {k:v for k,v in types.items() if isinstance(k, str) and t.startswith(k)}
        if len(strs) == 0:
            raise ValueError(f'type {t} not found')
        else:
            k, v = sorted(list(strs.items()))[-1]
            if k == t:
                return NVARCHAR if v == sa.NVARCHAR else VARCHAR if v == sa.VARCHAR else v
            else:
                n = int(t[len(k):])
                return v(n)
    else:
        return types.get(t, t)


_orders = {1 : asc, True: asc, 'asc': asc, asc : asc, -1: desc, False: desc, 'desc': desc, desc: desc}

def _data_and_columns(executed):
    columns = list(executed.keys())
    data = list(executed)
    return data, columns

def valid_session(session):
    """
    TO DO: How do we check if db connection expired??

    Parameters
    ----------
    session : db session
        sql database session

    Returns
    -------
    bool
        is the session still valid?

    """
    return session is not None

def _relabel(res, selection, strict = True):
    """
    selection defines the CASE we want the columns to be in
    We convert either columns or dicts into the case from 

    Parameters
    ----------
    res : pd.DataFrame/dict
        original value
    selection : str/list of str
        columnes/keys in the case we want them
    strict : bool, optional
        if False, will replace opportunistically. If True, needs all res & selection to match. The default is True.

    Returns
    -------
    pd.DataFrame/dict
        same object with case matching
        
    Example
    -------
    >>> res = dict(a = 1, b = 2, c = 3)
    >>> selection = ['A', 'B']
    >>> assert _relabel(res, selection) == res                  ## no replacement, strict
    >>> assert _relabel(res, selection, strict = False) == dict(A = 1, B = 2, c = 3) ## replaceme what is possible
    >>> assert _relabel(res, ['A', 'B', 'C']) == dict(A = 1, B = 2, C = 3)
    >>> assert _relabel(res, ['A', 'B', 'C'], True) == dict(A = 1, B = 2, C = 3)

    """
    if not is_strs(selection):
        return res
    lower_selection = lower(as_list(selection))
    if isinstance(res, pd.DataFrame):
        columns = list(res.columns)
        if columns == selection:
            return res
        if lower(list(res.columns)) == lower_selection:
            res.columns = selection
        elif not strict:
            lower2selection = dict(zip(lower_selection, selection))
            res.columns = [lower2selection.get(col.lower(),col) for col in res.columns]    
        return res
    elif isinstance(res, dict):
        if strict and sorted(lower(list(res.keys()))) == sorted(lower_selection):
            columns = res.keys()
            if sorted(columns) == sorted(selection): ## nothing to do
                return res
            lower2selection = dict(zip(lower_selection, selection))
            return type(res)({lower2selection[key.lower()] : value for key, value in res.items()})
        elif not strict:
            lower2selection = dict(zip(lower_selection, selection))
            return type(res)({lower2selection.get(key.lower(), key): value for key, value in res.items()})
        else:
            return res            
    else:
        return res       

def _get_columns(table):
    """
    returns a dict from lower-case columns to actual columns
    works with tables, join objects or sql_cursors
    """
    if isinstance(table, sql_cursor):
        table = table._table
    cols = table.columns
    res = {}
    for col in cols:
        name = col.name.lower()
        res[name] = res.get(name, []) + [col]
    return res


def _pw_filter(col, v):
    if is_regex(v):
        p = v.pattern
        if not p.startswith('%'):
            p = '%' + p
        if not p.endswith('%'):
            p = p + '%'
        return col.like(p)
    elif is_str(v) and v.startswith('%') and v.endswith('%'):
        return col.like(v)
    elif isinstance(v, list):
        return sa.or_(*[_pw_filter(col, i) for i in v])
    else:
        return col == v

@loop(list)
def _like_within_doc(v, k):
    return '%' + dumps(encode({k : v}))[1:-1] + '%'


class sql_groupby():
    """
    a helper function
    """
    def __init__(self, cursor, by):
        self.cursor = cursor
        self.by = by
        
    def __call__(self, **aggregate):
        by = as_list(self.by)
        cursor = self.cursor
        by_cols = cursor._col(by)
        columns = cursor.table.columns
        by_ = [columns[b] for b in by_cols]
        aggregate_ = _func(cursor._col(aggregate))
        statement = select(*by_, *[f(columns[col]) for col, f in aggregate_.items()]).group_by(*by_)
        res = cursor.execute(statement, commit = False, transform = list)
        rs = dictable(res, by + list(aggregate.keys()))
        return rs
    
    def __getattr__(self, f):        
        by = as_list(self.by)
        cursor = self.cursor
        aggregate = {key: f for key in ulist(cursor.selection or cursor.columns) - by}
        return self(**aggregate)

class sql_df():
    """
    a helper class designd to allow access of DataFrames this way:

    >>> sql_table(...).df()
    >>> sql_table(...).df[::]
    >>> sql_table(...).df[0] ## a pd.Series of the 1st column
    >>> sql_table(...).df[:20] ## a pd.DataFrame of the top 20 rows

    """
    def __init__(self, cursor):
        self.cursor = cursor

    def __call__(self, coerce_float = True, start = None, stop = None, step = None):
        """
        This is a more optimized, faster version for reading the table. 
        It retuns the data as a pd.DataFrame,
        In addition, it converts NUMERIC type (which is cast to decimal.Decimal) into a float

        
        Parameters
        ----------
        coerce_float: bool
            converts sql.types.NUMERIC columns into float

        Returns
        -------
        pd.DataFrame
            The data, optimized as a dataframe.

        
        Example: speed comparison
        --------

        >>> from pyg import * 
        >>> t = sql_table(db = 'db', table = 'tbl', nullable = dict(a = int, b = float, c = str))
        >>> rs =dictable(a = range(100)) * dict(b = list(np.arange(0,100,1.0))) * dict(c = list('abcdefghij'))
        >>> t = t.insert_many(rs)      
        
        ## reading the data as t[::] is slow...
        >>> x = timer(lambda : t[::])()
        2022-08-21 18:03:52,594 - pyg - INFO - TIMER: took 0:00:13.139333 sec
 
        ## reading the data as t.df() is much faster...
        >>> y = timer(lambda : t.df())()
        2022-08-21 18:04:24,809 - pyg - INFO - TIMER: took 0:00:00.456988 sec


        """
        cursor = self.cursor
        statement = cursor.statement()
        if (is_int(start) and start < 0) or (is_int(stop) and stop < 0):
            n = len(cursor)
            start = n + start if is_int(start) and start < 0 else start                            
            stop = n + stop if is_int(stop) and stop < 0 else stop
        if start and cursor.order is not None:
            statement = statement.offset(start)
            stop = stop if stop is None else stop - start
            start = None
        if stop is not None:
            statement = statement.limit(1+stop)
        if _pd_is_old:
            data, columns = cursor.execute(statement = statement, transform = _data_and_columns, commit = False)
            res = pd.DataFrame(data, columns = columns)
        else:
            res = cursor.execute(statement = statement, transform = pd.DataFrame, commit = False)
        if start is not None or stop is not None or step is not None:
            res = res.iloc[slice(start, stop, step)]
        if coerce_float:
            for col in res.columns:
                t = cursor.table.columns[col].type
                if isinstance(t, NUMERIC) and not isinstance(t, (FLOAT, INT)):
                    res[col] = res[col].astype(float)
        return _relabel(res, cursor.selection)
    
    def __getitem__(self, value):
        if isinstance(value, slice):
            start, stop, step = value.start, value.stop, value.step
            return self(start = start, stop = stop, step = step)
        elif isinstance(value, int):
            return pd.Series(self.cursor[value])
        


def _servers():
    res = cfg_read().get('sql_server', {})
    if isinstance(res, dict):
        res[None] = res.get('null')
        res[True] = res.get('true')
        res[False] = res.get('false')
        res['None'] = res.get('None', res[None])
    return res

@cache
def _schema(schema = None):
    return schema or cfg_read().get('sql_schema', 'dbo')


@cache
def _databases():
    """
    configures the sql_databases
    from pyg import * 
    
    cfg = cfg_read()
    cfg['sql_database'] = {None : 'db', 'res' : 'res'}
    cfg_write(cfg)

    """
    dbs = cfg_read().get('sql_database', {None: 'master'})
    if isinstance(dbs, str):
        dbs = {dbs : dbs}
    if None not in dbs:
        dbs[None] = dbs.get('null')
    dbs[True] = dbs.get('true')
    dbs[False] = dbs.get('false')
    return dbs

@cache
def _database(db = None):
    dbs = _databases()
    db = dbs.get(db, db)
    db = dbs.get(db, db)
    return db
    
def get_server(server = None):
    """
    Determines the sql server string
    We support a server config which looks like:
        config['sql_server'] = dict(dev = 'server.test', prod = 'prod.server')
    """
    servers = _servers()
    server = server or None
    if isinstance(servers, str):
        if server is None or server is True:
            server = servers
    else:
        server = servers.get(server, server)
        server = servers.get(server, server) #double redirection supported
    if server is None:
        raise ValueError('please provide server or set a "sql_server" in cfg file: from pyg_base import *; cfg = cfg_read(); cfg["sql_server"] = "server"; cfg_write(cfg)')
    return server


def get_driver(driver = None):
    """
    determines the sql server driver
    """
    if driver is None or driver is True:
        driver = cfg_read().get('sql_driver')
    if driver is None:
        odbc_drivers = [d for d in pyodbc.drivers() if d.startswith('ODBC')]
        if len(odbc_drivers):
            driver = sorted(odbc_drivers)[-1]
        if driver is None:
            raise ValueError('No ODBC drivers found for SQL Server, please save one: cfg = cfg_read(); cfg["sql_driver"] = "ODBC+Driver+17+for+SQL+Server"; cfg_write(cfg)')    
        else:
            driver = driver.replace(' ', '+')
            return driver
    elif is_int(driver):
        return 'ODBC+Driver+%i+for+SQL+Server'%driver
    else:
        return driver


def _pairs2connection(*pairs, **connection):
    connection = connection.copy()
    for pair in pairs:
        ps = pair.split(';')
        for p in ps:
            if p:
                k, v = p.split('=')
                k = k.strip()
                v = v.strip().replace("'","")
                connection[k] = v
    connection = {k.lower() : replace(replace(v, ' ','+'),['{','}'],'') for k, v in connection.items() if v is not None}
    return connection


def _db(connection):
    db = connection.pop('db', None)
    if db is None:
        db = connection.pop('database', None)
    db = _database(db)
    return db
    

_CSTR = 'Driver={ODBC Driver 17 for SQL Server};Server=%(server)s;Database=%(db)s;Authentication=ActiveDirectoryIntegrated;'

def get_connection_info(*pairs, **connection):
    """
    determines the connection string
    """
    connection = _pairs2connection(*pairs, **connection)
    server = get_server(connection.pop('server', None))
    connection['driver'] = get_driver(connection.pop('driver', None))
    db = _db(connection)
    if '//' in server:
        return server
    else:
        cstr = _CSTR%dict(server=server, db = db)
        return cstr

def get_cstr(*pairs, **connection):
    """
    determines the connection string
    """
    connection = _pairs2connection(*pairs, **connection)
    server = get_server(connection.pop('server', None))
    connection['driver'] = get_driver(connection.pop('driver', None))
    db = _db(connection)
    if '//' in server:
        return server
    else:
        params = '&'.join('%s=%s'%(k,v) for k,v in connection.items())
        cstr = 'mssql+pyodbc://%(server)s/%(db)s%(params)s'%dict(server=server, db = db, params = '?' +params if params else '')
        return cstr


def create_schema(engine, session, schema, create = True, server = None, db = None):
    """
    creates a new schema for a particular engine.

    Parameters
    ----------
    engine : sql engine
        engine used to verify that the database has/does not have a specific schema
    schema : str
        name of a schema.
    create : bool/str
        If set to True, will create a missing schema. Alternatively, if create is set to either 'database' or 'schema' will also create schema
    session : sql ORM session, optional
        If provided, will use this to execute a session. The default is None.


    Returns
    -------
    schema : str
        name of a schema

    """
    if schema is None:
        return
    key = (server, db, schema, create)
    if key not in _SCHEMAS:
        try:
            if schema not in engine.dialect.get_schema_names(session):
                if create is True or (is_str(create) and (create.lower()[0] in 'sd')):
                    session.execute(sa.schema.CreateSchema(schema))
                else:
                    raise ValueError(f'Schema {schema} does not exist. You have to explicitly mandata the creation of a schema by setting create=True or create="d" or create="s"')
                logger.info('creating schema: %s'%schema)
        except AttributeError: #MS SQL vs POSTGRES
            if not engine.dialect.has_schema(session, schema):
                session.execute(sa.schema.CreateSchema(schema))
                logger.info('creating schema: %s'%schema)
        _SCHEMAS[key] = schema
    return _SCHEMAS[key]


@cache
def _create_engine(server, db , **connection):
    connection_info = get_connection_info(server=server, db = db, **connection)    
    try:
        con = pyodbc.connect(connection_info, autocommit = False)    
        e = sa.create_engine("mssql://" + connection_info, creator = lambda : con, echo = False, use_setinputsizes = True)
        #print('created engine using connection info', "mssql://" + connection_info)
    except Exception:
        cstr = get_cstr(server=server, db = db, **connection)
        e = sa.create_engine(cstr)        
        #print('created engine using csr', cstr)
    return e


def _get_engine(*pairs, **connection):  
    connection = _pairs2connection(*pairs, **connection)
    server = connection.pop('server', None)
    if isinstance(server, Engine):
        return server
    elif isinstance(server, Session):
        return server.get_bind()
    session = connection.pop('session', None)
    if session is not None:
        return session.get_bind()
    engine = connection.pop('engine', None)
    if isinstance(engine, Engine):
        return engine
    connection['driver'] = get_driver(connection.pop('driver', None))
    create = connection.pop('create', False)
    db = _db(connection)
    if (server, db) in SESSIONS:
        return SESSIONS[(server, db)].get_bind()
    server = get_server(server)
    if callable(engine): # delegates connection to another function
        e = Dict(server = server, db = db, environment = server, **connection).apply(engine)
    else:
        e = _create_engine(server = server, db = db, **connection)
    try:
        sa.inspect(e)
    except Exception:
        if create is True or (is_str(create) and 'd' in create.lower()): ## create can be a "level" specifying if it is to be created
            cstr = get_cstr(server=server, db = db, **connection)    
            create_database(cstr)
            logger.info('creating database: %s'%db)
        else:
            raise ValueError('You have to explicitly permission the creation of the database by setting create = True or create ="d"')
    return e



def get_session(db, server = None, engine = None, session = None, session_maker = None, **session_kwargs):
    """
    returns an ORM session

    Parameters
    ----------
    db : str
        database name.
    server : str
        server name.
    engine : a sql session object, optional
        A SQL engine. The default is None.
    session : ORM-like session, optional
        An existing session. The default is None.

    session_maker: a function/class that creates a session
        if None defaults sqlalchemy Session

    Returns
    -------
    Session

    """
    address = (server, db)
    if session is not None:
        if address not in SESSIONS:
            SESSIONS[address] = session
        return session
    if address not in SESSIONS:
        engine = _get_engine(server = server, db = db, engine = engine)
        session_maker = session_maker or Session
        SESSIONS[address] = session = session_maker(engine, **session_kwargs)
    return session


def get_engine(*pairs, **connection):
    """
    returns a sqlalchemy engine object
    accepts either 
    *pairs: 'driver={ODBC Driver 17 for SQL Server}' or
    **connection: keyword arguments that look like driver = 'ODBC Driver 17 for SQL Server'    
    """
    return _get_engine(*pairs, **connection)
    

def _get_table(table_name, schema, db, server, create, engine = None, session = None):
    key = (server, db, schema, table_name, create) 
    if key not in _TABLES:
        engine = _get_engine(server = server, db = db, schema = schema, create = create, engine = engine, session = session)
        meta = MetaData()
        _TABLES[key] = Table(table_name, meta, autoload_with = engine, schema = schema, extend_existing = True)
    return _TABLES[key]


def sql_has_table(table_name, schema, db, server, engine = None, session = None):
    e = _get_engine(server = server, db = db, schema = schema, create = False, engine = engine, session = session)
    return sa.inspect(e).has_table(table_name)
        


def sql_table(table, db = None, non_null = None, nullable = None, _id = None, schema = None, 
              server = None, reader = None, writer = None, pk = None, doc = None, mode = None, 
              spec = None, selection = None, order = None, defaults = None, joint = None, create = None, 
              engine = None, session = None, dry_run = None):
    """
    Creates a sql table. Can also be used to simply read table from the db
    Parameters
    ----------
    table : str
        name of table can also be passed as 'database.table'.
    db : str, optional
        name of database. The default is None.
    non_null : str/list of strs/dict 
        dicts of non-null column names to their type, optional. The default is None.
    nullable : str/list of strs/dict , optional
        dicts of null-able column names to their type, optional. The default is None.
    _id: str/list of strs/dict , optional
        dicts of column that are auto-completed by the server and the user should not provide these.
    schema : str, optional
        like 'dbo'. The default is None.
    server : str, optional
        Name of connection string. The default is None.
    reader : Bool/string, optional
        How should data be read. The default is None.
    writer : bool/string, optional
        How to transform the data before saving it in the database. The default is None.
    pk : list, optional
        primary keys on which the table is indexed. if pk == 'KEY' then we assume 'KEY' will be uniquely valued. The default is None.
    doc : str / True, optional
        If you want the DOCUMENT (a dict of stuff) to be saved to a single column, specify.
    mode : int, optional
        NOT IMPLEMENTED CURRENTLY
    spec : sqlalchemy selection object
        can specify a filter on the resulting data, applied in a WHERE
    selection: None/str/list(str)
        can specify column selection in a SELECT statement
    order: dict/str/liststr
        can specifiy a SORT statement
    joint: a list of tuples:
        Each tuple contains the object to join the table with, as well as the other parameters used in a sql alchemy join statement 
    create: bool/str
        Creation policy. The "mongo" document store policy is to create a database on the fly.
        Here the policy is more nuanced:
        'd' or True : will create the database, the schema and the table if needed
        's'         : will create a schema & table if database exists but if database doesn't, will throw
        't'         : will create a table if db & schema exists and will throw if either db or schema are missing
        '' or False : will throw
        if create is None:
            if no nullable and no non_null and doc is False:
                create = False
            else:
                create = 's'
    engine: sqlalchemy Engine, None, or a function that returns an engine
        This allows connection management to be delegated to the user who may want to manage her own connection protocol
    defaults:
        this is default variable that is added to the document if it does not have its keys
    Returns
    -------
    res : sql_cursor
        A hybrid object we love.
    Example: simple table creation
    ---------
    >>> from pyg import * 
    >>> table = sql_table(table = 'test', nullable = dict(a = int, b = str), db = 'db')
    >>> table.insert(dict(a = 1, b = 'a'))
    >>> assert len(table) == 1
    >>> table.insert(dictable(a = [2,3], b = 'b'))
    >>> assert len(table) == 3
    >>> assert len(table.inc(table.c.a > 2)) == 1
    >>> assert table.inc(table.c.a > 2)[0] == dict(a = 3, b = 'b')
    >>> table.inc(b = 'a').delete()
    >>> assert len(table) == 2
    >>> assert table.distinct('b') == ['b']
    >>> table.drop()
    
    Example: ensure sql_table defaults for a doc store if 'doc' exists in existing table or no data columns are specified on creation
    --------
    table = sql_table(table = 'test', pk = 'key', schema ='dbo', db = 'db')
    assert table.doc == 'doc'
    table = sql_table(table = 'test', db = 'db', schema = 'dbo')
    assert tbl.doc == 'doc'
    table.drop()
    Example: simple join
        
    """
    table, db, schema, session, server, engine = impartial((table, db, schema, session, server, engine ))
    if isinstance(table, str):
        values = table.split('.')
        if len(values) == 2:
            schema = schema or values[0]
            if schema != values[0]:
                raise ValueError('schema cannot be both %s and %s'%(values[0], db))
            table = values[1]
        elif len(values)>2:
            raise ValueError('not sure how to translate this %s into a db.table format'%table)
    elif isinstance(table, partial): # support for using the partial rather than the actual table
        # if not schema and not db and not server and not pk and not writer and not doc and not engine:
        #     return table()
        db = table.keywords.get('db') if db is None else db
        schema = table.keywords.get('schema') if schema is None else schema
        server = table.keywords.get('server') if server is None else server
        writer = table.keywords.get('writer') if writer is None else writer
        engine = table.keywords.get('engine') if engine is None else engine
        session = table.keywords.get('session') if session is None else session
        dry_run = table.keywords.get('dry_run') if dry_run is None else dry_run
        doc = table.keywords.get('doc') if doc is None else doc
        pk = table.keywords.get('pk') if pk is None else pk
        table = table.keywords['table']
    elif isinstance(table, sql_cursor):
        # if not schema and not db and not server and not pk and not writer and not doc and not engine:
        #     return table
        # we want to remove 
        db = table.db if db is None else db
        engine = table.engine if engine is None else engine
        session = table.session if session is None else session
        schema = table.schema if schema is None else schema
        server = table.server if server is None else server
        writer = table.writer if writer is None else writer
        dry_run = table.dry_run if dry_run is None else dry_run
        doc = table.doc if doc is None else doc
        pk = table.pk if pk is None else pk
        table = table.name
    
    ### we resolve some parameters
    if doc is True: #user wants a doc
        doc = _doc
    non_null = non_null or {}
    nullable = nullable or {}
    pks = pk or {}
    if isinstance(pks, list):
        pks = {k : str for k in pks}
    elif isinstance(pks, str):
        pks = {pks : str}
    if isinstance(non_null, list):
        non_null = {k : str for k in non_null}
    elif isinstance(non_null, str):
        non_null = {non_null : str}
    #pks.update(non_null)
    if isinstance(nullable, list):
        nullable = {k : str for k in nullable}
    elif isinstance(nullable, str):
        nullable = {nullable: str}
    if isinstance(table, str):
        table_name = table 
    else:
        table_name = table.name
        if schema is None:
            schema = table.schema

    ## do we have any columns in table?
    cols = []
    if isinstance(table, sa.sql.schema.Table):
        for col in table.columns:
            col = copy(col)
            del col.table
            cols.append(col)
    if _id is not None:
        if isinstance(_id, str):
            _id = {_id : int}
        if isinstance(_id, list):
            _id = {i : int for i in _id}
        for i, t in _id.items():
            if i not in [col.name for col in cols]:
                if t == int:                    
                    cols.append(Column(i, Integer, Identity(always = True)))
                elif t == datetime.datetime:
                    cols.append(Column(i, DATETIME(timezone=True), nullable = False, server_default=func.now()))
                else:
                    raise ValueError('not sure how to create an automatic item with column %s'%t)

    col_names = [col.name for col in cols]
    pk_cols   = [Column(k, _as_type(t, _pk_types), nullable = False, autoincrement = False) for k, t in pks.items() if k not in col_names]
    non_nulls = [Column(k, _as_type(t, _types), nullable = False, autoincrement = False) for k, t in non_null.items() if k not in col_names and k not in pks]    
    nullables = [Column(k.lower(), _as_type(t, _types), nullable = True, autoincrement = False) for k, t in nullable.items() if k not in col_names and k not in pks and k not in non_null]
    if not doc or doc in col_names or doc in pks or doc in non_null or doc in nullable:
        docs = []
    else:        
        docs = [Column(doc, String, nullable = True, autoincrement = False)]
    cols = cols + pk_cols + non_nulls + nullables + docs


    ## creation logic:
    if create is None:
        create = 's' if len(cols) else False

    ## time to access/create tables
    engine = _get_engine(server = server, db = db, schema = schema, create = create, engine = engine, session = session)
    session = get_session(db = db, engine = engine, session = session)
    schema = create_schema(engine = engine, schema = _schema(schema), create = create, session = session, db = db, server = server)
    try:
        tbl = _get_table(table_name = table_name, schema = schema, db = db, server = server, create = create, engine = engine, session = session)
        if doc is None and _doc in [col.name for col in tbl.columns]:
            doc = _doc
    except sa.exc.NoSuchTableError:        
        if doc is None and len(non_null) == 0 and len(nullable) == 0: #user specified nothing but pk so assume table should contain SOMETHING :-)
            doc = _doc
        meta = MetaData()
        if len(cols) == 0:
            raise ValueError('You seem to be trying to create a table with no columns? Perhaps you are trying to point to an existing table and getting its name wrong?')
        if create is True or (is_str(create) and (create.lower()[0] in 'tsd')):
            logger.info('creating table: %s.%s.%s%s'%(db, schema, table_name, [col.name for col in cols]))
            tbl = Table(table_name, meta, *cols, schema = schema)
            meta.create_all(engine)
            idx_keys = [tbl.c[key] for key in pks]
            if pks:
                idx = sa.Index("idx_pks", *idx_keys, unique=True)
                idx.create(engine)
        else:
            raise ValueError(f'table {table_name} does not exist. You need to explicitly set create=True or create="t/s/d" to mandate table creation')
    res = sql_cursor(table = tbl, schema = schema, db = db, server = server, engine = engine, session = session, dry_run = dry_run,
                     reader = reader, writer = writer, defaults = defaults,
                     pk = list(pk) if isinstance(pk, dict) else pk, doc = doc,
                     spec = spec, selection = selection, order = order, joint = joint)
    return res
        

### global. indexed on server and db


class sql_cursor(object):
    """
    # pyg-sql
    
    pyg-sql creates sql_cursor, a thin wrapper on sql-alchemy (sa.Table), providing three different functionailities:

    - simplified create/filter/sort/access of a sql table
    - maintainance of a table where records are unique per specified primary keys
    - creation of a full no-sql like document-store

    pyg-sql supports simple joins but not much more than that.
    
    ## access simplification
    
    sqlalchemy use-pattern make Table create the "statement" and then let the engine session/connection to execute. sql_cursor keeps tabs internally of:

    - the table
    - the engine
    - the "select", the "order by" and the "where" statements

    This allows us to

    - "query and execute" in one go
    - build statements interactively, each time adding to previous "where" or "select"
    

    :Example: table creation
    ------------------------
    >>> from pyg_base import * 
    >>> from pyg_sql import * 
    >>> import datetime
    
    >>> t = sql_table(db = 'test', table = 'students', non_null = ['name', 'surname'], 
                          _id = dict(_id = int, created = datetime.datetime), 
                          nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float))
    >>> t = t.delete()


    :Example: table insertion
    -------------------------
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 48)
    >>> t = t.insert(name = 'anna', surname = 'git', age = 37)
    >>> assert len(t) == 2
    >>> t = t.insert(name = ['ayala', 'itamar', 'opher'], surname = 'gate', age = [17, 11, 16])
    >>> assert len(t) == 5

    :Example: simple access
    -----------------------
    >>> assert t.sort('age')[0].name == 'itamar'                                                     # youngest
    >>> assert t.sort('age')[-1].name == 'yoav'                                                      # access of last record
    >>> assert t.sort(dict(age=-1))[0].name == 'yoav'                                                # sort in descending order
    >>> assert t.sort('name')[::].name == ['anna', 'ayala', 'itamar', 'opher', 'yoav']
    >>> assert t.sort('name')[['name', 'surname']][::].shape == (5, 2)                              ## access of specific column(s)
    >>> assert t.distinct('surname') == ['gate', 'git']
    >>> assert t['surname'] == ['gate', 'git']
    >>> assert t[dict(name = 'yoav')] == t.inc(name = 'yoav')[0]
    
    
    :Example: access is case-insensitive
    ------------------------------------
    >>> from pyg_sql import *
    >>> t = sql_table(db = 'test', table = 'case_insensitive', nullable = dict(number = int, text = str))
    >>> self = t.delete()

    When we insert, we convert the case to the database columns convention:
        
    >>> assert t.insert_one(doc = dict(NUMBER = 1, TEXT = 'world')) == {'number': 1, 'text': 'world'}
    >>> assert t.insert_one(dict(Number = 2, Text = 'hello')) == {'number': 2, 'text': 'hello'}

    When we access the columns without specification, we get the database column names
    
    >>> assert t[0] == {'number': 1, 'text': 'world'}

    When we filter or sort, we can filter using any case for column names
    
    >>> assert len(t.inc(NUMBER = 2)) == 1

    assert t.sort('TEXT')[0] == {'number': 2, 'text': 'hello'}

    When we SELECT, we can use any case, and key-case will follow your cursor.selection case:
        
    >>> assert t[['NUMBER','TexT']][0] == {'NUMBER': 1, 'TexT': 'world'}
    >>> assert list(t[['Number','Text']].df().columns) == ['Number', 'Text']
    >>> assert sorted(t[['Number','Text']][::].columns) == ['Number', 'Text']

    :Example: simple filtering
    --------------------------
    >>> assert len(t.inc(surname = 'gate')) == 3
    >>> assert len(t.inc(surname = 'gate').inc(name = 'ayala')) == 1    # you can build filter in stages
    >>> assert len(t.inc(surname = 'gate', name = 'ayala')) == 1        # or build in one step
    >>> assert len(t.inc(surname = 'gate').exc(name = 'ayala')) == 2

    >>> assert len(t > dict(age = 30)) == 2
    >>> assert len(t <= dict(age = 37)) == 4
    >>> assert len(t.inc(t.c.age > 30)) == 2  # can filter using the standard sql-alchemy .c.column objects
    >>> assert len(t.where(t.c.age > 30)) == 2  # can filter using the standard sql-alchemy "where" statement 


    Example: simple joining
    -----------------------
    >>> from pyg import * 
    >>> students = sql_table(table = 'students', pk = ['id'], non_null = ['name', 'surname'], db = 'db')
    >>> students.insert([dict(name = 'ann', surname = 'andrews', id = 'a'),
                         dict(name = 'ben', surname = 'baxter', id = 'b'),
                         dict(name = 'charles', surname = 'cohen', id = 'c')])

    >>> subjects = sql_table(table = 'subjects', pk = ['id'], non_null = ['subject', 'teacher'], db = 'db')
    >>> subjects.insert([dict(subject = 'maths', teacher = 'mandy miles', id = 'm'),
                         dict(subject = 'geography', teacher = 'george graham', id = 'g'),
                         dict(subject = 'zoology', teacher = 'zoe zhenya', id = 'z')])

    >>> student_subject = sql_table(table = 'student_subject', non_null = {'student_id':str, 'subject_id':str, 'score': int})
    >>> student_subject.insert([dict(student_id = 'a', subject_id = 'm', score = 90),
                                dict(student_id = 'a', subject_id = 'z', score = 85),
                                dict(student_id = 'b', subject_id = 'm', score = 70),
                                dict(student_id = 'b', subject_id = 'g', score = 60),
                                dict(student_id = 'c', subject_id = 'g', score = 50),
                                dict(student_id = 'c', subject_id = 'z', score = 45),
                                ])
    

    >>> self = student_subject.join(students, dict(student_id = 'id'))

    self[::]
    id|name   |score|student_id|subject_id|surname
    a |ann    |90   |a         |m         |andrews
    a |ann    |85   |a         |z         |andrews
    b |ben    |70   |b         |m         |baxter 
    b |ben    |60   |b         |g         |baxter 
    c |charles|50   |c         |g         |cohen  
    c |charles|45   |c         |z         |cohen  

    self.inc(surname = '%co%')[::]
    dictable[2 x 6]
    id|name   |score|student_id|subject_id|surname
    c |charles|50   |c         |g         |cohen  
    c |charles|45   |c         |z         |cohen  

    >>> self.inc(score = [90,50])[::]
    dictable[2 x 6]
    id|name   |score|student_id|subject_id|surname
    a |ann    |90   |a         |m         |andrews
    c |charles|50   |c         |g         |cohen  

    ## double join
    >>> student_subject.join(students, dict(student_id = 'id')).join(subjects, dict(subject_id = 'id'))[::]
    id|id_1|name   |score|student_id|subject  |subject_id|surname|teacher      
    a |m   |ann    |90   |a         |maths    |m         |andrews|mandy miles  
    a |z   |ann    |85   |a         |zoology  |z         |andrews|zoe zhenya   
    b |m   |ben    |70   |b         |maths    |m         |baxter |mandy miles  
    b |g   |ben    |60   |b         |geography|g         |baxter |pportgeorge graham
    c |g   |charles|50   |c         |geography|g         |cohen  |george graham
    c |z   |charles|45   |c         |zoology  |z         |cohen  |zoe zhenya       

    >>> full_join = student_subject.join(students, dict(student_id = 'id')).join(subjects, dict(subject_id = 'id'))
    >>> full_join.inc(TEACHER = '%mandy%', SCORE = 90)
    sql_cursor: db.dbo.student_subject  
    SELECT student_id, subject_id, score, dbo.students.id, dbo.students.name, dbo.students.surname, dbo.subjects.id AS id_1, dbo.subjects.subject, dbo.subjects.teacher 
    FROM dbo.student_subject JOIN dbo.students ON student_id = dbo.students.id JOIN dbo.subjects ON subject_id = dbo.subjects.id 
    WHERE dbo.subjects.teacher LIKE "%mandy%" AND score = 90
    1 records

    :Example: insertion of "documents" into string columns...    
    ----------------------------------------------------------
    It is important to realise that we already have much flexibility behind the scene in using "documents" inside string columns:

    >>> t = t.delete()
    >>> assert len(t) == 0; assert t.count() == 0
    >>> import numpy as np
    >>> t.insert(name = 'yoav', surname = 'git', details = dict(kids = {'ayala' : dict(age = 17, gender = 'f'), 'opher' : dict(age = 16, gender = 'f'), 'itamar': dict(age = 11, gender = 'm')}, salary = np.array([100,200,300]), ))

    >>> t[0] # we can grab the full data back!

    {'_id': 81,
     'created': datetime.datetime(2022, 6, 30, 0, 10, 33, 900000),
     'name': 'yoav',
     'surname': 'git',
     'doc': None,
     'details': {'kids': {'ayala':  {'age': 17, 'gender': 'f'},
                          'opher':  {'age': 16, 'gender': 'f'},
                          'itamar': {'age': 11, 'gender': 'm'}},
                 'salary': array([100, 200, 300])},
     'dob': None,
     'age': None,
     'grade': None}

    >>> class Temp():
            pass
            
    >>> t.insert(name = 'anna', surname = 'git', details = dict(temp = Temp())) ## yep, we can store actual objects...
    >>> t[1]  # and get them back as proper objects on loading

    {'_id': 83,
     'created': datetime.datetime(2022, 6, 30, 0, 16, 10, 340000),
     'name': 'anna',
     'surname': 'git',
     'doc': None,
     'details': {'temp': <__main__.Temp at 0x1a91d9fd3a0>},
     'dob': None,
     'age': None,
     'grade': None}

    ## primary keys 
    
    Primary Keys are applied if the primary keys (pk) are specified. 
    Now, when we insert into a table, if another record with same pk exists, the record will be replaced.
    
    :Example: primary keys and deleted records
    ------------------------------------------
    The table as set up can have multiple items so:
    
    >>> t = t.delete()
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 46)
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 47)
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 48)
    >>> assert len(t) == 3
    
    >>> t = t.delete() 
    >>> t = sql_table(db = 'test', table = 'students', non_null = ['name', 'surname'], 
                          _id = dict(_id = int, created = datetime.datetime), 
                          nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float), 
                          pk = ['name', 'surname'])         ## <<<------- We set primary keys

    >>> t = t.delete()
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 46)
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 47)
    >>> t = t.insert(name = 'yoav', surname = 'git', age = 48)
    >>> assert len(t) == 1 
    >>> assert t[0].age == 48


    ## sql_cursor as a document store

    If we set doc = True, the table will be viewed internally as a no-sql-like document store. 

    - the nullable columns supplied are the columns on which querying will be possible
    - the primary keys are still used to ensure we have one document per unique pk
    - the document is jsonified (handling non-json stuff like dates, np.array and pd.DataFrames) and put into the 'doc' column in the table, but this is invisible to the user.

    :Example: doc management
    ------------------------
    
    We now suppose that we are not sure what records we want to keep for each student

    >>> from pyg import *
    >>> import datetime
    >>> t = sql_table(db = 'test', table = 'unstructured_students', non_null = ['name', 'surname'], 
                          _id = dict(_id = int, created = datetime.datetime), 
                          nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float), 
                          pk = ['name', 'surname'],
                          doc = True)   ##<---- The table will actually be a document store

    We are now able to keep varied structure per each record. We are only able to filter against the columns specified above

    >>> t = t.delete()
    
    >>> doc = dict(name = 'yoav', surname = 'git', age = 30, profession = 'coder', children = ['ayala', 'opher', 'itamar'])
    >>> inserted_doc = t.insert_one(doc)
    >>> assert t.inc(name = 'yoav', surname = 'git')[0].children == ['ayala', 'opher', 'itamar']

    >>> doc2 = dict(name = 'anna', surname = 'git', age = 28, employer = 'Cambridge University', hobbies = ['chess', 'music', 'swimming'])
    >>> _ = t.insert_one(doc2)
    >>> assert t[dict(age = 28)].hobbies == ['chess', 'music', 'swimming']  # Note that we can filter or search easily using the column 'age' that was specified in table. We cannot do this on 'employer'
    
    :Example: document store containing pd.DataFrames.
    ----------
    
    >>> from pyg import *
    >>> doc = dict(name = 'yoav', surname = 'git', age = 35, 
                   salary = pd.Series([100,200,300], drange(2)),
                   costs = pd.DataFrame(dict(transport = [0,1,2], food = [4,5,6], education = [10,20,30]), drange(2)))
    
    >>> t = sql_table(db = 'test', table = 'unstructured_students', non_null = ['name', 'surname'], 
                          _id = dict(_id = int, created = datetime.datetime), 
                          nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float), 
                          pk = ['name', 'surname'],
                          writer = 'c:/temp/%name/%surname.parquet', ##<---- The location where pd.DataFrame/Series are to be stored
                          doc = True)   

    >>> inserted = t.insert_one(doc)
    >>> import os
    >>> assert 'costs.parquet' in os.listdir('c:/temp/yoav/git') and ('salary.parquet' in os.listdir('c:/temp/yoav/git'))
    
    We can now access the data seemlessly:

    >>> read_from_db = t.inc(name = 'yoav')[0]     
    >>> read_from_file = pd_read_parquet('c:/temp/yoav/git/salary.parquet')
    >>> assert list(read_from_db.salary.values) == [100, 200, 300]
    >>> assert list(read_from_file.values) == [100, 200, 300]
    
    
    Session management
    ------------------
    There are three modes we want to support:
    
    launch and forget:
        t = sql_table(...)
        t.insert_one() ## run and commit
        t.insert_one() ## run and commit
        
    internal context run:
        t = sql_table(...)
        with t.connect(dry_run = True): ## session is still none, but dry_run must be set
            assert t.session is None
            t.insert_one() ## do not commit yet
            t.insert_one() ## do not commit yet
        ## committed upon __exit__ of context
        
    externally managed context:
        with Session(engine) as session:
            t = sql_table(..., session = session)
            t.insert_one() ## do not commit yet
            t.insert_one() ## do not commit
            session.commit()
    
        
        
    
    
    
    
    
    """
    
    session_maker = Session
    
    def __init__(self, table, schema = None, db = None, engine = None, server = None, session = None, dry_run = None,
                 spec = None, selection = None, order = None, joint = None, reader = None, writer = None,
                 pk = None, defaults = None, doc = None, **_):
        """
        Parameters
        ----------
        table : sa.Table
            Our table
        db : string, optional
            Name of the db where table is.
        schema : string, optional
            Name of the db schema for table.
        engine : sa,Engine, optional
            The sqlalchemy engine
        server : str , optional
            The server for the engine. If none, uses the default in pyg config file
        session: db session, optional
            An uncommitted session
        dry_run: bool
            status of the uncommitted transaction. if True, will roll-back at commit time.
        spec : sa.Expression, optional
            The "where" statement
        selection : str/list of str, optional
            The columns in "select"
        order : dict or list, optional
            The columns in ORDER BY. The default is None.
        reader :
            This is only relevant to document store            
        writer : callable/str/bool
            This is only relevant to document store and specifies how documents that contain complicated objects are transformed. e.g. Use writer = 'c:/%key1/%key2.parquet' to specify documents saved to parquet based on document keys        
        doc: bool
            Specifies if to create the table as a document store
        """
        
        table, db, schema, session, server, engine = impartial((table, db, schema, session, server, engine ))
        if is_str(table):
            ### you shouldn't really construct the table using sql_cursor, sql_table should be the entry point.
            table = sql_table(table = table, schema = schema, db = db, server = server, 
                              engine = engine, session = session, dry_run = dry_run,
                              pk = pk, doc = doc, 
                              reader = reader, writer = writer)
        
        
        if isinstance(table, sql_cursor):
            db = table.db if db is None else db
            engine = table.engine if engine is None else engine
            server = table.server if server is None else server
            session = table.session if session is None else session
            spec = table.spec if spec is None else spec
            selection = table.selection if selection is None else selection
            schema = table.schema if schema is None else schema
            order = table.order if order is None else order
            joint = table.joint if joint is None else joint
            reader = table.reader if reader is None else reader
            writer = table.writer if writer is None else writer
            dry_run = table.dry_run if dry_run is None else dry_run
            pk = table.pk if pk is None else pk
            doc = table.doc if doc is None else doc
            defaults = table.defaults if defaults is None else defaults
            table = table.table
    
        self.table = table
        self.schema = schema
        self.db = db
        self.server = server
        self.engine = engine or get_engine(db = db, server = server, sesion = session, engine = engine)
        self.session = session 
        self.spec = spec
        self.selection = selection
        self.order = order
        self.joint = joint
        self.reader = reader
        self.writer = writer
        self.pk = pk
        self.defaults = defaults
        self.doc = doc
        self.dry_run = dry_run ## indicating we are within a transaction, please keep the same session
        if session is not None:
            address = (self.server, self.db)
            if address not in SESSIONS:
                SESSIONS[address] = session

    
    def copy(self, **kwargs):
        return type(self)(self, **kwargs)

    
    def connect(self, dry_run = False, session = None):
        return self.copy(dry_run = dry_run, session = session)

    
    def groupby(self, *by, **aggregate):
        """
        returns a simple groupby of data in table
        
        >>> cursor = sql_table(server = 'DESKTOP-LU5C5QF', schema = 'dbo', db = 'test_db', table = 'test_groupby', non_null = dict(a = int, b = int, c = int), doc = False)
        >>> data = dictable(a = 1, b = [1,1,2,2], c = [1,2,3,4]) + dictable(a = 0, b = [1,1,2,2], c = [1,2,3,4])
        >>> cursor.insert_many(data)
        >>> by = ('a',)
        >>> aggregate = dict(c = 'max', b = 'min')
        >>> cursor.groupby('a', c = 'max')     

        dictable[2 x 2]
        a|c
        0|4
        1|4

        >>> cursor.groupby('a', 'b', c = 'min')     
        dictable[4 x 3]
        a|b|c
        0|1|1
        1|1|1
        0|2|3
        1|2|3

        cursor[['a','b']].groupby('a').max
        dictable[2 x 2]
        a|b
        0|2
        1|2
        
        cursor.groupby('a', 'b').min
        dictable[4 x 3]
        a|b|c
        0|1|1
        1|1|1
        0|2|3
        1|2|3
        
        cursor.groupby('a', 'b')(c = 'max')
        dictable[4 x 3]
        a|b|c
        0|1|2
        1|1|2
        0|2|4
        1|2|4
        
        cursor.drop()
        """
        by = as_list(by)
        res = sql_groupby(cursor = self, by = by)
        if aggregate:
            res = res(**aggregate)
        return res
        

    def connection(self, session = None):
        """
        Creates a valid session and attach it to the cursor
        
        Parameters:
        -----------
        dry_run: bool
            if set to True, when exiting the context, will rollback rather than commit
                    
        """
        if session is not None:
            self.session = session
            address = (self.server, self.db)
            if address not in SESSIONS:
                SESSIONS[address] = session
        if self.session is None:
            address = (self.server, self.db)        
            if address not in SESSIONS:
                SESSIONS[address] = self.session_maker(self.engine)
            return SESSIONS[address]
        else:
            return self.session
    
    def create_index(self, *columns, name = None, unique = False):
        """
        Creates an index on the table. If an existing index exists matching the same definitions, will raise rather than create the same.
        We deliberately want you NOT to specify name unless you really feel it... name defaults to sorted column names joined together
        
        Parameters:
        -----------        
        columns: strs
            names of columns in new index
            
        unique: bool
            is it a unique index

        name: str
            index name
            
        See sqlalchemy.Index for full description of the parameters above
        
        Example:
        -------
        >>> from pyg import * 
        >>> self = sql_binary_store('DESKTOP-GOQ0NSM/test_db/dbo/binary_store_indexed_on_key/%some/%other.sql').cursor
        >>> idx = self.create_index('idx_pks', 'key', unique = True)
        >>> assert len(self.table.indexes) == 1

        Example: fail where trying to create an index which is the same
        -------
        >>> import pytest
        >>> with pytest.raises(ValueError):
        >>>     idx = self.create_index('not_same_name_but_same_definition', 'key', unique = True)
        >>> assert len(self.table.indexes) == 1

        """
        table = self.table
        column_names = self._col(columns)
        name = name or '_'.join(sorted(column_names))
        ## we check for existence pre creation
        for i in table.indexes: 
            if sorted([c.name for c in i.columns]) == sorted(column_names) and i.unique == unique:
                if i.name != name:
                    raise ValueError(f'{i.name} index already exists with same keys and uniqueness')
                else:
                    return i
        columns = [table.c[col] for col in column_names]        
        return sa.Index(name, *columns, unique = unique).create(self.engine)

    
    def pooled_execute(self, statement, args = None, kwargs = None, max_workers = _MAX_WORKERS, pool_name = None, transform = None):
        """
        max_workers: bool / 0
            we can only have one worker per pool for sql, to ensure that all statements get executed
        """
        if not max_workers:
            return self.execute(statement = statement, args = args, kwargs = kwargs, transform = transform)
        session = self.connection()
        pool_name = pool_name or (self.server, self.db) ## a pool per session
        pool = executor_pool(1, pool_name)
        commit = self.session is None and self.dry_run is None
        pool.submit(session_execute, session = session, statement = statement, args = args, kwargs = kwargs, commit = commit)
        return 


    def execute(self, statement, args = None, kwargs = None, transform = None, commit = None):
        """
        executes a statement in two modes:
            if self.session exists or self.dry_run is set, we assume we are within a transaction and will simply execute using connection
            if both are None, we "lock-and-load", committing after every execution statement(s)  

        Parameters
        ----------
        statement : single or multiple sql statements
        args/kwargs: parameters for the execution statement
        transform: if you want the result of the statement to be transformed

        """
        session = self.connection()
        if commit is None:
            commit = self.session is None and self.dry_run is None
        try:
            res = session_execute(session, statement = statement, args = args, kwargs = kwargs, commit = commit)
            if transform:
                res = transform(res)
        except (sa.exc.PendingRollbackError, sa.exc.DisconnectionError, sa.exc.InvalidatePoolError) as e: ## if session has expired, we reconnect
            if self.session is None:        
                address = (self.server, self.db)            
                session = SESSIONS[address] = self.session_maker(self.engine) # invalidate the session and create a new one
                res = session_execute(session, statement = statement, args = args, kwargs = kwargs, commit = commit)
                if transform:
                    res = transform(res)
            else:
                raise e
        return res
    
    def commit(self):
        session = self.connection()
        if self.dry_run:
            session.rollback()
        else:
            session.commit()
        return self

    def rollback(self):
        session = self.connection()
        session.rollback()
        return self
        
    def __enter__(self):
        """
        context manager entry point, creating an ORM session
        
        Example:
        --------
        with sql_table(..) as t:
            t.insert()
            t.delete()
            
        """
        if self.dry_run is None: 
            self.dry_run = {} 
        connection = self.connection()
        if hasattr(connection, '__enter__'):
            connection.__enter__()
        return self


    def __exit__(self, type, value, traceback):
        """
        context manager exit point, removing the ORM session
        
        Example:
        --------
        with sql_table(..) as t:
            t.insert()
            t.delete()
            
        """
        self.commit()
        connection = self.connection()
        if hasattr(connection, '__exit__'):
            connection.__exit__(type, value, traceback)
        if self.dry_run == {}: 
            self.dry_run = None
        return self
        

    @property
    def _ids(self):
        """
        columns generated by the SQL Server and should not be provided by the user
        """
        return [c.name for c in self.table.columns if c.autoincrement]
    
    def _and(self, doc, keys):
        keys = self._col(keys)
        if len(keys) == 1:
            key = keys[0]
            return self.table.c[key] == doc[key]
        else:
            return sa.and_(*[self.table.c[i] == doc[i] for i in keys])

    def _id(self, doc):
        """
        creates a partial filter based on the document keys
        """
        pks = {i: doc[i] for i in self._pk if i in doc}
        if len(pks):
            return pks
        ids = {i : doc[i] for i in self._ids if i in doc}
        if len(ids):
            return ids
        return {}

    @property
    def nullables(self):
        """
        columns that are nullable
        """
        return [c.name for c in self.tbl.columns if c.nullable]
    
    @property
    def non_null(self):        
        """
        columns that must not be Null
        """
        return sorted([c.name for c in self.tbl.columns if c.nullable is False and c.server_default is None])

    @property
    def _columns(self):
        cols = self.columns
        return dict(zip(lower(cols), cols))


    def _kw(self, **kwargs):
        """
        converts kwargs dict into a sqlalchemy filtering expression
        usually, we just use the column names
        If the table is a document store and the key is not in the table columns, we assume the user wants to filter within the documnet
        This is a little bit of a hack since we encode all documents but works quite well for simple queries
        
        Example: query within a document store
        --------
        >>> from pyg import * 
        >>> table = partial(sql_table, db = 'test_db', schema = 'dbo', table = 'hello', nullable = 'a', pk = 'b', doc = True)
        >>> c = db_cell(a = 'a', b = 'b', ccy = 'USD', asset = 'bond', db = table)()
        >>> d = db_cell(a = 'b', b = 'c', ccy = 'EUR', asset = 'bond', db = table)()
        >>> e = db_cell(a = 'a', b = 'd', ccy = 'EUR', asset = 'bond', db = table)()

        >>> self = table()
        >>> assert len(self.inc(ccy = ['USD','EUR'])) == 3
        >>> assert len(self.inc(ccy = ['USD'])) == 1
        >>> assert len(self.inc(ccy = 'EUR')) == 2


        Example: query for a date within a doc store
        --------------------------------------------
        >>> from pyg import * 
        >>> import datetime
        >>> table = partial(sql_table, db = 'test_db', schema = 'dbo', table = 'dates', nullable = dict(a = datetime.datetime), pk = 'b', doc = True)
        >>> c = db_cell(a = dt(2000), b = 'b', ccy = dt(2000), asset = 'bond', db = table)()
        >>> d = db_cell(a = dt(2010), b = 'c', ccy = dt(2010), asset = 'bond', db = table)()
        >>> e = db_cell(a = dt(2020), b = 'd', ccy = 3, asset = 'bond', db = table)()

        >>> self = table()
        >>> assert len(self.inc(ccy = dt(2000))) == 1
        >>> assert len(self.inc(ccy = [dt(2000), dt(2010)])) == 2
        >>> assert len(self.inc(ccy = 3)) == 1
        
        """
        columns = _get_columns(self._table)
        conditions = []
        for k, v in kwargs.items():
            k = k.lower()
            if k in columns:
                col = columns[k]
                if len(col) > 1:
                    raise ValueError(f'condition {k}={v} has two columns {col} by this name so cannot satisfy')
                else:
                    col = col[0]
                conditions.append(_pw_filter(col, v))
            elif self.doc:
                col = columns[self.doc][0]
                vs = _like_within_doc(v, k)
                conditions.append(_pw_filter(col, vs))
            else:
                raise ValueError(f'column {k} not found')
        return sa.and_(*conditions)


    def _c(self, expression):
        """
        converts an expression to a sqlalchemy filtering expression
        
        :Example:
        ---------
        
        >>> expression = dict(a = 1, b = 2)
        >>> assert t._c(expression) == sa.and_(t.c.a == 1, t.c.b == 2)
        c = table.insert_many(dictable(a = ['a','aa','b', 'c'], b = ['bb','b1','c','d']))
        """
        if isinstance(expression, dict):
            return self._kw(**expression)
        elif isinstance(expression, (list, tuple)):
            return sa.or_(*[self._c(v) for v in expression])            
        else:
            return expression  
    
    @property
    def c(self):
        return self.table.c
    
            
    @property
    def _pk(self):
        """
        list of primary keys
        """
        return ulist(sorted(set(as_list(self.pk))))

    @property
    def reset(self):
        res = self.copy()
        res.spec = None
        res.selection = None
        res.order = None
        return res

    def find(self, *args, **kwargs):
        """
        This returns a table with additional filtering. note that you can build it iteratively
        
        :Parameters:
        ------------
        args: list of sa.Expression
            filter based on sqlalchemy tech
        
        kwargs: dict
            simple equality filters
        
        :Example:
        ---------
        >>> t.where(t.c.name == 'yoav')
        >>> t.find(name = 'yoav')
        
        :Example: building query in stages...
        ---------
        >>> t.inc(name = 'yoav').exc(t.c.age > 30) ## all the yoavs aged 30 or less        
        """
        if len(args) == 0 and len(kwargs) == 0:
            return self
        res = self.copy()
        if len(kwargs) > 0 and len(args) == 0:
            e = self._c(kwargs)
        elif len(args) > 0 and len(kwargs) == 0:
            e = self._c(args)
        else:
            raise ValueError('either args or kwargs must be empty, cannot have an "and" and "or" together')            
        if self.spec is None:
            res.spec = e
        else:
            res.spec = sa.and_(self.spec, e)            
        return res

    inc = find
    where = find
    filter = find
    
    def __sub__(self, other):
        """
        remove a column from the selection (select *WHAT* from table)

        :Parameters:
        ----------
        other : str/list of str
            names of columns excluded in SELECT statement
        """
        if self.selection is None:
            return self.select(self.columns - other)
        elif is_strs(self.selection):
            selection = self._col(as_list(self.selection))
            return self.select(ulist(selection) - other)
        else:
            selection = as_list(self.selection)
            other = as_list(other)
            selection = [col for col in selection if col.name not in other]
            return self.select(selection)
            
    def drop(self):
        if self.selection or self.spec:
            raise ValueError('To avoid confusing .delete and .drop, dropping a table can only be done if there is no selection and no filtering')
        logger.info('dropping table: %s.%s.%s'%(_database(self.db), 
                                                  self.schema, 
                                                  self.table.name))

        self.table.drop(bind = self.engine)

    
    def join(self, right, onclose = None, isouter = False, full = False):
        res = self.copy()
        res.joint = as_list(self.joint) + [(right, onclose, isouter, full)]
        return res

    @property
    def _table(self):
        """
        a table supporting join objects
        """
        res = self.table
        for j in as_list(self.joint):            
            right, onclose, isouter, full = j
            if isinstance(right, partial):
                right = right()
            if isinstance(right, sql_cursor):
                right = right._table
            if is_strs(onclose):
                onclose = as_list(onclose)
                onclose = dict(zip(onclose, onclose))
            if is_dict(onclose):
                lhs = _get_columns(self.table)
                rhs = _get_columns(right)
                conditions = []
                for l,r in onclose.items():
                    l = lhs[l.lower()]
                    if len(l) > 1:
                        raise ValueError(f'multiple columns of same name {l}')
                    else:
                        l = l[0]
                    if is_str(r):
                        r = rhs[r.lower()]
                        if len(r) > 1:
                            raise ValueError(f'multiple columns of same name {l}')
                        else:
                            r = r[0]
                    conditions.append(l == r)
                onclose = sa.and_(*conditions)
            res = res.join(right, onclose, isouter = isouter, full = full)
        return res

    def exc(self, *args, **kwargs):
        """
        Exclude: This returns a table with additional filtering OPPOSITE TO inc. note that you can build it iteratively
        
        :Parameters:
        ------------
        args: list of sa.Expression
            filter based on sqlalchemy tech
        
        kwargs: dict
            simple equality filters
        
        :Example:
        ---------
        >>> t.where(t.c.name != 'yoav')
        >>> t.exc(name = 'yoav') 
        
        :Example: building query in stages...
        ---------
        >>> t.inc(name = 'yoav').exc(t.c.age > 30) ## all the yoavs aged 30 or less        
        """
        if len(args) == 0 and len(kwargs) == 0:
            return self
        elif len(kwargs) > 0 and len(args) == 0:
            e = not_(self._c(kwargs))
        elif len(args) > 0 and len(kwargs) == 0:
            e = not_(self._c(args))
        else:
            raise ValueError('either args or kwargs must be empty, cannot have an "and" and "or" together')            
        res = self.copy()
        if self.spec is None:
            res.spec = e
        else:
            res.spec = sa.and_(self.spec, e)            
        return res
    
    
    def __len__(self):
        statement = select(func.count()).select_from(self._table)
        if self.spec is not None:
            statement = statement.where(self.spec)
        return self.execute(statement, transform = list, commit = False)[0][0]
    
    count = __len__


    def _col(self, column):
        """
        map column into the underlying table columns
        
        Parameter
        ----------
        column: str/list/tuple/dict
            
        """
        columns = self.columns
        cols = dict(zip(lower(columns), columns))
        if is_str(column):
            return cols[column.lower()]
        elif isinstance(column, (list, tuple)):
            return [cols[c.lower()] if isinstance(c,str) else c for c in column]
        elif isinstance(column, dict):
            return type(column)({cols[k.lower()]: v for k, v in column.items()})
        else:
            raise ValueError(f'column {column} cannot be converted to table columns')

    @property    
    def columns(self):
        return ulist([col.name for col in self.table.columns])

    def select(self, *value):
        if len(value) == 0:
            return self
        res = self.copy()
        res.selection = as_list(value)
        return res
    
    def _enrich(self, doc, columns = None):
        """
        We assume we receive a dict of key:values which go into the db.
        some of the values may in fact be an entire document
        """
        docs = {k : v for k, v in doc.items() if isinstance(v, dict)}
        columns = ulist(self.columns if columns is None else columns)
        res = type(doc)({key : value for key, value in doc.items() if key in columns}) ## These are the only valid columns to the table
        if len(docs) == 0:
            return res
        missing = {k : [] for k in columns if k not in doc}
        for doc in docs.values():
            for m in missing:
                if m in doc:
                    missing[m].append(doc[m])
        found = {}
        conflicted = {} #
        for k, v in missing.items():
            if len(v) == 1:
                found[k] = v[0]
            elif len(v) > 1:
                p = [i for i in v if is_primitive(i)]
                if len(set(p)) == 1:
                    found[k] = p[0]
                else:
                    conflicted[k] = v
        if conflicted:
            raise ValueError(f'got multiple possible values for each of these columns: {conflicted}')
        res.update(found)
        return res
    
    
    def _insert_one_statement(self, doc, ids, ignore_bad_keys = False, write = True):
        if self.defaults:
            doc.update({k : v for k,v in self.defaults.items() if k not in doc})
        doc = _relabel(res = doc, selection = self.columns, strict = False) ## we want to replace what is possible
        edoc = self._dock(doc) if write else doc
        columns = self.columns - ids
        if not ignore_bad_keys:
            bad_keys = {key: value for key, value in edoc.items() if key not in columns}
            if len(bad_keys) > 0:
                raise ValueError(f'cannot insert into db a document with these keys: {bad_keys}. The table only has these keys: {columns}')        
        doc_id = self._id(edoc)
        res = self._write_doc(edoc, columns = columns) if write else edoc
        res_no_ids = type(res)({k : v for k, v in res.items() if k not in ids}) if ids else res
                
        tbl, statement, args = None, self.table.insert(), ([res_no_ids],)
        if doc_id: ## there may be documents to remove first        
            tbl = self.inc().inc(**doc_id)
            if len(tbl):
                delete_statement = tbl._delete_statement()                
                statement, args = [delete_statement, self.table.insert()], [(), ([res_no_ids],)]
        return tbl, statement, args

        
    def insert_one(self, doc, ignore_bad_keys = False, write = True, max_workers = _MAX_WORKERS, pool_name = None):
        """
        insert a single document to the table

        Parameters
        ----------
        doc : dict
            record.
        ignore_bad_keys : 
            Suppose you have a document with EXTRA keys. Rather than filter the document, set ignore_bad_keys = True and we will drop irrelevant keys for you

        """
        ids = self._ids
        tbl, statement, args = self._insert_one_statement(doc = doc, ids = ids, ignore_bad_keys = ignore_bad_keys, write = write)
        if ids and self._pk: ## there are auto-generated ids and a unique primary keys
            self.execute(statement, args)
            ## now we grab ids
            if len(tbl):
                read = tbl.sort(ids)._read_statement() ## raw format
                read['data'] = read['data'][-1:] ## just the last document
                docs = tbl._rows_to_docs(**read, reader = False) ## do not transform the document, keep in raw format?
                latest = Dict(docs[-1])
                doc.update(latest[ids])
        else:
            self.pooled_execute(statement, args, max_workers = max_workers, pool_name = pool_name)
        return doc
    
    
    def _dock(self, doc, columns = None):
        """
        converts a usual looking document into a {self.doc : doc} format. 
        We then enrich the new document with various parameters. 
        This prepares it for "docking" in the database
        """
        
        columns = columns or self.columns
        edoc = self._enrich(doc, columns)
        if self.doc is None or self.doc is False or isinstance(edoc.get(self.doc), dict):
            return edoc
        else:
            edoc = {key: value for key, value in edoc.items() if key in columns}
            pk = self._pk + [self.doc]
            drop = [key for key in edoc if key not in pk]
            edoc[self.doc] = type(doc)({k : v for k, v in doc.items() if k not in drop})
            return edoc

    def _writer(self, writer = None, doc = None, kwargs = None):
        doc = doc or {}
        if writer is None:
            writer = doc.get(_root)
        if writer is None:
            writer = self.writer
        return as_writer(writer, kwargs = kwargs)
            
    def _write_doc(self, doc, writer = None, columns = None):
        columns = columns or self.columns
        writer = self._writer(writer, doc = doc, kwargs = doc)
        res = type(doc)({key: self._write_item(value, writer = writer) for key, value in doc.items() if key in columns})
        return res

    def _write_item(self, item, writer = None, kwargs = None):
        """
        This does NOT handle the entire document. 
        """
        if not isinstance(item, dict):
            return item
        writer = self._writer(writer, item, kwargs = kwargs)
        res = item.copy()
        for w in as_list(writer):
            res = w(res)
        if isinstance(res, dict):
            res = dumps(res)
        return res
    
    def _undock(self, doc, columns = None):
        """
        converts a document which is of the format {self.doc : doc} into a regular looking document
        """
        if self.doc is None or self.doc is False or not isinstance(doc.get(self.doc), dict):
            return Dict(doc) if type(doc) == dict else doc
        res = doc[self.doc]
        columns = columns or self.columns
        for col in ulist(columns) - self.doc:
            if col in doc and col not in res:
                res[col] = doc[col]
        return Dict(res) if type(res) == dict else res

    def _reader(self, reader = None):
        return as_reader(self.reader if reader is None else reader)

    def _read_item(self, item, reader = None, load = True):
        reader = self._reader(reader)
        res = item
        if is_str(res) and res.startswith('{') and load:
            res = loads(res)
        for r in as_list(reader):
            res = res[r] if is_strs(r) else r(res)
        return res

    def _read_row(self, row, reader = None, columns = None, load = True):
        """
        reads a tuple of values (assumed to match the  columns)
        converts them into a dict document, does not yet undock them
        """
        reader = self._reader(reader)
        res = row
        
        if isinstance(res, ROW_TYPE):
            res = tuple(res)
        if isinstance(res, (list, tuple)):
            res = type(res)([self._read_item(item, reader = reader, load = load) for item in res])
            if columns:
                if len(columns) != len(res):
                    raise ValueError('mismatch in columns')
                res = dict(zip(columns, res)) # this zip can be evil
        return res
        
    def _read_statement(self, start = None, stop = None, step = None):
        """
        returns a list of records from the database. returns a list of tuples
        """
        statement = self.statement()
        if (is_int(start) and start < 0) or (is_int(stop) and stop < 0):
            n = len(self)
            start = n + start if is_int(start) and start < 0 else start                            
            stop = n + stop if is_int(stop) and stop < 0 else stop
        if start and self.order is not None:
            statement = statement.offset(start)
            stop = stop if stop is None else stop - start
            start = None
        if stop is not None:
            statement = statement.limit(1+stop)
        data, columns = self.execute(statement, transform = _data_and_columns, commit = False)
        if start is not None or stop is not None or step is not None:
            data = data[slice(start, stop, step)]
        return dict(data = data, columns = columns)
    
    
    @property
    def df(self):
        """
        This is a more optimized, faster version for reading the table. 
        It retuns the data as a pd.DataFrame,
        In addition, it converts NUMERIC type (which is cast to decimal.Decimal) into a float

        
        Parameters
        ----------
        coerce_float: bool
            converts sql.types.NUMERIC columns into float

        Returns
        -------
        pd.DataFrame
            The data, optimized as a dataframe.

        
        Example: speed comparison
        --------

        >>> from pyg import * 
        >>> t = sql_table(db = 'db', table = 'tbl_df', server = 'DESKTOP-GOQ0NSM', nullable = dict(a = int, b = float, c = str))
        >>> rs = dictable(a = range(100)) * dict(b = list(np.arange(0,100,1.0))) * dict(c = list('abcdefghij'))
        >>> t = t.insert_many(rs)      
        
        ## reading the data as t[::] is slow...
        >>> x = timer(lambda : t[::])()
        2022-08-21 18:03:52,594 - pyg - INFO - TIMER: took 0:00:13.139333 sec
 
        ## reading the data as t.df() is much faster...
        >>> y = timer(lambda : t.df())()
        2022-08-21 18:04:24,809 - pyg - INFO - TIMER: took 0:00:00.456988 sec

        Example: access using get item
        --------
        >>> t.df[:10]
        >>> t.df[100:120]
        >>> t.df[10]
        """
        return sql_df(self)
            
    
    def _rows_to_docs(self, data, reader = None, load = True, columns = None):
        """
        starts at raw values from the database and returns a list of read-dicts (or a single dict) from the database
        """
        if columns is None:
            columns = as_list(self.selection) if self.selection else self.columns
        reader = self._reader(reader)
        if isinstance(data, list):
            return [self._read_row(row, reader = reader, columns = columns, load = load) for row in data]
        else:        
            return self._read_row(data, reader = reader, columns = columns, load = load)

    def docs(self, start = None, stop = None, step = None, reader = None):
        read = self._read_statement(start = start, stop = stop, step = step)
        docs = self._rows_to_docs(reader = reader, **read)
        return dictable(docs)
    
    def read(self, item = 0, reader = None):
        value = len(self) + item if item < 0 else item
        statement = self.statement()
        if self.order is None:
            data, columns = self.execute(statement.limit(value+1), transform = _data_and_columns, commit = False)
            row = data[value]
        else: ## can pull precisely one row given ordering
            data, columns = self.execute(statement.offset(value).limit(value+1), transform = _data_and_columns, commit = False)
            row = data[0]
        doc = self._rows_to_docs(data = row, reader = reader, columns = columns)
        rtn = self._undock(doc)
        return _relabel(rtn, self.selection)

                
    def __getitem__(self, value):
        """
        There are multiple modes to getitem:
        
        - t['key'] or t['key1', 'key2'] are equivalent to t.distinct('key') or t.distinct('key1', 'key2') 
        - t[['key1', 'key2']] will add a "SELECT key1, key2" to the statent
        - t[0] to grab a specific record. Note: this works better if you SORT the table first!, use t.sort('age')[10] to grab the name of the 11th youngest child in the class
        - t[::] a slice of the data: t.sort('age')[:10] are the 10 youngest students 
        
        :Example: support for columns case matching selection
        ---------
        >>> from pyg import * 
        >>> t = sql_table(db = 'db', table = 't', nullable = ['a', 'b'])
        >>> t.delete()
        >>> t.insert(dict(a = dict(x=1,y=2), b = 2))
        >>> assert t[['A']][::] == dictable(A = '1')
        >>> assert t[['A']].df().columns[0] == 'A'
        >>> assert t[['A']][0] == Dict(A = '1')
        >>> assert t[['A', 'b']].read(0) == Dict(A = '1', b = '2')
        """
        if isinstance(value, list):
            return self.select(value)

        elif isinstance(value, (str, tuple)):
            return self.distinct(*as_list(value))

        elif isinstance(value, dict):
            res = self.inc(**value)
            n = len(res)
            if n == 1:
                return res[0]
            elif n == 0:
                raise ValueError(f'no records found for {value}')
            else:
                raise ValueError(f'multiple {n} records found for {value}')

        elif isinstance(value, slice):
            start, stop, step = value.start, value.stop, value.step
            if step is False:
                step = None
                reader = False
            else:
                reader = None
            read = self._read_statement(start = start, stop = stop, step = step)
            docs = self._rows_to_docs(reader = reader, **read)
            columns = read['columns'] #self.columns
            res = dictable([self._undock(doc, columns = columns) for doc in docs])
            return _relabel(res, self.selection)

        elif is_int(value):
            return self.read(value)
    
    def update_one(self, doc, upsert = True, max_workers = _MAX_WORKERS, pool_name = None):
        """
        Similar to insert, except will throw an error if upsert = False and an existing document is not there
        """
        if self.defaults:
            doc.update({k : v for k,v in self.defaults.items() if k not in doc})
        existing = self.inc().inc(**self._id(doc))
        n = len(existing)
        if n == 0:
            if upsert is False:
                raise ValueError(f'no documents found to update {doc}')
            else:
                return self.insert_one(doc, max_workers = max_workers, pool_name = pool_name)
        elif self._pk:
            return self.insert_one(doc, max_workers = max_workers, pool_name = pool_name)
        elif n == 1:
            doc = _relabel(doc, self.columns, strict = False)
            edoc = self._dock(doc)
            wdoc = self._write_doc(edoc)
            ids = self._ids
            for i in ids:
                if i in wdoc:
                    del wdoc[i]
            existing.update(max_workers = 0, **wdoc) ## we need to wait to see value
            res = existing[0] 
            res.update(edoc)
            return self._undock(res)
        elif n > 1:
            raise ValueError(f'multiple documents found matching {doc}')
                
            
    def insert_many(self, docs, write = True, max_workers = _MAX_WORKERS, pool_name = None):
        """
        insert multiple docs. 

        Parameters
        ----------
        docs : list of dicts, dictable, pd.DataFrame
        """
        ids = self._ids
        rs = dictable(docs)
        if self.defaults:
            rs = rs(**{k : v for k,v in self.defaults.items() if k not in rs.keys()})
        rs = _relabel(rs, self.columns, strict = False)
        if len(rs) > 0:
            if False:
                statement = []
                args = []
                for doc in rs:        
                    tbl, statement_, args_ = self._insert_one_statement(doc = doc, ids = ids, write = write)
                    if isinstance(statement_, list):
                        statement.extend(statement_)
                        args.extend(args_)
                    else:
                        statement.append(statement_)
                        args.append(args_)                    
                self.pooled_execute(statement, args, kwargs = None, max_workers = max_workers, pool_name = pool_name)
            else:
                for doc in rs:        
                    tbl, statement_, args_ = self._insert_one_statement(doc = doc, ids = ids, write = write)
                    self.pooled_execute(statement_, args_, kwargs = None, max_workers = max_workers, pool_name = pool_name)
        return self

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __add__(self, item):
        """
        if item is dict, equivalent to insert_one(item)
        if item is dictable/pd.DataFrame, equivalent to insert_many(item)
        """
        if is_dict(item) and not is_dictable(item):
            self.insert_one(item)
        else:
            self.insert_many(item)
        return self

        
    def insert(self, data = None, columns = None, max_workers = _MAX_WORKERS, pool_name = None, **kwargs):
        """
        This allows an insert of either a single row or multiple rows, from anything like 

        :Example:
        ----------
        >>> self.insert(name = ['father', 'mother', 'childa'], surname = 'common_surname') 
        >>> self.insert(pd.DataFrame(dict(name = ['father', 'mother', 'childa'], surname = 'common_surname')))
        
        
        Since we also support doc management, there is a possibility one is trying to enter a single document of shape dict(name = ['father', 'mother', 'childa'], surname = 'common_surname')
        We force the user to use either insert_one or insert_many in these cases.
        
        """
        rs = dictable(data = data, columns = columns, **kwargs) ## this allows us to insert multiple rows easily as well as pd.DataFrame
        if self.defaults:
            rs = rs(**{k : v for k,v in self.defaults.items() if k not in rs.keys()})
        if len(rs)>1:
            pk = self._pk
            if pk:
                u = rs.listby(self._pk)
                u = dictable([row for row in u if len((row - pk).values()[0])>1])
                if len(u):
                    u = u.do(try_back(unique))                
                    u0 = u[0].do(try_back(unique))
                    u0 = {k:v for k,v in u0.items() if k in self._pk or isinstance(v, list)}
                    raise ValueError('When trying to convert data into records, we detected multiple rows with same unique %s, e.g. \n\n%s\n\nCan you please use .insert_many() or .insert_one() to resolve this explicitly'%(self._pk, u0))
            elif self.doc:
                if isinstance(data, list) and len(data) < len(rs):
                    raise ValueError('Original value contained %s rows while new data has %s.\n We are unsure if you are trying to enter documents with list in them.\nCan you please use .insert_many() or .insert_one() to resolve this explicitly'%(len(data), len(rs)))
                elif isinstance(data, dict) and not isinstance(data, dictable):
                    raise ValueError('Original value provided as a dict while now we have %s multiple rows.\nWe think you may be trying to enter a single document with lists in it.\nCan you please use .insert_many() or .insert_one() to resolve this explicitly'%len(rs))
        return self.insert_many(rs, max_workers = max_workers, pool_name = pool_name)

    def find_one(self, doc = None, *args, **kwargs):
        res = self.find(*args, **kwargs)
        if doc:
            filter_by_doc = self._id(doc)
            if filter_by_doc is not None:
                res = res.find(filter_by_doc)
        if len(res) == 1:
            return res
        elif len(res) == 0:
            raise ValueError('no document found for %s %s %s'%(doc, args, kwargs))
        elif len(res) > 1:
            raise ValueError('multiple documents found for %s %s %s'%(doc, args, kwargs))
    
    
    def _select(self):
        """
        performs a selection based on self.selection

        >>> from pyg import *        
        >>> t1  = sql_table(table = 't1', nullable = ['a', 'b', 'c'], db = 'db')
        >>> t1.insert(dictable(a = 'a', b = ['a','b','c'], c = 'hi'))
        t1[['a','b']].inc(B = 'b')

        >>> t2  = sql_table(table = 't2', nullable = ['x', 'y', 'z'], db = 'db')
        >>> t2.insert(dictable(x = 'x', y = ['a','b','c'], z = 'world'))

        >>> t4  = sql_table(table = 't4', nullable = ['a', 'b', 'c'], db = 'db')
        >>> t4.insert(dictable(a = 'u', b = ['a','b','c'], c = 'beautiful'))

        t1.join(t2, t1.c['b']==t2.c['y'])[['a', 'b', 'z']][::]
        t1.join(t4, 'b').inc(a = 'a')
        [col.name for col in t1.table.join(t4.table, t1.c.b==t4.c.b).columns]
        """
        table = self._table
        if self.selection is None:
            return select(table)
        columns = _get_columns(self._table)
        selection = []
        for col in as_list(self.selection):
            if is_str(col):
                col = col.lower()
                if col in columns:
                    s = columns[col]
                    if len(s) > 1:
                        raise ValueError(f'{col} is in two tables {s} so cannot resolve')
                    else:
                        s = s[0]                        
                    selection.append(s)
                else:
                    raise ValueError(f'{col} not found in the table')
            else:
                selection.append(col) ## it is assumed to be a sqlalchemy selection
        statement = select(selection).select_from(table)
        return statement

    
    def statement(self):
        """
        We build a statement from self.spec, self.selection and self.order objects
        A little like:
        
        >>> self.table.select(self.selection).where(self.spec).order_by(self.order)

        """
        statement = self._select()            
        if self.spec is not None:
            statement = statement.where(self.spec)
        if self.order is not None:
            order = self.order
            cols = _get_columns(self._table)
            if isinstance(order, (str,list)):
                order = {o: 1 for o in as_list(order)}
            order_by = []
            for k, v in order.items():
                col = cols[k.lower()]
                if len(col) > 1:
                    raise ValueError(f'cannot sort as multiple col {k} in join: {col}')
                else:
                    order_by.append(_orders[v](col[0]))
            statement = statement.order_by(*order_by)
        return statement
    
    def update(self, max_workers = _MAX_WORKERS, pool_name = None, **kwargs):
        if len(kwargs) == 0:
            return self
        statement = self.table.update()
        if self.spec is not None:
            statement = statement.where(self.spec)
        statement = statement.values(kwargs)
        self.pooled_execute(statement = statement, max_workers = max_workers, pool_name = pool_name)
        return self

    set = update

    def full_delete(self, max_workers = 0, pool_name = None):
        statement = self._delete_statement()
        self.pooled_execute(statement = statement, max_workers = max_workers, pool_name = pool_name)
        return self
        

    def _delete_statement(self):
        statement = self.table.delete()
        if self.spec is not None:
            statement = statement.where(self.spec)
        return statement

    def delete(self, *df, **inc):
        """
        removes values in current table. 
        
        :Parameters:
        -------------
        inc: dict
            values to filter table. simple filter: e.g. inc = dict(stock = 'AAPL') will remove AAPL data only.
            
        df: dictable/dataframe                
            You can further restrict what is deleted using df. e.g. if df is:

                date               
                2023-01-07 00:00:00
                2023-01-08 00:00:00
                2023-01-09 00:00:00
                ...11 rows...
                2023-01-15 00:00:00
                2023-01-16 00:00:00
                2023-01-17 00:00:00
            Then we remove all dates between 7th and 17th of Jan 2023
            
            Why df? chunking!
            In principle, .delete(date = list(df.date.values)) does the same.
            df filter is done in "CHUNKS" to ensure that the SQL statement does not become too lonh            
        """
        res = self.inc(**inc)
        if len(df) > 1:
            raise ValueError('only a single dataframe/dictable for filtering is supported')
        elif len(df) == 1:
            rs = dictable(df[0])
            for i in range(0, len(rs), _CHUNK):
                res.inc(list(rs[i:i+_CHUNK])).delete()
            return self
        if len(res) > 0:
            res.full_delete()
        return self

    def sort(self, *order, **orderby):
        """
        Allows sorting of the table by multiple methods:
            
        :Example:
        ---------
        >>> t = sql_table(db = 'db', table = 'tbl_df', server = 'DESKTOP-GOQ0NSM', nullable = dict(a = int, b = float, c = str))
        >>> t.sort(a = 'desc', b = 'asc')
        >>> t.sort(dict(a = 'desc', b = 'asc'))
        >>> t.sort(a = -1, b = 1)
        >>> t.sort('a', 'b')
        >>> t.sort(['a', 'b'])
        """        
        order = as_list(order)
        if len(order) == 1 and isinstance(order[0], dict):
            order = order[0]
        if len(orderby):
            if isinstance(order, dict):
                order.append(orderby)
            elif len(order) == 0:
                order = orderby
            else:
                raise ValueError('cannot mix order and orderby')
        if not order:
            return self
        else:
            res = self.copy()
            res.order = order
            return res
        
    @property
    def name(self):
        """
        table name
        """
        return self.table.name
    
    
    def distinct(self, *keys):
        """
        select DISTINCT *keys FROM TABLE
        """
        keys = as_list(keys)
        if len(keys) == 0 and self.selection is not None:
            keys = as_list(self.selection)
        if is_strs(keys):
            keys = self._col(keys)
            keys = [self.table.columns[k] for k in keys]
        query = self.connection().query(*keys)
        if self.spec is not None:
            query = query.where(self.spec)        
        res = query.distinct().all()
        if len(keys)==1:
            res = [row[0] for row in res]
        return res
    
    def __getattr__(self, attr):
        if attr.lower() in self._columns:
            return self.distinct(attr)
        else:
            raise AttributeError(f"'sql_cursor' object has no attribute '{attr}'")
    
    def __repr__(self):
        statement = self.statement()
        params = statement.compile().params
        prefix = '%s.%s'%(self.schema, self.table.name) if self.schema else self.table.name
        text = str(statement).replace('"','').replace(prefix + '.','')
        for k,v in params.items():
            text = text.replace(':'+k, '"%s"'%v if is_str(v) else dt2str(v) if is_date(v) else str(v))
            
        res = 'sql_cursor: %(db)s.%(table)s%(pk)s %(doc)s %(w)s\n%(statement)s\n%(n)i records'%dict(db = _database(self.db), table = prefix, 
                                                                                              doc = 'DOCSTORE[%s]'%self.doc if self.doc else '', 
                                                                                              w = '\nwriter: %s\n'%self.writer if is_str(self.writer) else '',
                                                                                              pk = self._pk if self._pk else '', 
                                                                                              n = len(self), 
                                                                                              statement = text)
        return res

                
    @property
    def address(self):
        """
        :Returns:
        ---------
        tuple
            A unique combination of the server address, db name and table name, identifying the table uniquely. This allows us to create an in-memory representation of the data in pyg-cell

        """
        return ('server', self.server), ('db', self.db), ('schema', self.schema), ('table', self.table.name)


    def read_sql(self, inc = None, columns = None, index = None, coerce_float : bool = True, duplicate = None, sort = None, **more_inc):
        """
        reads a dataframe from a table

        Parameters
        ----------
        inc : dict, optional
            filtering the table as "include". The default is None.
        columns : str/list/dict, optional
            Names of the columns. The default is None, which returns all columns except the one in filters or in index
        index : str, optional
            name of the index.
        coerce_float : bool, optional
            When reading decimals, coerce into float? The default is True.
        duplicate : str/callable, optional
            how to handle duplicates per index value. The default is None.
        sort: str, optional
            when we have duplicate values, what should we sort them by? This is useful for point-in-time
            
        
        Example:
        --------
        >>> from pyg import * 
        >>> import datetime
        >>> self = sql_table(table = 'stock_prices', db = 'test_db', schema = 'dbo', server = 'DESKTOP-LU5C5QF',
                             non_null = dict(country = str, stock = str, 
                                             date = datetime.datetime, 
                                             price = float, 
                                             volume = int))

        >>> self.delete()
        
        ### we start by inserting some data...

        >>> aapl = pd.DataFrame(dict(price = np.random.normal(0,1,1000),
                                     volume = np.random.randint(0,100, 1000)), drange(-999))

        >>> aapl['stock'] = 'aapl'; aapl['country'] = 'US'
        >>> aapl.to_sql(name = 'stock_prices', con = self.engine, schema = 'dbo', 
                      if_exists = 'append', index = True, index_label = 'date')

        >>> ibm = pd.DataFrame(dict(price = np.random.normal(0,1,1000),
                                     volume = np.random.randint(0,100, 1000)), drange(-999))

        >>> ibm['stock'] = 'ibm'; ibm['country'] = 'US'
        >>> ibm.to_sql(name = 'stock_prices', con = self.engine, schema = 'dbo', 
                      if_exists = 'append', index = True, index_label = 'date')

        ### now let us read the data:
        
        >>> self.read_sql(inc= dict(stock = 'aapl', country = 'us'), index = 'date')
        
                       price  volume
        date                        
        2020-04-22  0.361650      95
        2020-04-23  1.328385      71
        2020-04-24  0.492578      65
        2020-04-25  0.111558      85
        2020-04-26 -0.242754      13
                     ...     ...
        2023-01-12  2.271275      60
        2023-01-13  0.042965       9
        2023-01-14  0.421812      83
        2023-01-15 -1.024092      83
        2023-01-16 -0.820798      40
        
        >>> self.read_sql(inc = dict(stock = 'aapl', country = 'us'), index = 'date', columns = 'price')
        >>> self.read_sql(stock = 'aapl', country = 'us', index = 'date', columns = 'price') ## same
        
        date
        2020-04-22    0.361650
        2020-04-23    1.328385
        2020-04-24    0.492578
        2020-04-25    0.111558
        2020-04-26   -0.242754
          
        2023-01-12    2.271275
        2023-01-13    0.042965
        2023-01-14    0.421812
        2023-01-15   -1.024092
        2023-01-16   -0.820798
        Name: price, Length: 1000, dtype: float64        
        

        Example: handling duplicates
        ----------------------------
        There are three methods: 
        1) ignore duplicates and grab all the data:
    
        >>> self.read_sql(country = 'us', index = 'date')
                   stock     price  volume
        date                              
        2020-04-22  aapl -0.984502      24
        2020-04-22   ibm  1.053406      58
        2020-04-23  aapl -0.768723      28
        2020-04-23   ibm  1.008445      66
        2020-04-24  aapl -0.187649      12
                 ...       ...     ...
        2023-01-14  aapl  0.234413      43
        2023-01-15   ibm -0.297252      15
        2023-01-15  aapl  0.901687      77
        2023-01-16  aapl  1.248626      79
        2023-01-16   ibm -0.580478      11


        2) make duplicates raise an error:

        >>> self.read_sql(country = 'us', index = 'date', duplicate = True)

        
        File "C:\github\pyg-sql\src\pyg_sql\_sql_table.py", line 2209, in read_sql
            raise ValueError(f'found duplicate entries: {n}')
        
        ValueError: found duplicate entries:             
                    stock  price  volume
        date                            
        2020-04-22      2      2       2
        2020-04-23      2      2       2
        2020-04-24      2      2       2
        2020-04-25      2      2       2
        2020-04-26      2      2       2

        3) create a simple function that can resolve the dataframe on each date:
            
        self.read_sql(country = 'us', index = 'date', duplicate = lambda df: df[df.stock == 'aapl'].iloc[0])

                   stock     price  volume       date
        date                                         
        2020-04-23  aapl  0.868748      63 2020-04-23
        2020-04-24  aapl  0.549170      83 2020-04-24
        2020-04-25  aapl -0.106516      70 2020-04-25
        2020-04-26  aapl  1.390382      99 2020-04-26
        2020-04-27  aapl  1.803603      61 2020-04-27
                 ...       ...     ...        ...
        2023-01-13  aapl -0.409951       8 2023-01-13
        2023-01-14  aapl  0.012633       2 2023-01-14
        2023-01-15  aapl  0.998084      11 2023-01-15
        2023-01-16  aapl -0.600712      95 2023-01-16
        2023-01-17  aapl  0.582729      80 2023-01-17
        
        4) use "sort" param to pick the value you want. This is usually applied using point-in-time
        self.read_sql(country = 'US', index = 'stock', sort = 'date', duplicate = 'last')

        Out[8]: ## last values for all US stocks:
                    date     price  volume
        stock                             
        aapl  2023-01-17  0.582729      80
        ibm   2023-01-17  1.518480      99


        Example: multiple index
        ---------------
        >>> self.read_sql(country = 'us', index = ['stock', 'date'], duplicate = True)
        
                             price  volume
        stock date                        
        aapl  2020-04-22 -0.984502      24
              2020-04-23 -0.768723      28
              2020-04-24 -0.187649      12
              2020-04-25 -0.799186       7
              2020-04-26 -0.363066      84
                           ...     ...
        ibm   2023-01-12  0.224751      23
              2023-01-13 -0.831409      93
              2023-01-14  0.770350      65
              2023-01-15 -0.297252      15
              2023-01-16 -0.580478      11
              
        >>> self.read_sql(country = 'us', columns = 'price', index = ['stock', 'date'])

        stock  date      
        aapl   2020-04-22   -0.984502
               2020-04-23   -0.768723
               2020-04-24   -0.187649
               2020-04-25   -0.799186
               2020-04-26   -0.363066
          
        ibm    2023-01-12    0.224751
               2023-01-13   -0.831409
               2023-01-14    0.770350
               2023-01-15   -0.297252
               2023-01-16   -0.580478
        Name: price, Length: 2000, dtype: float64
        """
        inc = inc or {}
        inc.update(more_inc)
        idx = as_list(index)
        if columns is None:
            cols = ulist(self.columns) - list(inc.keys()) - idx
        if isinstance(columns, (str, list)):
            cols = {col : col for col in as_list(columns)}
        else:
            cols = columns
        if len(cols) == 0:
            raise ValueError('no columns selected to load')            
        table = self.inc(**inc)[ ulist(cols.keys()) + idx]
        if sort:
            table = table.sort(sort)
        df = table.df(coerce_float = coerce_float)            
        if idx and len(df):
            if duplicate is None or duplicate is False:   
                res = df.set_index(idx).sort_index()
            elif duplicate is True or duplicate == 'fail':
                n = df.groupby(idx).count()
                if len(n) < len(df):
                    n = n[n[cols[0]]>1]
                    raise ValueError(f'found duplicate entries: {n}')
                else:
                    res = df.set_index(idx).sort_index()
            else:
                res = df.groupby(idx).apply(duplicate)
        else:
            res = df
        return res[columns] if is_str(columns) else res

        
    def to_sql(self, df, index = None, columns = None, series = None, method = None, inc = None, duplicate = None, sort = None, chunksize = None, upload_xor = True, **more_inc):
        """
        :Parameters:
        -------------
        df: pd.DataFrame or pd.Series
        
        index: str or None
            name of the index
        
        inc: dict
            a filter to define uniqueness
        
        series: str or None:
            name of the column if df is a series
            
        method: str or callable.
            We are going to allow various methods of sending data to sql...

        columns: None or dict
            you may need to rename some columns prior to insertion. 

        1) insert: ignore any duplicates with existing data
        2) replace: delete any existing data based on params, and then insert

        For the remaining choice of methods, we have to think how we handle duplicates by index:
        e.g. for the same date we have two prices: existing in table and new.
        NOTE: update/append require the index to be provided and enforce uniqueness of the index on the dataframe provided
    
        3) update:  delete existing duplicates FROM THE SQL TABLE and insert new ones
        4) append:  delete duplicates FROM THE new dataframe provided 
        5) some callable(new, existing) function to merge the newly provided dataframe and existing data. 
    
        upload_xor: bool
            We may be interested in using a dataframe ONLY for values that exist in the database.
            i.e. we want to update/merge with existing values but we don't want to add new records
            xor is the data that is in new but not in existing and if upload_xor = False, this data is dropped
            
        duplicate: str
            Only used if method is callable and a dataframe of the existing data in table needs to be read using self.read_sql()
            see sql_cursor.read_sql for explanation
        
        sort: str
            Only used if method is callable and a dataframe of the existing data in table needs to be read using self.read_sql()
            see sql_cursor.read_sql for explanation
        
        chunksize: int/None
            chunksize used to push the data to the SQL database see pandas df.to_sql() for explanation

        Example:
        ---------

        >>> from pyg import * 
        >>> server = 'DESKTOP-GOQ0NSM' # 'DESKTOP-LU5C5QF'
        >>> import datetime
        >>> self = sql_table(table = 'stock_prices', db = 'test_db', schema = 'dbo', server = server, create = True,
                             non_null = dict(country = str, stock = str, 
                                             date = datetime.datetime, 
                                             price = float, 
                                             volume = int))

        >>> self.delete()
    
        Example: simple insert
        ----------------------
        >>> inc = dict(stock = 'tsla', country = 'US'); index = 'date'; kwargs = {}
        >>> tsla = pd.DataFrame(dict(price = np.random.normal(0,1,1000),
                                             volume = np.random.randint(0,100, 1000)), drange(-999))
        >>> encoded = self.to_sql(tsla, country = 'US', stock = 'tsla', index = 'date')
        >>> decode(encoded)        
                
                       price  volume country stock
        date                                      
        2020-04-22 -1.761752       6      US  tsla
        2020-04-23 -0.110846      99      US  tsla
        2020-04-24  0.758875       5      US  tsla
        2020-04-25 -1.028290      63      US  tsla
        2020-04-26 -0.601785      64      US  tsla
                     ...     ...     ...   ...
        2023-01-12 -1.168227      99      US  tsla
        2023-01-13 -0.026940      74      US  tsla
    
        >>> self
        Out[4]: 
        sql_cursor: test_db.dbo.stock_prices  
        SELECT country, stock, date, price, volume 
        FROM dbo.stock_prices
        1000 records 
   
        Example: method = 'insert': ignoring what's already in the table, or any potential duplicates...
        --------
        >>> prices_later = pd.DataFrame(dict(price = np.random.normal(0,1,500),
                                             volume = np.random.randint(0,100, 500)), drange(1,500))
        >>> full_history = pd.concat([tsla, prices_later])
        >>> self.to_sql(full_history, country = 'US', stock = 'tsla', index = 'date')        

        >>> self    
        sql_cursor: test_db.dbo.stock_prices  
        SELECT country, stock, date, price, volume 
        FROM dbo.stock_prices
        2500 records  ### <-------- We now have duplicate data for the history        

        Example: method = 'replace': replacing completely existing data
        -----------------------------
        >>> method = 'replace'
        >>> self.to_sql(prices_later, country = 'US', stock = 'tsla', index = 'date', method = 'replace')        
        >>> assert len(self) == 500
        
        Example: method = 'append': keep existing values but add new ones:
        ---------
        >>> full_history_bad = full_history.copy()
        >>> full_history_bad['price'] = 0.0
        >>> df = full_history_bad; inc = dict(country = 'US', stock = 'tsla'); index = 'date'; method = 'append'
        >>> self.to_sql(df = full_history_bad, country = 'US', stock = 'tsla', index = 'date', method = 'append')
        
        >>> assert len(self.inc(stock = 'tsla')) == 1500 ## we are back to full history
        >>> assert len(self.inc(stock = 'tsla').exc(price = 0)) == 500 ## we have the original good values
        
        Example: method = 'update': overwrite table duplicates:
        ---------
        >>> full_history_bad['price'] = 1.0
        >>> df = full_history_bad.iloc[:1250]
        >>> inc = dict(country = 'US', stock = 'tsla'); index = 'date'
        >>> method = lambda new, existing: new
        >>> self.to_sql(df = full_history_bad, country = 'US', stock = 'tsla', index = 'date', method = 'update')
        >>> assert len(self.inc(stock = 'tsla')) == 1500 ## we are back to full history
        >>> assert len(self.inc(stock = 'tsla').exc(price = 1)) == 250 ## we overwrote the ones that are duplicate by index
                
        """
        return pd_to_sql(df = df, table = self.name, db = self.db, server = self.server, index = index, columns = columns,
                  series = series, method = method, inc = inc, 
                  duplicate = duplicate, sort = sort, chunksize = chunksize, 
                  upload_xor = upload_xor, **more_inc)



def pd_to_sql(df, table = None, db = None, server = None, schema = None, index = None, columns = None,
              series = None, method = None, inc = None, session = None, engine = None,
              duplicate = None, sort = None, chunksize = None, 
              upload_xor = True, **more_inc):
    """
    
    
    :Parameters:
    -------------
    table, db, server, schema: str or partial 
        see sql_table constructor

    df: pd.DataFrame or pd.Series
    
    index: str or None
        name of the index
    
    inc: dict
        a filter to define uniqueness
    
    series: str or None:
        name of the column if df is a series
        
    method: str or callable.
        We are going to allow various methods of sending data to sql...

    columns: None or dict
        you may need to rename some columns prior to insertion. 

    1) insert: ignore any duplicates with existing data
    2) replace: delete any existing data based on params, and then insert

    For the remaining choice of methods, we have to think how we handle duplicates by index:
    e.g. for the same date we have two prices: existing in table and new.
    NOTE: update/append require the index to be provided and enforce uniqueness of the index on the dataframe provided

    3) update:  delete existing duplicates FROM THE SQL TABLE and insert new ones
    4) append:  delete duplicates FROM THE new dataframe provided 
    5) some callable(new, existing) function to merge the newly provided dataframe and existing data. 

    upload_xor: bool
        We may be interested in using a dataframe ONLY for values that exist in the database.
        i.e. we want to update/merge with existing values but we don't want to add new records
        xor is the data that is in new but not in existing and if upload_xor = False, this data is dropped
        
    duplicate: str
        Only used if method is callable and a dataframe of the existing data in table needs to be read using self.read_sql()
        see sql_cursor.read_sql for explanation
    
    sort: str
        Only used if method is callable and a dataframe of the existing data in table needs to be read using self.read_sql()
        see sql_cursor.read_sql for explanation
    
    chunksize: int/None
        chunksize used to push the data to the SQL database see pandas df.to_sql() for explanation

    Example:
    ---------

    >>> from pyg import * 
    >>> server = 'DESKTOP-GOQ0NSM' # 'DESKTOP-LU5C5QF'
    >>> db = 'test_db'; schema = 'dbo'
    >>> import datetime
    >>> self = sql_table(table = 'stock_prices', db = 'test_db', schema = 'dbo', server = server, create = True,
                         non_null = dict(country = str, stock = str, 
                                         date = datetime.datetime, 
                                         price = float, 
                                         volume = int))

    >>> self.delete()

    Example: simple insert
    ----------------------
    >>> inc = dict(stock = 'tsla', country = 'US'); index = 'date'; kwargs = {}
    >>> tsla = pd.DataFrame(dict(price = np.random.normal(0,1,1000),
                                         volume = np.random.randint(0,100, 1000)), drange(-999))
    >>> encoded = pd_to_sql(df = tsla, db = db, server = server, schema = schema, table = 'stock_prices', country = 'US', stock = 'tsla', index = 'date')
    >>> assert eq(decode(encoded), tsla)
            
                   price  volume country stock
    date                                      
    2020-04-22 -1.761752       6      US  tsla
    2020-04-23 -0.110846      99      US  tsla
    2020-04-24  0.758875       5      US  tsla
    2020-04-25 -1.028290      63      US  tsla
    2020-04-26 -0.601785      64      US  tsla
                 ...     ...     ...   ...
    2023-01-12 -1.168227      99      US  tsla
    2023-01-13 -0.026940      74      US  tsla

    >>> self
    Out[4]: 
    sql_cursor: test_db.dbo.stock_prices  
    SELECT country, stock, date, price, volume 
    FROM dbo.stock_prices
    1000 records 
   
    Example: method = 'insert': ignoring what's already in the table, or any potential duplicates...
    --------
    >>> prices_later = pd.DataFrame(dict(price = np.random.normal(0,1,500),
                                         volume = np.random.randint(0,100, 500)), drange(1,500))
    >>> full_history = pd.concat([tsla, prices_later])
    >>> self.to_sql(full_history, country = 'US', stock = 'tsla', index = 'date')        

    >>> self    
    sql_cursor: test_db.dbo.stock_prices  
    SELECT country, stock, date, price, volume 
    FROM dbo.stock_prices
    2500 records  ### <-------- We now have duplicate data for the history        

    Example: method = 'replace': replacing completely existing data
    -----------------------------
    >>> method = 'replace'
    >>> pd_to_sql(prices_later, table = self, country = 'US', stock = 'tsla', index = 'date', method = 'replace')        
    >>> assert len(self) == 500
    
    Example: method = 'append': keep existing values but add new ones:
    ---------
    >>> full_history_bad = full_history.copy()
    >>> full_history_bad['price'] = 0.0
    >>> df = full_history_bad; inc = dict(country = 'US', stock = 'tsla'); index = 'date'; method = 'append'
    >>> self.to_sql(df = full_history_bad, country = 'US', stock = 'tsla', index = 'date', method = 'append')
    
    >>> assert len(self.inc(stock = 'tsla')) == 1500 ## we are back to full history
    >>> assert len(self.inc(stock = 'tsla').exc(price = 0)) == 500 ## we have the original good values
    
    Example: method = 'update': overwrite table duplicates:
    ---------
    >>> full_history_bad['price'] = 1.0
    >>> df = full_history_bad.iloc[:1250]
    >>> inc = dict(country = 'US', stock = 'tsla'); index = 'date'
    >>> method = lambda new, existing: new
    >>> self.to_sql(df = full_history_bad, country = 'US', stock = 'tsla', index = 'date', method = 'update')
    >>> assert len(self.inc(stock = 'tsla')) == 1500 ## we are back to full history
    >>> assert len(self.inc(stock = 'tsla').exc(price = 1)) == 250 ## we overwrote the ones that are duplicate by index
            
    """
    
    inc = inc or {}
    inc.update(more_inc)
    index = index or df.index.name or 'index'
    idx = as_list(index)
    res = pd.DataFrame(df)
    series = series or 'value'
    if isinstance(df, pd.Series):
        res.columns = [series]
    res.columns = [str(col) for col in res.columns]
    if columns is None:
        columns = columns_ = {col : col for col in res.columns}
        if len(columns) < len(res.columns):
            raise ValueError(f'cannot insert non unique columns {res.columns}')
    elif isinstance(columns, list):
        columns = columns_ = {col : col for col in columns}
    elif isinstance(columns, str):
        columns = columns_ = {columns: columns}
    else: ## we are renaming columns as well as selecting them
        columns_ = {v: k for k, v in columns.items()}
        if len(columns_) < len(columns):
            raise ValueError('cannot insert non-unique column ids')
    res = res[list(columns.keys())].rename(columns = columns)
    rtn = dict(_obj = pd_read_sql, 
               server = server,
               db = db,
               schema = schema,
               table = table,
               inc = inc, 
               columns = series if isinstance(df, pd.Series) else columns_, 
               index = index,
               duplicate = duplicate, sort = sort)
    if len(res) == 0:
        return rtn
    res.index.name = index
    for k, v in inc.items():
        res[k] = v
    if isinstance(table, (partial, sql_cursor)):
        existing_table = sql_table(server = server, db = db, schema = schema, table = table, engine = engine, session = session)
        table = existing_table.table
    else:
        engine = _get_engine(server = server, db = db, schema = schema, session = session, engine = engine, create = False)
        con = session or engine
        if not sa.inspect(engine).has_table(table):
            res.to_sql(name = table, con = con, schema = schema, chunksize = chunksize, 
                       if_exists = 'append', index = True if index else False, index_label = index)
            return rtn
        else:
            existing_table = sql_table(server = server, db = db, schema = schema, table = table, engine = engine, session = session)
    engine = existing_table.engine
    con = session or existing_table.engine
    if method is None or method == 'insert': # I don't care about duplicates
        res.to_sql(name = table, con = con, schema = schema, chunksize = chunksize, 
                   if_exists = 'append', index = True if index else False, index_label = index)
        return rtn
    existing_table = sql_table(server = server, db = db, schema = schema, table = table, session = session, engine = engine)
    if method == 'replace':
        existing_table.inc(**inc).delete()
        res.to_sql(name = table, con = con, schema = schema, chunksize = chunksize, 
                   if_exists = 'append', index = True if index else False, index_label = index)
        return rtn
    if len(idx) == 0:
        raise ValueError(f'cannot {method} based on an index, unless an index is provided')
    n = res.groupby(idx).count()
    if len(n) < len(res):
        duplicates = n[n.iloc[:,0]>1]
        if len(duplicates) > 0:
            raise ValueError(f'The dataframe provided contains duplicates: {duplicates}')
    if method in ('update', 'append'):
        existing = existing_table.inc(**inc)[idx].df().set_index(idx).sort_index() ## all we care are the idx
    elif callable(method):
        #existing = self.inc(**inc).df().set_index(idx).sort_index() ## all we care are the idx
        existing = existing_table.read_sql(inc = inc, index = index, duplicate = duplicate, sort = sort)
    else:
        raise ValueError(f'unknown method: {method}. method needs to be append/insert/replace/update or a callable function')            
    if len(existing) == 0:
        res.to_sql(name = table, con = con, schema = schema, chunksize = chunksize, 
                   if_exists = 'append', index = True, index_label = index)
        return rtn
    duplicates = sorted(set(res.index) & set(existing.index))
    if not upload_xor:
        res = res.loc[duplicates]
        if len(res) == 0:
            return rtn
    if len(duplicates) == 0:
        pass
    elif method == 'append': ## we remove duplicates from the new data
        res = res.drop(duplicates)
    elif method == 'update': ## we remove duplicates from the table
        to_delete = dictable(duplicates, idx)
        existing_table.delete(to_delete, **inc)
    elif callable(method):
        new = res.loc[duplicates]
        existing = existing.loc[duplicates]
        res = res.drop(duplicates)
        merged = method(new, existing)
        res = pd.concat([res, merged])
        to_delete = dictable(duplicates, idx)
        existing_table.delete(to_delete, **inc)
    if len(res) > 0:
        res.to_sql(name = table, con = con, schema = schema, chunksize = chunksize, 
                   if_exists = 'append', index = True, index_label = index)
    return rtn



def pd_read_sql(table = None, db = None, server = None, schema = None, engine = None, session = None,
                inc = None, columns = None, index = None, coerce_float : bool = True, duplicate = None, sort = None, **more_inc):
    """
    a thin wrapper around sql_cursor.to_sql()
    
    Parameters
    ----------
    table, db, server, schema: str or partial 
        see sql_table constructor
        
    all the other parameters: see sql_cursor.read_sql
    
    """
    cursor = sql_table(table = table, db = db, server = server, schema = schema, engine = engine, session = session)
    return cursor.read_sql(inc = inc, index = index, columns = columns, coerce_float = coerce_float,
                           duplicate = duplicate, sort = sort, **more_inc)
    