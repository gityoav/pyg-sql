import sqlalchemy as sa
from sqlalchemy_utils.functions import create_database
from pyg_base import cache, cfg_read, as_list, dictable, replace, Dict, is_dict, is_dictable, is_strs, is_str, is_int, is_date, dt2str, ulist, try_back, unique
from pyg_encoders import as_reader, as_writer, dumps, loads
from sqlalchemy import Table, Column, Integer, String, MetaData, Identity, Float, DATE, DATETIME, TIME, select, func, not_, desc, asc
from sqlalchemy.orm import Session
import datetime
from copy import copy
from pyg_base import logger

_id = '_id'
_doc = 'doc'
_root = 'root'
_deleted = 'deleted'
_archived = 'archived_'

@cache
def _servers():
    res = cfg_read().get('sql_server', {})
    if isinstance(res, dict):
        res[None] = res.get('null')
        res[True] = res.get('true')
        res[False] = res.get('false')
    return res

@cache
def _schema(schema = None):
    return schema or cfg_read().get('sql_schema', 'dbo')

@cache
def _database(db = None):
    return db or cfg_read().get('sql_database', 'master')

    
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
        import pyodbc
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
        db = _database(connection.pop('database', None))
    return db
    
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
        return 'mssql+pyodbc://%(server)s/%(db)s%(params)s'%dict(server=server, db = db, params = '?' +params if params else '')

def create_schema(engine, schema):
    if schema is None:
        return
    try:
        if schema not in engine.dialect.get_schema_names(engine):
            engine.execute(sa.schema.CreateSchema(schema))
            logger.info('creating schema: %s'%schema)
    except AttributeError: #MS SQL vs POSTGRES
        if not engine.dialect.has_schema(engine, schema):
            engine.execute(sa.schema.CreateSchema(schema))
            logger.info('creating schema: %s'%schema)
    return schema
    
def get_engine(*pairs, **connection):    
    """
    returns a sqlalchemy engine object
    accepts either *pairs: 'driver={ODBC Driver 17 for SQL Server}'
    or keyword arguments that look like driver = 'ODBC Driver 17 for SQL Server'    
    """
    connection = _pairs2connection(*pairs, **connection)
    server = get_server(connection.pop('server', None))
    connection['driver'] = get_driver(connection.pop('driver', None))
    db = _db(connection)    
    if isinstance(server, sa.engine.base.Engine):
        return server
    cstr = get_cstr(server=server, db = db, **connection)    
    e = sa.create_engine(cstr)
    try:
        sa.inspect(e)
    except Exception:
        logger.info('creating database: %s'%db)
        create_database(cstr)
        e = sa.create_engine(cstr)       
    return e
    
_types = {str: String, 'str' : String, 
          int : Integer, 'int' : Integer,
          float: Float, 
          'dec' : sa.DECIMAL(0), 'dec1' : sa.DECIMAL(1), 'dec2' : sa.DECIMAL(2), 'dec3' : sa.DECIMAL(3), 'dec4' : sa.DECIMAL(4), 'dec5' : sa.DECIMAL(5), 
          datetime.date: DATE, 'date' : DATE,
          datetime.datetime : DATETIME, 'datetime' : DATETIME,
          datetime.time: TIME, 'time' : TIME,
          bin : sa.VARBINARY}

_orders = {1 : asc, True: asc, 'asc': asc, asc : asc, -1: desc, False: desc, 'desc': desc, desc: desc}

def sql_table(table, db = None, non_null = None, nullable = None, _id = None, schema = None, server = None, reader = None, writer = None, pk = None, doc = None, mode = None):
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

    Raises
    ------
    ValueError
        DESCRIPTION.

    Returns
    -------
    res : sql_cursor
        A hybrid object we love.

    """
    if isinstance(table, str):
        values = table.split('.')
        if len(values) == 2:
            schema = schema or values[0]
            if schema != values[0]:
                raise ValueError('schema cannot be both %s and %s'%(values[0], db))
            table = values[1]
        elif len(values)>2:
            raise ValueError('not sure how to translate this %s into a db.table format'%table)
    e = get_engine(server = server, db = db, schema = schema)
    
    non_null = non_null or {}
    nullable = nullable or {}
    pks = pk or {}
    if isinstance(pks, list):
        pks = {k : String for k in pks}
    elif isinstance(pks, str):
        pks = {pks : String}
    if isinstance(non_null, list):
        non_null = {k : String for k in non_null}
    elif isinstance(non_null, str):
        non_null = {non_null : String}
    pks.update(non_null)
    if isinstance(nullable, list):
        nullable = {k : String for k in nullable}
    elif isinstance(nullable, str):
        nullable = {nullable: String}
    if isinstance(table, str):
        table_name = table 
    else:
        table_name = table.name
        if schema is None:
            schema = table.schema
    schema = create_schema(e, _schema(schema))
    if doc is True:
        doc = _doc
    meta = MetaData()
    i = sa.inspect(e)
    if not i.has_table(table_name, schema = schema):
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
        non_nulls = [Column(k, _types.get(t, t), nullable = False) for k, t in pks.items() if k not in col_names]
        nullables = [Column(k.lower(), _types.get(t, t)) for k, t in nullable.items() if k not in col_names] 
        docs = [Column(doc, String, nullable = True)] if doc else []
        cols = cols + non_nulls + nullables + docs
        if len(cols) == 0:
            raise ValueError('You seem to be trying to create a table with no columns? Perhaps you are trying to point to an existing table and getting its name wrong?')
        logger.info('creating table: %s.%s.%s%s'%(db, schema, table_name, [col.name for col in cols]))
        tbl = Table(table_name, meta, *cols, schema = schema)
        meta.create_all(e)
    else:
        # logger.info('schema :%s'%schema)
        tbl = Table(table_name, meta, autoload_with = e, schema = schema)
        cols = tbl.columns
        non_nulls = [Column(k, _types.get(t, t), nullable = False) for k, t in pks.items()]
        if non_nulls is not None:
            for key in non_nulls:
                if key.name not in cols.keys():
                    raise ValueError('column %s does not exist in %s.%s'%(key, db, table_name))
                elif cols[key.name].nullable is True:
                    raise ValueError('WARNING: You defined %s as a primary but it is nullable in %s.%s'%(key, db, table_name))
    res = sql_cursor(table = tbl, db = db, server = server, engine = e, spec = None, selection = None, reader = reader, writer = writer, pk = pk, doc = doc)
    return res

class sql_cursor(object):
    """
    # pyg-sql
    
    pyg-sql creates sql_cursor, a thin wrapper on sql-alchemy (sa.Table), providing three different functionailities:

    - simplified create/filter/sort/access of a sql table
    - maintainance of a table where records are unique per specified primary keys while we auto-archive old data
    - creation of a full no-sql like document-store

    pyg-sql "abandons" the relational part of SQL: we make using a single table extremely easy while forgo any multiple-tables-relations completely.
    
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


    ## insertion of "documents" into string columns...
    
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

    ## primary keys and auto-archive
    
    Primary Keys are applied if the primary keys (pk) are specified. 
    Now, when we insert into a table, if another record with same pk exists, the record will be replaced.
    Rather than simply delete old records, we create automatically a parallel deleted_database.table to auto-archive these replaced records.
    This ensure a full audit and roll-back of records is possible.

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

    Where did the data go to? We automatically archive the deleted old records for dict(name = 'yoav', surname = 'git') here:

    >>> t.deleted 
    
    t.deleted is a table by same name,
    
    - exists on deleted_test database, 
    - same table structure with added 'deleted' column
    
    >>> assert len(t.deleted.inc(name = 'yoav', age = 46)) > 0
    >>> t.deleted.delete() 

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
    """
    def __init__(self, table, db = None, engine = None, server = None, spec = None, selection = None, order = None, reader = None, writer = None, pk = None, doc = None, **_):
        """
        Parameters
        ----------
        table : sa.Table
            Our table
        db : string, optional
            Name of the db where table is.
        engine : sa,Engine, optional
            The sqlalchemy engine
        server : str , optional
            The server for the engine. If none, uses the default in pyg config file
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
        if is_str(table):
            table = sql_table(table = table, db = db, server = server)
            
        if isinstance(table, sql_cursor):
            db = table.db if db is None else db
            engine = table.engine if engine is None else engine
            server = table.server if server is None else server
            spec = table.spec if spec is None else spec
            selection = table.selection if selection is None else selection
            order = table.order if order is None else order
            reader = table.reader if reader is None else reader
            writer = table.writer if writer is None else writer
            pk = table.pk if pk is None else pk
            doc = table.doc if doc is None else doc
            table = table.table
    
        self.table = table
        self.db = db
        self.server = get_server(server)
        self.engine = engine or get_engine(db = self.db, server = self.server)
        self.spec = spec
        self.selection = selection
        self.order = order
        self.reader = reader
        self.writer = writer
        self.pk = pk
        self.doc = doc
    
    def copy(self):
        return type(self)(self)
    
    @property
    def schema(self):
        """
        table schema
        """
        return self.table.schema        
    
    @property
    def _ids(self):
        """
        columns generated by the SQL Server and should not be provided by the user
        """
        return [c.name for c in self.table.columns if c.server_default is not None]
    
    def _and(self, doc, keys):
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
        keys = {i: doc[i] for i in doc if isinstance(doc[i], (int, str, datetime.datetime))}
        if len(keys):
            return keys
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
        ids = self._ids
        return sorted([c.name for c in self.tbl.columns if c.nullable is False and c.name not in ids])
        
    
    def _c(self, expression):
        """
        converts an expression to a sqlalchemy filtering expression
        
        :Example:
        ---------
        >>> expression = dict(a = 1, b = 2)
        >>> assert t._c(expression) == sa.and_(t.c.a == 1, t.c.b == 2)
        """
        if isinstance(expression, dict):
            t = self.table.c    
            return sa.and_(*[sa.or_(*[t[k] == i for i in v]) if isinstance(v, list) else t[k] == self._c(v) for k,v in expression.items()]) 
        elif isinstance(expression, (list, tuple)):
            return sa.or_(*[self._c(v) for v in expression])            
        else:
            return expression  
    
    @property
    def c(self):
        return self.table.c
    
    def __ge__(self, kwargs):
        """
        provides a quick filtering by overloading operator:
        
        :Example:
        ---------
        >>> assert t >= dict(age = 30) == t.c.age >= 30
        >>> assert t > dict(weight = 30, height = 20) == sa.and_(t.c.weight >= 30, t.c.height > 20)
        """
        if not isinstance(kwargs, dict) and len(self._ids) == 1:
            kwargs = {self._ids : kwargs}
        c = self.table.c
        return self.inc(sa.and_(*[c[key] >= value for key, value in kwargs.items()]))

    def __gt__(self, kwargs):
        if not isinstance(kwargs, dict) and len(self._ids) == 1:
            kwargs = {self._ids : kwargs}
        c = self.table.c
        return self.inc(sa.and_(*[c[key] > value for key, value in kwargs.items()]))

    def __le__(self, kwargs):
        if not isinstance(kwargs, dict) and len(self._ids) == 1:
            kwargs = {self._ids : kwargs}
        c = self.table.c
        return self.inc(sa.and_(*[c[key] <= value for key, value in kwargs.items()]))

    def __lt__(self, kwargs):
        if not isinstance(kwargs, dict) and len(self._ids) == 1:
            kwargs = {self._ids : kwargs}
        c = self.table.c
        return self.inc(sa.and_(*[c[key] < value for key, value in kwargs.items()]))
            
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
            return self.select(ulist(as_list(self.selection)) - other)
        else:
            raise ValueError('cannot subtract these columns while the selection is non empty, use self.select() to reset selection')
    
    def drop(self):
        logger.info('dropping table: %s.%s.%s'%(self.db, 
                                                  self.schema, 
                                                  self.table.name))

        self.table.drop(bind = self.engine)

    
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
    
    
    @property
    def session(self):
        return Session(self.engine)
    
    
    def __len__(self):
        statement = select(func.count()).select_from(self.table)
        if self.spec is not None:
            statement = statement.where(self.spec)
        return list(self.engine.connect().execute(statement))[0][0]
    
    count = __len__

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
        found = {k : v[0] for k, v in missing.items() if len(set(v)) == 1}
        conflicted = {k : v for k, v in missing.items() if len(set(v)) > 1}
        if conflicted:
            raise ValueError('got multiple possible values for each of these columns: %s'%conflicted)
        res.update(found)
        return res
                
    def insert_one(self, doc, ignore_bad_keys = False, write = True):
        """
        insert a single document to the table

        Parameters
        ----------
        doc : dict
            record.
        ignore_bad_keys : 
            Suppose you have a document with EXTRA keys. Rather than filter the document, set ignore_bad_keys = True and we will drop irrelevant keys for you

        """
        edoc = self._dock(doc) if write else doc
        columns = self.columns
        if not ignore_bad_keys:
            bad_keys = {key: value for key, value in edoc.items() if key not in columns}
            if len(bad_keys) > 0:
                raise ValueError('cannot insert into db a document with these keys: %s. The table only has these keys: %s'%(bad_keys, columns))        
        res = self._write_doc(edoc, columns = columns) if write else edoc
        if self._pk and not self._is_deleted():
            doc_id = self._id(res)
            ids = self._ids
            res_no_ids = type(res)({k : v for k, v in res.items() if k not in ids}) if ids else res
            tbl = self.inc().inc(**doc_id)
            rows = tbl.sort(ids)._read_statement() ## row format
            docs = tbl._rows_to_docs(rows, reader = False, load = False) ## do not transform the document, keep in raw format
            if len(docs) == 0:
                with self.engine.connect() as conn: 
                    conn.execute(self.table.insert(),[res_no_ids])
                if ids:    
                    latest = tbl[0]
                    doc.update(latest[ids])
            else:                
                if len(docs) == 1:
                    latest = docs[0] 
                else:
                    latest = docs[-1]
                    tbl.exc(**tbl._id(latest)).full_delete()
                latest = Dict(latest)
                deleted = datetime.datetime.now()
                for d in docs:
                    for i in ids:
                        if i in d:
                            del d[i]
                    d[_deleted] = deleted
                self.deleted.insert_many(docs, write = False)
                self.inc(self._id(latest)).update(**(res_no_ids))
                doc.update(latest[ids])
        else:
            with self.engine.connect() as conn:
                conn.execute(self.table.insert(), [res])
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
            edoc[self.doc] = doc
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
        #res = self._dock(res, columns = columns) if dock else res
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
        for col in columns - self.doc:
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
        if isinstance(res, sa.engine.row.LegacyRow):
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
            stop = n + stop if is_int(stop) and start < 0 else stop
        if start and self.order is not None:
            statement = statement.offset(start)
            stop = stop if stop is None else stop - start
        if stop is not None:
            statement = statement.limit(1+stop)
        res = list(self.engine.connect().execute(statement))
        rows = res[slice(start, stop, step)]
        return rows
    
    def _rows_to_docs(self, rows, reader = None, load = True):
        """
        starts at raw values from the database and returns a list of read-dicts (or a single dict) from the database
        """
        columns = as_list(self.selection) if self.selection else self.columns
        reader = self._reader(reader)
        if isinstance(rows, list):
            return [self._read_row(row, reader = reader, columns = columns, load = load) for row in rows]
        else:        
            return self._read_row(rows, reader = reader, columns = columns, load = load)

    def docs(self, start = None, stop = None, step = None, reader = None):
        rows = self._read_statement(start = start, stop = stop, step = step)
        docs = self._rows_to_docs(rows, reader = reader)
        return dictable(docs)
    
    def read(self, item = 0, reader = None):
        value = len(self) + item if item < 0 else item
        statement = self.statement()
        if self.order is None:
            row = list(self.engine.connect().execute(statement.limit(value+1)))[value]
        else:
            row = list(self.engine.connect().execute(statement.offset(value).limit(1)))[0]
        doc = self._rows_to_docs(row, reader = reader)
        rtn = self._undock(doc)
        return rtn

                
    def __getitem__(self, value):
        """
        There are multiple modes to getitem:
        
        - t['key'] or t['key1', 'key2'] are equivalent to t.distinct('key') or t.distinct('key1', 'key2') 
        - t[['key1', 'key2']] will add a "SELECT key1, key2" to the statent
        - t[0] to grab a specific record. Note: this works better if you SORT the table first!, use t.sort('age')[10] to grab the name of the 11th youngest child in the class
        - t[::] a slice of the data: t.sort('age')[:10] are the 10 youngest students     
        
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
                raise ValueError('no records found for %s'%value)
            else:
                raise ValueError('multiple %s records found for %s'%(n, value))

        elif isinstance(value, slice):
            start, stop, step = value.start, value.stop, value.step
            if step is False:
                step = None
                reader = False
            else:
                reader = None
            rows = self._read_statement(start = start, stop = stop, step = step)
            docs = self._rows_to_docs(rows, reader = reader)
            columns = self.columns
            res = dictable([self._undock(doc, columns = columns) for doc in docs])
            return res

        elif is_int(value):
            return self.read(value)
    
    def update_one(self, doc, upsert = True):
        """
        Similar to insert, except will throw an error if upsert = False and an existing document is not there
        """
        existing = self.inc().inc(**self._id(doc))
        n = len(existing)
        if n == 0:
            if upsert is False:
                raise ValueError('no documents found to update %s'%doc)
            else:
                return self.insert_one(doc)
        elif self._pk:
            return self.insert_one(doc)
        elif n == 1:
            edoc = self._dock(doc)
            wdoc = self._write_doc(edoc)
            for i in self._ids:
                if i in wdoc:
                    del wdoc[i]
            existing.update(**wdoc)
            res = existing[0]
            res.update(edoc)
            return self._undock(res)
        elif n > 1:
            raise ValueError('multiple documents found matching %s '%doc)
                
            
    def insert_many(self, docs, write = True):
        """
        insert multiple docs. 

        Parameters
        ----------
        docs : list of dicts, dictable, pd.DataFrame
        """
        rs = dictable(docs)
        if len(rs) > 0:
            if self._pk and not self._is_deleted():
                _ = [self.insert_one(doc, write = write) for doc in rs]
            else:
                columns = self.columns - self._ids
                if write:
                    rows = [self._dock(row, columns) for row in rs]
                    rows = [self._write_doc(row, columns = columns) for row in rows]
                else:
                    rows = list(rs)
                with self.engine.connect() as conn:
                    conn.execute(self.table.insert(), rows)
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

        
    def insert(self, data = None, columns = None, **kwargs):
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
        return self.insert_many(rs)

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
        """
        if self.selection is None:
            statement = select(self.table)
        elif is_strs(self.selection):               
            c = self.table.c
            selection = [c[v] for v in as_list(self.selection)]
            statement = select(selection).select_from(self.table)
        else: ## user provided sql alchemy selection object
            statement = select(self.selection).select_from(self.table)
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
            cols = self.table.columns
            if isinstance(order, (str,list)):
                order = {o: 1 for o in as_list(order)}           
            statement = statement.order_by(*[_orders[v](cols[k]) for k, v in order.items()])
        return statement
    
    def update(self, **kwargs):
        if len(kwargs) == 0:
            return self
        statement = self.table.update()
        if self.spec is not None:
            statement = statement.where(self.spec)
        statement = statement.values(kwargs)
        with self.engine.connect() as conn:
            conn.execute(statement)
        return self
    
    set = update

    def full_delete(self):
        """
        A standard delete will actually auto-archive a table with primary keys. # i.e. we have full audit
        .full_delete() will drop the currently selected records without archiving them first
        
        """
        statement = self.table.delete()
        if self.spec is not None:
            statement = statement.where(self.spec)
        with self.engine.connect() as conn:
            conn.execute(statement)
        return self

    def delete(self, **kwargs):
        res = self.inc(**kwargs)
        ids = self._ids
        if len(res):
            if self._pk and not self._is_deleted(): ## we first copy the existing data out to deleted db
                rows = self._read_statement() 
                docs = self._rows_to_docs(rows, reader = False, load = False)
                deleted = datetime.datetime.now()
                for doc in docs:
                    doc[_deleted] = deleted
                    for i in ids:
                        if i in doc:
                            del doc[i]
                self.deleted.insert_many(docs, write = False)
            res.full_delete()
        return self

    def sort(self, order = None):
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
        if len(keys) == 0 and self.selection is not None:
            keys = as_list(self.selection)
        session = Session(self.engine)
        cols = [self.table.columns[k] for k in keys]
        query = session.query(*cols)
        if self.spec is not None:
            query = query.where(self.spec)        
        res = query.distinct().all()
        if len(keys)==1:
            res = [row[0] for row in res]
        return res
    
    def __repr__(self):
        statement = self.statement()
        params = statement.compile().params
        text = str(statement).replace(self.table.name+'.','')
        for k,v in params.items():
            text = text.replace(':'+k, '"%s"'%v if is_str(v) else dt2str(v) if is_date(v) else str(v))
            
        res = 'sql_cursor: %(db)s.%(table)s%(pk)s %(doc)s %(w)s\n%(statement)s\n%(n)i records'%dict(db = self.db, table = self.table.name, doc = 'DOCSTORE[%s]'%self.doc if self.doc else '', 
                                                                                              w = 'writer = %s'%self.writer if is_str(self.writer) else '',
                                                                                              pk = self._pk if self._pk else '', 
                                                                                              n = len(self), 
                                                                                              statement = text)
        return res

    def _is_deleted(self):
        return is_str(self.schema) and self.schema.startswith(_archived)

    @property
    def deleted(self):
        if self._is_deleted():
            return self
        else:        
            schema = _archived + (self.schema or '')
            # logger.info('archived schema: %s'%schema)
            res = sql_table(table = self.table, db = self.db, non_null = dict(deleted = datetime.datetime), 
                            server = self.server, 
                            pk = self.pk, 
                            doc = self.doc, 
                            writer = self.writer, 
                            reader = self.reader, 
                            schema = schema)
            res.spec = self.spec
            res.order = self.order
            res.selection = self.selection
            return res
                
    @property
    def address(self):
        """
        :Returns:
        ---------
        tuple
            A unique combination of the server address, db name and table name, identifying the table uniquely. This allows us to create an in-memory representation of the data in pyg-cell

        """
        return ('server', self.server or get_server()), ('db', self.db), ('schema', self.schema), ('table', self.table.name)


