from pyg_base import cache, Dict, is_pd, is_arr, is_dict, dictable, cfg_read
from pyg_sql._sql_table import sql_table, _pairs2connection, _schema, _database, get_server, _types
from pyg_encoders import encode, cell_root, root_path, root_path_check, dictable_decode, WRITERS

import pandas as pd
import pickle
import re
from pyg_base._bitemporal import _asof 

sql_table_ = cache(sql_table)
_sql = '.sql'
_ts = '.ts'
_dictable = '.dictable'
_dictable_decode = encode(dictable_decode)
_key = 'key'
_data = 'data'
_date = 'date'
_variable_name = re.compile('^[A-Za-z]+[A-Za-z0-9_]*')


def _parse_variable_name(key):
    """
    >>> key = 'param_49(date)'
    >>> 
    """
    name = _variable_name.search(key).group(0)
    return name

def _parse_variable_type(key, name):
    remain = key[len(name):]
    if remain.startswith('('):
        return _types[remain[1:].split(')')[0].lower()]
    else:
        return _types[str]

def _parse_root(root):
    if '.' in root:
        suffix = root.split('.')[-1]
        root = root[: -1 - len(suffix)]
    else:
        suffix = None
    keys = root.split('%')[1:]
    names = [_parse_variable_name(key) for key in keys]
    types = [_parse_variable_type(key, name) for key, name in zip(keys, names)]
    columns = dict(zip(names, types))
    root = '/'.join(['%' + name for name in names])
    return Dict(root = root, columns = columns, suffix = suffix)


def _parse_path(path):
    params = []
    ps = path.split('/')
    if len(ps) < 5:
        raise ValueError('%s must have at least five items: server/database/schema/table/root'%path)
    for i in range(len(ps)):
        if '?' in ps[i]:
            ps[i], prm = ps[i].split('?')
            params.extend(prm.split('&'))            
    connections = Dict(_pairs2connection(*params))
    server, db, schema, table = ps[:4]
    root = '/'.join(ps[4:])
    server = get_server(server or connections.pop('server',None))
    db = _database(db or connections.pop('db',None))
    schema = _schema(schema or connections.pop('schema', None))
    doc = connections.pop('doc', 'true')
    doc = dict(true = True, false = False).get(doc.lower(), doc)        
    return connections + dict(doc = doc, schema = schema, db = db, server = server, root = root, table = table,
                              path = '%s/%s/%s/%s/%s'%(server, db, schema, table, root))
    


def sql_binary_store(path):
    """
    splits a path which resembles a sql-alchemy connection string, to its bits

    Parameters
    ----------
    path : str
        A string with '/' as separators of the format:
        server/database/schema/table/root    
        
        The "root" bit, can be itself of a more fancy format: This is the key by which we save the data.
        root = 'fx/USDJPY/price'
        
        The parameters can be left as blank so for example:
        /database/schema/table/root    
        will map to the default server (specified in cfg_read)
        
        We also support some fancy stuff like:            
        path = 'server/database/schema?doc=true&name=yoav/table?whatever=1/root/path.sql' but not really useful.
        
        You may leave "blank" and then we will default.. so e.g.:
        '/database//table/root.sql' is perfectly acceptable and will default to default server and schema

    Returns
    -------
    dict
        various connection parameters. specifically, the cursor parameter actually generates the table
    """
    args = _parse_path(path)
    args.cursor = sql_table(table = args.table, db = args.db, schema = args.schema, 
                       pk = _key, server = args.server, 
                       non_null = {_data : bin}, doc = args.doc)
    return args


def sql_dumps(obj, path):
    """
    converts an obj into a binary within a sql binary store. a litle like pickle.dumps
    
    :Example
    --------
    >>> from pyg import *
    >>> path = '/test_db//test_table/key'
    >>> self = sql_binary_store(path).cursor
    >>> self.deleted
    >>> obj = pd.Series([1,2,3])
    >>> sql_dumps(obj, path)
    >>> sql_loads(path)

    Parameters
    ----------
    obj : object
        item to be pickled into binary.
    path : str
        path sqlalchemy-like to save the pickled binary in.

    Returns
    -------
    string 
        path

    """
    res = sql_binary_store(path)
    data = pickle.dumps(obj)
    cursor = res.cursor
    root = res.root
    # print('dumping into...\n', cursor)
    cursor.update_one({_key : root, _data : data})
    # print(cursor)
    return res.path


    

def sql_loads(path):
    """
    loads an obj from a binary within a sql binary store, a litle like pickle.laods
    """    
    res = sql_binary_store(path)
    cursor = res.cursor
    root = res.root
    row = cursor.inc(**{_key :root})
    if len(row) == 0:
        # print('no documents found in...\n', row)
        raise ValueError('no document found in %s' %(res-'cursor'))
    elif len(row) > 1:
        raise ValueError('multiple documents found \n%s'%row)
    else:
        # print('loading from...\n', row)
        data = row[0][_data]
        if isinstance(data, bytes):
            return pickle.loads(data)
        else:
            return data

_sql_loads = encode(sql_loads)

def sql_encode(value, path):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject of multiple pickled files that can be decoded

    Parameters:
    ----------
    value : document or dataframe
        value to be encoded inside a sql database
        
    path : str
        a sqlalchemy-like string
        
    Example: writing a single dataframe
    --------
    >>> from pyg import * 
    >>> value = pd.Series([1,2])
    >>> path = 'mssql+pyodbc://localhost/database_here?doc=false/xyz.table_name/root_of_doc'
    >>> res = sql_encode(value, path)
    >>> table = sql_table(db = 'database_here', schema = 'xyz', table = 'table_name')
    >>> assert len(table.inc(key = 'root_of_doc'))>0
    >>> sql_loads(path)
    
    Example: writing a document
    ---------------------------
    >>> from pyg import * 
    >>> value = dict(a = pd.Series([1,2]), b = pd.Series([3,4]))
    >>> path = 'mssql+pyodbc://localhost/database_here?doc=false/xyz.table_name/root_of_doc'
    >>> res = sql_encode(value, path)
    >>> table = sql_table(db = 'database_here', schema = 'xyz', table = 'table_name')
    >>> keys = table.distinct('key')
    >>> assert 'root_of_doc/a' in keys and 'root_of_doc/b' in keys    
    >>> assert eq(value['a'],sql_loads(path+'/a'))
    """
    if path.endswith(_sql):
        path = path[:-len(_sql)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value) or is_arr(value):
        path = root_path_check(path)
        return dict(_obj = _sql_loads, path = sql_dumps(value, path))       
    elif is_dict(value):
        res = type(value)(**{k : sql_encode(v, '%s/%s'%(path,k)) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            return dict(_obj = _dictable_decode, 
                        df =  sql_dumps(df, path if path.endswith(_dictable) else path + _dictable),
                        loader = _sql_loads)
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([sql_encode(v, '%s/%i'%(path,i)) for i, v in enumerate(value)])
    else:
        return value
    
def sql_write(doc, root = None):
    """
    writes dataframes within a document into a sql.
    
    :Example:
    ---------
    >>> from pyg import * 
    >>> from pyg_sql._sql_writer import path_to_connection
    >>> db = partial(sql_table, 
                     table = 'tickers', 
                     db = 'bbgs', 
                     pk = ['ticker', 'item'], 
                     server = 'localhost', 
                     writer = 'mssql+pyodbc://localhost/bbgs?driver=ODBC+Driver+17+for+SQL+Server&doc=false/bbg_data/%ticker/%item.sql', 
                     doc = True)
    >>> path = db().writer
    >>> res = path_to_connection(path)
    >>> ticker = 'CLA Comdty'
    >>> item = 'price'
    >>> doc = db_cell(passthru, data = pd.Series([1,2,3],drange(2)), 
                      array = np.array([1,2,3]),
                      list_of_values = [np.array([1,2,]), pd.DataFrame([1,2])],
                      ticker = ticker, item = item, db = db)
    >>> doc = doc.go()
    
    >>> get_cell('tickers', 'bbgs', server = 'localhost', ticker = ticker, item = item)
    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return sql_encode(doc, path)
    
WRITERS[_sql] = sql_write


