from pyg_base import dictattr, is_date
from pyg_encoders import root_path
from pyg_sql._sql_table import _types, _type_codes, get_server, _database, _pairs2connection, _schema
import re
import datetime
import pandas as pd
_variable_name = re.compile('^[A-Za-z]+[A-Za-z0-9_]*')
_key = 'key'


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
    return dictattr(root = root, columns = columns, suffix = suffix)


def _array_type(arr):
    if isinstance(arr, pd.DatetimeIndex):
        return datetime.datetime
    elif isinstance(arr[0], datetime.date):
        return datetime.date
    elif isinstance(arr[0], datetime.datetime):
        return datetime.datetime
    elif is_date(arr[0]):
        return datetime.datetime
    tp = str(arr.dtype)
    if tp.startswith('int'): 
        return int
    elif tp.startswith('float'): 
        return float
    elif tp.startswith('bool'): 
        return bool
    elif tp == 'object':
        return str
    else:
        raise ValueError(f'cannot parse column type "{tp}" for array with these values: {arr[:2]}')


def _parse_table(table, inc = None, df = None):
    """
    returns a dict with 'table' as well as pk/nullable
    """
    if df is None and inc is None:
        return dict(table = table)
    inc = inc or {}
    table = root_path(inc, table)
    if not table:
        table = '|'

    res = {}
    if inc:
        res['pk'] = {key : type(value) for key, value in inc.items()}

    nullable = {}
    if isinstance(df, pd.Series):
        nullable = {'value': _array_type(df.values)}
    elif isinstance(df, pd.DataFrame):
        nullable = {col: _array_type(df[col].values) for col in df.columns}
    
    nullable_cols = sorted(nullable)
    nullable_codes = [_type_codes[_types[nullable[k]]] for k in nullable_cols]
    
    if nullable:
        addon = '|'.join(nullable_cols).lower()
        if len(set(nullable_codes)) == 1:
            type_addon = nullable_codes[0]
        else:
            type_addon = ''.join(nullable_codes)
        if type_addon not in ('', 'f'):
            addon = addon  + '|' + type_addon
        if table.endswith('|'):
            table = table + addon
        tp = _array_type(df.index)
        if tp != int:
            index = df.index.name or 'index'
            nullable[index] = tp
        res['nullable'] = nullable
    
    res['table'] = table
    return res


def sql_parse_path(path, inc = None, df = None):
    """
    parses a string path 

    Parameters
    ----------
    path : str
        path of a sql sql.
    inc : dict, optional
        Extra variables you may want in your columns. The default is None.
    df : pd.Series or DataFrame, optional
        
    
    Examples
    --------
    >>> sql_parse_path('server/database/schema/table/root.sql')

    {'doc': True,
     'schema': 'schema',
     'db': 'database',
     'server': 'server',
     'root': 'root.sql',
     'table': 'table',
     'path': 'server/database/schema/table/root.sql'}
    
    
    >>> path  = 'server/database/schema/%stock_world/root.sql'
    >>> df = pd.Series(np.random.normal(0,1,100), drange(99))
    >>> args = sql_parse_path('server/database/schema/%stock_world/root.sql', inc = dict(stock = 'hello'))
    >>> args

    {'doc': True,
     'schema': 'schema',
     'db': 'database',
     'server': 'server',
     'root': 'root.sql',
     'table': 'hello_world',    ### %stock mapped to 'hello'
     'path': 'server/database/schema/%stock_world/root.sql',
     'pk': {'stock': str}}      #### primary keys are stock

    
    >>> sql_parse_path('server/database/schema/%stock_world|/root.sql', inc = dict(stock = 'hello'), df = df)
        {'doc': True,
         'schema': 'schema',
         'db': 'database',
         'server': 'server',
         'root': 'root.sql',
         'table': 'hello_world|',
         'path': 'server/database/schema/%stock_world|/root.sql',
         'pk': {'stock': str}}
                
    """
    inc = inc or {}    
    params = []
    ps = path.split('/')
    if len(ps) < 5:
        raise ValueError('%s must have at least five items: server/database/schema/table/root'%path)
    for i in range(len(ps)):
        if '?' in ps[i]:
            ps[i], prm = ps[i].split('?')
            params.extend(prm.split('&'))            
    connections = dictattr(_pairs2connection(*params))
    server, db, schema, table = ps[:4]
    root = '/'.join(ps[4:])
    server = get_server(server or connections.pop('server',None))
    db = _database(db or connections.pop('db',None))
    schema = _schema(schema or connections.pop('schema', None))
    doc = connections.pop('doc', 'true')
    doc = dict(true = True, false = False).get(doc.lower(), doc)        
    table_params = _parse_table(table, inc = inc, df = df)
    res = connections | dict(doc = doc, schema = schema, db = db, server = server, root = root, table = table, path = '%s/%s/%s/%s/%s'%(server, db, schema, table, root)) | table_params 
    return res


