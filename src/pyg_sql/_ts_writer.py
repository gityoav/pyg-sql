from pyg_base import is_pd, is_dict, dictable
from pyg_base._bitemporal import _updated
from pyg_encoders import encode, decode, cell_root, root_path, root_path_check, dictable_decode, WRITERS
from pyg_sql._sql_table import pd_read_sql, pd_to_sql
from pyg_sql._parse import sql_parse_path, sql_parse_table, _key
import pandas as pd
from functools import partial

_ts = '.ts'
_pd = '.pd'


def sql_ts_store(path):
    """
    Suppose we want to support a document store where the only large objects within it are pandas dataframes which are timeseries

    Let us take an example:
        
    Suppose we have stocks documents in stocks table unique by 'stock', 'exchange' and 'key'
    
    The database constructor should look like:
    
    >>> db = partial(sql_table, server = server, db = db, table = 'stocks', 
                         pk = ['stock', 'exchange'], doc = True, 
                         writer = 'server/db/schema/stock_%key/%stock/%exchange.pd')

    >>> price_doc =  db_cell(stock = 'AAPL',    
                             exchange = 'US', 
                             key = 'price_data',
                             db = db,
                             price = pd.DataFrame(dict(open = ... , high = , low = , close = ..), index), 
                             avg_volume = pd.Series(...),
                             open_int = pd.DataFrame(dict(open_int = values), index = dates)
                             db = db)
    
    >>> legal_doc  = db_cell(stock = 'AAPL',    
                             exchange = 'US', 
                             key = 'legal_data',
                             db = db,
                             legal_cases = pd.DataFrame(dict(case = ..., court =... judgement = ...))
                            
    
    doc.save() will save the document in 'stocks' table but we want price data saved in stock_price_data table
    and legal_doc data in stock_legal_data


    There are several questions to answer:
        Q1) what SQL table structures we want to support
        Q2) how do we manage re-writes? do we append? replace? update?
        Q3) how do we manage bitemporal data to ensure a full audit?
    
    Q2 & Q3 can be handled with code. Q1 is about the structure of the sql database.
        
    Q1: SQL table data structure
    ----------------------------
    
    Each of the pandas timeseries with the document will then have its own column structure.    

    We need a mapping from the dataframe structure to 
    1) table name 
    2) table structure

        df -> unique name of table
        series -> unique name of table

    So for example:
    price_doc.avg_volume, is a non-bitemporal timeseries pd.Series of int. It will be stored in table called "series"

    "series"
    key                date    value
    ---                ----    -----
    AAPL/US/avg_volume 1/1/01  102.34
    AAPL/US/avg_volume 1/1/02  234.56
    
    price_doc.open_int is a single-column non-bitemporal dataframe. It will be stored in table:
    open_int|i


    
    
    Q2: 
    --------------------------------
    We will need to handle three distinct types of dataframes:
    
    1) timeseries: is_ts(df) 
    2) bitemporal
    3) generic dataframe
    
    
    When we write a generic dataframe by key, we simply replace existing data
    
    
    """


def pd_dumps(obj, path, method = None):
    """
    converts a pandas dataframe into a table within a sql pandas store. a litle like pickle.dumps
    
    :Example
    --------
    >>> from pyg import *; import pandas as pd
    >>> server = 'DESKTOP-GOQ0NSM'; db = 'test_db'; schema = 'dbo'
    >>> obj = pd.Series(3., drange(10))
    >>> method = 'replace'
    >>> path = f'{server}/{db}/{schema}/test_table4/key3'

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
    args = sql_parse_path(path)
    res = pd_to_sql(df = obj, table = args.table, db = args.db, server = args.server, schema = args.schema, index = None, columns = None, series = None, 
                    method = method, inc = dict(key = args.root), duplicate = 'last')
    return res


_pd_read_sql = encode(pd_read_sql)
_dictable_decode = encode(dictable_decode)


def _pd_encode(value, server, db, schema, table, root, path, method = None, sep = '_', **kwargs):
    if is_pd(value):
        tbl = sql_parse_table(table = table, df = value, sep = sep)
        return pd_to_sql(df = value, table = tbl, schema = schema, server = server, db = db, method = method, inc = {_key : root})
    elif is_dict(value):
        res = type(value)(**{k : _pd_encode(v, server = server, db = db, schema = schema, table = table, 
                                            root = '%s/%s'%(root,k), 
                                            path = '%s/%s'%(path,k), 
                                            method = method, sep = sep) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            tbl = sql_parse_table(table = table if table.endswith(sep) else table + sep, df = df, sep = sep)
            return dict(_obj = _dictable_decode, 
                        df = pd_to_sql(df = value, table = tbl, schema = schema, server = server, db = db, method = method, inc = {_key : root}))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([_pd_encode(v, server = server, db = db, schema = schema, table = table, 
                                            root = '%s/%s'%(root,k), 
                                            path = '%s/%s'%(path,k), 
                                            method = method, sep = sep) for k, v in enumerate(value)])
    else:
        return value
    

def pd_encode(value, path, method = None, sep = '_'):
    """
    encodes a document or a single dataframe into a sql table
    from pyg import * 
    path = 'server/db/schema/table|/ticker/item'
    
    """
    args = sql_parse_path(path)
    return _pd_encode(value = value, method = method, sep = sep, **args)


    
def pd_write(doc, root = None, method = None):
    """
    writes dataframes within a document into a sql.
    
    :Example:
    ---------
    >>> from pyg import *; import pandas as pd
    >>> server = 'DESKTOP-GOQ0NSM'; db = 'test_db'; schema = 'dbo'
    >>> db = partial(sql_table, 
                     table = 'tickers', 
                     db = db, 
                     pk = ['ticker', 'item'], 
                     server = server, 
                     writer = f'{server}/{db}/{schema}/tickers_data/%ticker/%item.pd', 
                     doc = True)
    >>> path = db().writer
    >>> ticker = 'CLA Comdty'
    >>> item = 'price'
    >>> doc = db_cell(passthru, data = pd.Series([1.,2.,3.],drange(2)), ticker = ticker, item = item, db = db)
    >>> doc = doc.go()
    
    >>> get_cell('tickers', 'bbgs', server = 'localhost', ticker = ticker, item = item)
    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return pd_encode(doc, path, method = method)

    

WRITERS[_pd] = pd_write
WRITERS[_pd + 'r'] = partial(pd_write, method = 'replace')
WRITERS[_pd + 'i'] = partial(pd_write, method = 'insert')
WRITERS[_pd + 'a'] = partial(pd_write, method = 'append')
WRITERS[_pd + 'u'] = partial(pd_write, method = 'update')


