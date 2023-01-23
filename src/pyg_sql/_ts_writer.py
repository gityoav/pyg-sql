from pyg_base import is_pd, is_dict, dictable
from pyg_base._bitemporal import _updated
from pyg_encoders import encode, decode, cell_root, root_path, root_path_check, dictable_decode, WRITERS
from pyg_sql._sql_table import pd_read_sql, pd_to_sql
from pyg_sql._parse import sql_parse_path, sql_parse_table, _key
import pandas as pd
from functools import partial

_ts = '.ts'
_pd = '.pd'


_pd_read_sql = encode(pd_read_sql)
_dictable_decode = encode(dictable_decode)


def _pd_encode(value, server, db, schema, table, root, method = None, sep = '_'):
    if is_pd(value):
        tbl = sql_parse_table(table = table, df = value, sep = sep)
        return pd_to_sql(df = value, table = tbl, schema = schema, server = server, db = db, method = method, inc = {_key : root}, duplicate = 'last')
    elif is_dict(value):
        res = type(value)(**{k : _pd_encode(v, server = server, db = db, schema = schema, table = table, 
                                            root = '%s/%s'%(root,k), 
                                            method = method, sep = sep) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            tbl = sql_parse_table(table = table if table.endswith(sep) else table + sep, df = df, sep = sep)
            return dict(_obj = _dictable_decode, 
                        df = pd_to_sql(df = value, table = tbl, schema = schema, server = server, db = db, 
                                       method = 'replace', duplicate = None, inc = {_key : root}))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([_pd_encode(v, server = server, db = db, schema = schema, table = table, 
                                            root = '%s/%s'%(root,k), 
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
    return _pd_encode(value = value, 
                      method = method, 
                      sep = sep, 
                      server = args.server, 
                      db = args.db,
                      schema = args.schema,
                      table = args.table,
                      root = args.root)


    
def pd_write(doc, root = None, method = None, sep = '_'):
    """
    writes dataframes within a document into a sql.
    
    :Example:
    ---------
    >>> from pyg import *; import pandas as pd
    >>> server = 'DESKTOP-GOQ0NSM'; test_db = 'test_db'; schema = 'dbo'
    >>> server = 'DESKTOP-LU5C5QF'
    >>> db = partial(sql_table, 
                     table = 'tickers', 
                     db = test_db, 
                     pk = ['ticker', 'item'], 
                     server = server, 
                     writer = f'{server}/{test_db}/{schema}/tickers_data/%ticker/%item.pdr', 
                     doc = True)
    self = db()
    >>> ticker = 'CLA Comdty'
    >>> item = 'price'
    >>> doc = db_cell(passthru, data = pd.Series([1.,2.,3.,4],drange(2,5)), ticker = ticker, item = item, db = db)
    >>> doc = db_cell(passthru, 
                      data = pd.Series([1.,2.,3.,4],drange(2,5)), 
                      volume = pd.Series([1.,2.,3.,4],drange(-3)), 
                      ticker = ticker, item = item, db = db)
    >>> doc = doc.go()

    >>> get_cell('tickers', d, server = servers, ticker = ticker, item = item)
    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    args = sql_parse_path(path)
    return _pd_encode(value = doc, 
                      method = method, 
                      sep = sep, 
                      server = args.server, 
                      db = args.db,
                      schema = args.schema,
                      table = args.table,
                      root = args.root)

    
WRITERS[_pd] = pd_write
WRITERS[_pd + 'r'] = partial(pd_write, method = 'replace')
WRITERS[_pd + 'i'] = partial(pd_write, method = 'insert')
WRITERS[_pd + 'a'] = partial(pd_write, method = 'append')
WRITERS[_pd + 'u'] = partial(pd_write, method = 'update')


