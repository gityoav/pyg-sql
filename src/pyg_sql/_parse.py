from pyg_base import dictattr
from pyg_sql._sql_table import _types, get_server, _database, _pairs2connection, _schema
import re
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
    return dictattr(root = root, columns = columns, suffix = suffix)


def _parse_path(path):
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
    return connections + dict(doc = doc, schema = schema, db = db, server = server, root = root, table = table,
                              path = '%s/%s/%s/%s/%s'%(server, db, schema, table, root))

