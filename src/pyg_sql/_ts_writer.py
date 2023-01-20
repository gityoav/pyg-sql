from pyg_base._bitemporal import _asof 
from pyg_sql._parse import _parse_path, _parse_root, _parse_key

_ts = '.ts'



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
                             volume = pd.Series(...),
                             db = db)
    
    >>> legal_doc  = db_cell(stock = 'AAPL',    
                             exchange = 'US', 
                             key = 'price_data',
                             db = db,
                             legal_cases = pd.DataFrame(dict(case = ..., court =... judgement = ...))
                            
    
    doc.save() will save the document in 'stocks' table but we want price data saved in stock_price_data table
    and legal_doc data in stock_legal_data
    
    
    Each of the keys WITHIN the document will then have its own structure
    
    We will create the following additional table that is created on the fly:
    
    stock_price_data|date|open|high|low|close|dffff:
        
        key              date    open high low close
        ---              ----    ---- ---- --- -----
        'AAPL/US/price'  1/1/23  4.3  4.9  4.2 4.5
        
    
    And volume data will go into this table:
    
    stock_price_data|series|i
        key              date    value
        ---              ----    ---- 
        'AAPL/US/price'  1/1/23  121 
        'AAPL/US/price'  2/1/23  153


    
    """


def ts_dumps():
    """
    this is equivalent to pickle.dumps
    """
    pass

def ts_loads():
    pass
    


