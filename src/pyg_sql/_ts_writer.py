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


def ts_dumps():
    """
    this is equivalent to pickle.dumps
    """
    pass

def ts_loads():
    pass
    


