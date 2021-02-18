SYMBOL_TABLE_SQL = '''CREATE TABLE IF NOT EXISTS public.{table_name} (
                        company varchar NULL,
                        symbol varchar NULL,
                        "type" varchar NULL,
                        exchange varchar NULL,
                        UNIQUE (symbol)
                )'''

PRICE_TABLE_SQL = '''CREATE TABLE IF NOT EXISTS public."{table_name}" (
                        datetime timestamp NULL,
                        symbol varchar NULL,
                        "open" numeric NULL,
                        high numeric NULL,
                        low numeric NULL,
                        "close" numeric NULL,
                        volume numeric NULL,
                        UNIQUE (datetime, symbol)
                    )'''

DELISTED_TABLE = '''CREATE TABLE IF NOT EXISTS delisted (
                        symbol varchar NULL,
                        timeframe varchar NULL,
                        UNIQUE (symbol, timeframe)
                    )'''

WHITELIST_TABLE = '''CREATE TABLE IF NOT EXISTS whitelist (
                        symbol varchar NULL,
                        timeframe varchar NULL,
                        UNIQUE (symbol, timeframe)
                    )'''

COUNT_FAIL_TABLE = '''CREATE TABLE IF NOT EXISTS count_fail (
                        symbol varchar NULL,
                        timeframe varchar NULL,
                        count int NOT NULL DEFAULT 1,
                        UNIQUE (symbol, timeframe)
                    )'''

SYMBOL_LAST_DATE = '''SELECT symbol, datetime 
                      FROM (
                           SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY datetime DESC) r 
                           FROM "{table_name}"
                            ) T
                     WHERE T.r=1'''

CREATE_TEMPORARY_TABLE = '''
                            CREATE TEMPORARY TABLE {table_name} AS
                                {sub_query}
                         '''
DELETE_LAST_DATE = '''DELETE FROM "{table_name}" as A WHERE EXISTS (
                        SELECT 1 FROM {sub_table} C
                            WHERE A.symbol = C.symbol and A.datetime = C.datetime 
                         )
                    '''

ADD_COL = """ALTER TABLE {table_name} 
             ADD COLUMN IF NOT EXISTS {col_name} {data_type}"""

SELECT_LAST_N_ROWS = """SELECT * FROM (
                          SELECT
                            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY datetime DESC) AS r,
                            t.*
                          FROM "{table_name}" t 
                          WHERE symbol in ({symbols})) x
                        WHERE x.r <= {n_rows}"""

SELECT_LAST_N_ROWS_FROM_SYMBOL = """ SELECT * FROM "{table_name}"
                                     WHERE symbol='{symbol}'
                                     ORDER BY datetime DESC limit {n_rows}"""

NUMBER_OF_ROWS_TO_UPDATE = """SELECT max(r) FROM (SELECT 
                                                    symbol , max(datetime ) AS max_available FROM public."{table_name}"
                                                    WHERE symbol in ({symbols}) AND {non_null_col} IS NOT NULL
                                                    GROUP BY symbol) A 
                                JOIN 
                                     (SELECT
                                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY datetime DESC) AS r,
                                        t.*
                                      FROM "{table_name}" t 
                                      WHERE symbol in ({symbols})) B 
                                ON A.max_available = B.datetime and A.symbol = B.symbol"""

SELECT_SYMBOL_WITH_FILTER = '''SELECT {cols} FROM public."{table}"
                                  WHERE symbol IN ({symbol_list}) {extra_filter}'''

EXECLUDE_EXCHANGE = """ SELECT symbol FROM {profile_table} WHERE exchange IN ({exchange})"""
