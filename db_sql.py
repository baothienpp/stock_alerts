SYMBOL_TABLE_SQL = '''CREATE TABLE public.{table_name} (
                        company varchar NULL,
                        symbol varchar NULL,
                        "type" varchar NULL,
                        exchange varchar NULL,
                        UNIQUE (symbol)
                )'''

PRICE_TABLE_SQL = '''CREATE TABLE public.{table_name} (
                        datetime timestamp NULL,
                        symbol varchar NULL,
                        "open" numeric NULL,
                        high numeric NULL,
                        low numeric NULL,
                        "close" numeric NULL,
                        volume numeric NULL,
                        UNIQUE (datetime, symbol)
                    )'''

DELISTED_TABLE = '''CREATE TABLE delisted (
                        symbol varchar NULL,
                        UNIQUE (symbol)
                    )'''

DELETE_LAST_DATE = '''DELETE FROM {table_name} as A
                        WHERE EXISTS (
                            SELECT 1 FROM (
                                        SELECT symbol, max(datetime) as datetime FROM {table_name}
                                        GROUP BY symbol) C
                            WHERE A.symbol = C.symbol and A.datetime = C.datetime
                        )'''
