import time
import json
from datetime import datetime, timedelta
import requests
import pathos.multiprocessing as mp
import yfinance as yf
from kafka import KafkaProducer
from src.utils.sql_utils import *
from src.utils.logging import log
from src.sql.db_sql import *
from src.kafka_setting import KAFKA_TOPIC, KafkaMessage
from apscheduler.schedulers.blocking import BlockingScheduler

pd.options.mode.chained_assignment = None

SYMBOL_PROVIDER = 'FMP'
delist_exchange = ['HKSE', 'MCX', 'ASX']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))


def get_symbols_finhub() -> pd.DataFrame:
    TOKEN = 'bv2fgon48v6ubfulc70g'
    # TODO read symbol from database
    df = pd.read_csv('symbols.csv')

    all_symbol = []
    for code in df['code']:
        print(code)
        r = requests.get(f'https://finnhub.io/api/v1/stock/symbol?exchange={code}&token={TOKEN}')
        df_exchange = pd.json_normalize(r.json())
        if df_exchange.empty:
            continue
        df_exchange = df_exchange.rename(columns={'description': 'company'})
        df_exchange.drop('displaySymbol', axis=1, inplace=True)
        df_exchange = df_exchange[['company', 'symbol', 'type']]
        df_exchange['exchange'] = code
        all_symbol.append(df_exchange.copy())

    df = pd.concat(all_symbol)
    return df


def get_symbols_financialmodelingprep(table) -> pd.DataFrame:
    TOKEN = '3119db4ad74016f7aa96c97509309a0c'
    r = requests.get(f'https://financialmodelingprep.com/api/v3/stock/list?apikey={TOKEN}')
    df = pd.json_normalize(r.json())
    df.drop(columns=['price'], inplace=True)
    df.rename(columns={'name': 'company'}, inplace=True)
    if not isTableExist(table):
        df.to_sql(table, con=engine, if_exists='append', index=False)
        execute_sql_statement(f'''ALTER TABLE {table} ADD UNIQUE (symbol)''')
    else:
        insert_on_conflict_do_update(df, table, batch=5000)
    return df


def profile_fmp(table, symbol_table):
    TOKEN = '3119db4ad74016f7aa96c97509309a0c'
    symbol = read_from_sql_statement(f'select * from {symbol_table}')['symbol'].to_list()
    for chunk in chunks(symbol, 500):
        symbols = ','.join(chunk)
        r = requests.get(f'https://financialmodelingprep.com/api/v3/quote/{symbols.strip()}?apikey={TOKEN}')
        df_exchange = pd.json_normalize(r.json())
        if not isTableExist(table):
            df_exchange.to_sql(table, con=engine, if_exists='append', index=False)
            execute_sql_statement(f'''ALTER TABLE {table} ADD UNIQUE (symbol)''')
        else:
            insert_on_conflict_do_update(df_exchange, table, batch=500)


def process_batch(df_price, symbol):
    df_price.columns = df_price.columns.set_levels(df_price.columns.levels[1].str.strip(), level=1)
    df = df_price.iloc[:, df_price.columns.get_level_values(1) == symbol].droplevel(1, axis=1)
    df.dropna(inplace=True)

    if not df.empty:
        df.reset_index(inplace=True)
        df.drop(columns=['Adj Close'], inplace=True)
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']

        df['symbol'] = symbol
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        return df
    else:
        return symbol


def manage_delist(df_delist, main_table, count_table='count_fail', max_try=21):
    insert_on_conflict_do_increment(df_delist, table_name=count_table, count_col='count')
    # main_table is also timeframe
    symbols = read_from_sql_statement(
        f"SELECT symbol FROM {count_table} WHERE count >= {max_try} AND timeframe = '{main_table}'")

    if not symbols['symbol'].empty:
        symbols = ", ".join("'{0}'".format(s) for s in symbols)
        execute_sql_statement(f"DELETE FROM {count_table} WHERE symbol in ({symbols}) AND timeframe = '{main_table}'")
        execute_sql_statement(f'DELETE FROM "{main_table}" WHERE symbol in ({symbols})')
        insert_on_conflict_ignore()


def fill_db(timeframe, period=None, profile_table='', avgVolumne=200000, batch_size=500):
    df_company = read_from_sql_statement(f'select symbol from {profile_table} where "avgVolume" > {avgVolumne}')[
        'symbol'].to_list()

    delisted = read_from_sql_statement(f"select * from delisted where timeframe = '{timeframe}'")['symbol'].to_list()
    whitelist = read_from_sql_statement('select * from whitelist')['symbol'].to_list()

    if not isTableExist(table=f"{timeframe}"):
        log.info(f'table {timeframe} not exist. Creating ...')
        execute_sql_statement(PRICE_TABLE_SQL.replace('{table_name}', f'\"{timeframe}\"'))
    now = datetime.now()

    if timeframe == '1d':
        start_time = now - timedelta(days=729)
    else:
        start_time = now - timedelta(days=59)

    start_time = start_time.date()

    end_time = now + timedelta(days=1)
    end_time = end_time.date()

    log.info('Get symbol list')
    current_symbol = read_from_sql_statement(f'select distinct(symbol) from \"{timeframe}\"')['symbol'].to_list()
    symbol_to_download = list((set(df_company) - set(delisted) - set(current_symbol)) | set(whitelist))

    process_pool = mp.ProcessPool(nodes=mp.cpu_count())
    if period:
        download = lambda symbol: get_history(symbol, interval=timeframe, period=period)
    else:
        download = lambda symbol: get_history(symbol, interval=timeframe, start=start_time, end=end_time)

    # download(symbol_to_download[:10])
    for symbols in chunks(symbol_to_download, batch_size):
        output = process_pool.map(download, symbols)

        data = [obj for obj in output if isinstance(obj, pd.DataFrame)]
        data_df = pd.concat(data)
        data_df.drop_duplicates(subset=['datetime', 'symbol'], inplace=True)
        insert_on_conflict_do_update(data_df, table_name=f'\"{timeframe}\"', schema='public', batch=5000)

        no_data_symbol = [symbol for symbol in output if isinstance(symbol, str)]
        delist_df = pd.DataFrame(no_data_symbol, columns=['symbol'])
        delist_df['timeframe'] = timeframe
        manage_delist(delist_df, main_table=timeframe, count_table='count_fail')

        message = KafkaMessage(table=timeframe, symbols=symbols, period=100, mode='full').to_dict()
        log.info(f'Sending data to indicator consumer: {message}')
        producer.send(KAFKA_TOPIC, message)
        producer.flush()

    process_pool.close()
    process_pool.join()
    process_pool.clear()

    log.info('Finish refresh')


def update_db(timeframe, batch_size=500):
    log.info('Updating DB')
    log.info('Get delisted symbols')
    delisted = read_from_sql_statement(f"select * from delisted where timeframe = '{timeframe}'")['symbol'].to_list()
    now = datetime.now()

    log.info('Get symbols and last date')
    SYMBOL_LAST_DATE_SUBQUERY = SYMBOL_LAST_DATE.replace('{table_name}', f'"{timeframe}"')
    execute_sql_statement('DROP TABLE IF EXISTS tmp_symbol_lastdate')
    execute_sql_statement(CREATE_TEMPORARY_TABLE.replace('{table_name}', 'tmp_symbol_lastdate').replace('{sub_query}',
                                                                                                        SYMBOL_LAST_DATE_SUBQUERY))

    current_db = read_from_sql_statement(SYMBOL_LAST_DATE.replace('{table_name}', f'"{timeframe}"'))
    current_db = current_db[~current_db['symbol'].isin(delisted)]
    current_db['datetime'] = current_db['datetime'].dt.date.astype(str)

    end_time = now + timedelta(days=1)
    end_time = end_time.date()

    current_db['end'] = str(end_time)

    arguments = [(row[1]['symbol'], timeframe, row[1]['datetime'], row[1]['end']) for row in current_db.iterrows()]

    log.info('Delete last date')
    execute_sql_statement(
        DELETE_LAST_DATE.replace('{table_name}', f'"{timeframe}"').replace('{sub_table}', 'tmp_symbol_lastdate'))

    download = lambda x: get_history(*x)
    log.info('Start downloading ...')

    process_pool = mp.ProcessingPool(mp.cpu_count())
    for args in chunks(arguments, batch_size):
        output = process_pool.map(download, args)

        data = [obj for obj in output if isinstance(obj, pd.DataFrame)]
        insert_on_conflict_do_update(pd.concat(data), table_name=f'\"{timeframe}\"', schema='public', batch=5000)

        no_data_symbol = [symbol for symbol in output if isinstance(symbol, str)]
        delist_df = pd.DataFrame(no_data_symbol, columns=['symbol'])
        delist_df['timeframe'] = timeframe
        manage_delist(delist_df, main_table=timeframe, count_table='count_fail')

        symbols = [arg[0] for arg in args]
        message = KafkaMessage(table=timeframe, symbols=symbols, period=100, mode='append').to_dict()
        log.info(f'Sending data to indicator consumer: {message}')
        producer.send(KAFKA_TOPIC, message)
        producer.flush()

    process_pool.close()
    process_pool.join()
    process_pool.clear()

    log.info('Finish update')


def get_history(symbol, interval, start=None, end=None, period=None):
    try:
        if period:
            df = yf.download(symbol, period=period, interval=interval, threads=False)
        else:
            df = yf.download(symbol, start=start, end=end, interval=interval, threads=False)
        time.sleep(0.1)
        if df.empty:
            return symbol
        else:
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
            df.reset_index(inplace=True)
            df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            df.dropna(inplace=True)

            df['symbol'] = symbol
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            return df
    except Exception as e:
        log.debug(e)
        return symbol


def refresh_symbol(timeframe, period=None):
    log.info('Refreshing symbols')

    if SYMBOL_PROVIDER == 'FMP':
        SYMBOL_TABLE = f'symbol_{SYMBOL_PROVIDER.lower()}'
        PROFILE_TABLE = f'profile_{SYMBOL_PROVIDER.lower()}'
        get_symbols_financialmodelingprep(table=SYMBOL_TABLE)
        profile_fmp(table=PROFILE_TABLE, symbol_table=SYMBOL_TABLE)

        exchanges = ", ".join("'{0}'".format(e) for e in delist_exchange)
        symbols = read_from_sql_statement(EXECLUDE_EXCHANGE.format(profile_table=PROFILE_TABLE, exchange=exchanges))
        symbols['timeframe'] = timeframe
        insert_on_conflict_do_update(symbols, table_name='delisted')
    else:
        SYMBOL_TABLE = f'symbol_{SYMBOL_PROVIDER.lower()}'
        PROFILE_TABLE = f'profile_{SYMBOL_PROVIDER.lower()}'
        df_symbols = get_symbols_finhub()

    fill_db(timeframe, profile_table=PROFILE_TABLE, avgVolumne=200000, period=period)


if __name__ == '__main__':
    execute_sql_statement(DELISTED_TABLE)
    execute_sql_statement(WHITELIST_TABLE)
    execute_sql_statement(COUNT_FAIL_TABLE)

    refresh_60m = lambda: refresh_symbol('60m')
    update_60m = lambda: update_db('60m')

    refresh_1d = lambda: refresh_symbol('1d')
    update_1d = lambda: update_db('1d')

    sched = BlockingScheduler()
    sched.add_job(update_60m, 'cron', id='update_60m', hour='14-22', minute='28',
                  day_of_week='mon-fri')  # start at 29 because of warmup
    sched.add_job(refresh_60m, 'cron', id='refresh_60m', hour=1, day_of_week='mon-fri')

    sched.add_job(update_1d, 'cron', id='update_1d', hour=6, day_of_week='mon-fri')
    sched.add_job(refresh_1d, 'cron', id='refresh_1d', hour=1, day_of_week='mon-fri')
    sched.start()
