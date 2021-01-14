import time
import json
from datetime import datetime, timedelta
import requests
import pathos.multiprocessing as mp
import yfinance as yf
from kafka import KafkaProducer
from src.utils.sql_utils import *
from src.utils.logging import log
from src.sql.db_sql import PRICE_TABLE_SQL, DELETE_LAST_DATE, DELISTED_TABLE, SYMBOL_LAST_DATE, CREATE_TEMPORARY_TABLE
from src.kafka_setting import KAFKA_TOPIC, KafkaMessage
from apscheduler.schedulers.blocking import BlockingScheduler

pd.options.mode.chained_assignment = None

SYMBOL_PROVIDER = 'FMP'

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


def fill_db(timeframe, profile_table, avgVolumne, batch_size=500):
    df_company = read_from_sql_statement(f'select symbol from {profile_table} where "avgVolume" > {avgVolumne}')[
        'symbol'].to_list()
    delisted = read_from_sql_statement('select * from delisted')['symbol'].to_list()

    if not isTableExist(table=f"{timeframe}"):
        log.info(f'table {timeframe} not exist. Creating ...')
        execute_sql_statement(PRICE_TABLE_SQL.replace('{table_name}', f'\"{timeframe}\"'))
    now = datetime.now()

    if timeframe == '60m':
        start_time = now - timedelta(days=729)
    else:
        start_time = now - timedelta(days=59)

    start_time = start_time.date()

    end_time = now + timedelta(days=1)
    end_time = end_time.date()

    log.info('Get symbol list')
    current_symbol = read_from_sql_statement(f'select distinct(symbol) from \"{timeframe}\"')['symbol'].to_list()
    symbol_to_download = list(set(df_company) - set(delisted) - set(current_symbol))

    process_pool = mp.ProcessPool(nodes=mp.cpu_count())
    download = lambda symbol: get_history(symbol, interval=timeframe, start=start_time, end=end_time)

    for symbols in chunks(symbol_to_download, batch_size):
        output = process_pool.map(download, symbols)

        data = [obj for obj in output if isinstance(obj, pd.DataFrame)]
        no_data_symbol = [obj for obj in output if isinstance(obj, str)]

        insert_on_conflict_do_update(pd.concat(data), table_name=f'\"{timeframe}\"', schema='public', batch=5000)
        insert_on_conflict_do_update(pd.DataFrame(no_data_symbol, columns=['symbol']), table_name='delisted')

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
    delisted = read_from_sql_statement('select * from delisted')['symbol'].to_list()
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
        no_data_symbol = [obj for obj in output if isinstance(obj, str)]

        insert_on_conflict_do_update(pd.concat(data), table_name=f'\"{timeframe}\"', schema='public', batch=5000)
        insert_on_conflict_do_update(pd.DataFrame(no_data_symbol, columns=['symbol']), table_name='delisted')

        symbols = [arg[0] for arg in args]
        message = KafkaMessage(table=timeframe, symbols=symbols, period=100, mode='append').to_dict()
        log.info(f'Sending data to indicator consumer: {message}')
        producer.send(KAFKA_TOPIC, message)
        producer.flush()

    process_pool.close()
    process_pool.join()
    process_pool.clear()

    log.info('Finish update')


def get_history(symbol, interval, start, end):
    try:
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


def refresh_symbol(timeframe):
    log.info('Refreshing symbols')
    if not isTableExist(table='delisted'):
        execute_sql_statement(DELISTED_TABLE)

    if SYMBOL_PROVIDER == 'FMP':
        SYMBOL_TABLE = f'symbol_{SYMBOL_PROVIDER.lower()}'
        PROFILE_TABLE = f'profile_{SYMBOL_PROVIDER.lower()}'
        get_symbols_financialmodelingprep(table=SYMBOL_TABLE)
        profile_fmp(table=PROFILE_TABLE, symbol_table=SYMBOL_TABLE)
    else:
        SYMBOL_TABLE = f'symbol_{SYMBOL_PROVIDER.lower()}'
        PROFILE_TABLE = f'profile_{SYMBOL_PROVIDER.lower()}'
        df_symbols = get_symbols_finhub()

    fill_db(timeframe, profile_table=PROFILE_TABLE, avgVolumne=200000)


if __name__ == '__main__':
    refresh = lambda: refresh_symbol('60m')
    update = lambda: update_db('60m')

    sched = BlockingScheduler()
    sched.add_job(update, 'cron', id='update', hour='14-22', minute='28',
                  day_of_week='mon-fri')  # start at 29 because of warmup
    sched.add_job(refresh, 'cron', id='refresh', hour=1)
    sched.start()
