import json
from src.utils.logging import log
from kafka import KafkaConsumer

from src.indicator_server.indicators.indicators import TA
from src.utils.sql_utils import *
from src.sql.db_sql import *
from src.kafka_setting import KAFKA_GROUP_ID, KAFKA_TOPIC

pd.options.mode.chained_assignment = None

consumer = KafkaConsumer(KAFKA_TOPIC,
                         group_id=KAFKA_GROUP_ID,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         bootstrap_servers=['localhost:9092'])


def prefilter(df):
    df.drop(columns=['r'], inplace=True, errors='ignore')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(
        pd.to_numeric,
        errors='coerce')
    return df


def add_indicator(df, symbol, period):
    df_symbol = df[df['symbol'] == symbol]
    df_symbol.sort_values(by=['datetime'], inplace=True)
    df_symbol.reset_index().drop(columns=['index'], inplace=True)
    df_symbol['william'] = TA.WILLIAMS(df_symbol, period)
    df_symbol.dropna(inplace=True)
    insert_on_conflict_do_update(df_symbol, table_name=f'\"{table}\"', schema='public', batch=5000)
    return df_symbol.copy()


if __name__ == '__main__':
    for message in consumer:
        try:
            message = json.loads(message.value)
            log.info(message)
            table = message['table']
            symbols = message['symbols']
            period = message['period']
            mode = message['mode']
        except Exception as e:
            log.debug(e)
            log.debug('Message error')
            continue

        if not isColumnExist('william', table):
            log.info('Column not exists. Creating ...')
            execute_sql_statement(ADD_COL.format(table_name=f'"{table}"', col_name='william', data_type='numeric'))

        symbols_string_list = ", ".join("'{0}'".format(s) for s in symbols)

        if mode == 'full':
            df_selected = read_from_sql_statement(
                f'''SELECT * FROM "{table}" WHERE symbol in ({symbols_string_list})''')
        else:
            df_selected = read_from_sql_statement(
                SELECT_LAST_N_ROWS.format(table_name='60m', n_rows=period + 10, symbols=symbols_string_list))
        df_selected = prefilter(df_selected)

        process = lambda x: add_indicator(df_selected, x, period=period)
        output = [process(symbol) for symbol in symbols]

        log.info('Inserting into db ...')
        log.info('Finish')
