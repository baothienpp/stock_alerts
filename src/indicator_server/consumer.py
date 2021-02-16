import json
import certifi
from kafka import KafkaConsumer
from src.utils.logging import log

from src.indicator_server.indicators.indicators import TA
from src.indicator_server.indicators.roc import *
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
        pd.to_numeric, errors='coerce')
    return df


def calculate_william(df, symbol, period):
    df_symbol = df[df['symbol'] == symbol]
    df_symbol.sort_values(by=['datetime'], inplace=True)
    df_symbol.reset_index().drop(columns=['index'], inplace=True)
    df_symbol['william'] = TA.WILLIAMS(df_symbol, period)
    df_symbol.dropna(inplace=True)

    return df_symbol


def add_indicators(table='', symbols=None, mode='full', batch_size=50):
    log.info('Indicators')
    william_period = 100
    if not isColumnExist(col='william', table=table):
        execute_sql_statement(ADD_COL.format(table_name=f'"{table}"', col_name='william', data_type='numeric'))
        mode = 'full'

    for chunk in chunks(symbols, batch_size=batch_size):
        symbols_string_list = ", ".join("'{0}'".format(s) for s in chunk)

        log.info('Get data ...')
        if mode == 'full':
            df_selected = read_from_sql_statement(
                f'''SELECT datetime, symbol, open, high, low, close, volume
                    FROM "{table}" WHERE symbol in ({symbols_string_list})''')
        else:
            # Determine the last n null rows
            offset = read_from_sql_statement(
                NUMBER_OF_ROWS_TO_UPDATE.format(table_name=table,
                                                symbols=symbols_string_list, non_null_col='william'))
            if offset['max'].empty:
                offset = 5
            else:
                offset = offset['max'].iloc[0]

            # TODO select more efficiently by selecting only current row and (current row - n)
            df_selected = read_from_sql_statement(
                SELECT_LAST_N_ROWS.format(table_name=table, n_rows=william_period + offset,
                                          symbols=symbols_string_list))
        df_selected = prefilter(df_selected)

        log.info('Processing data ...')
        process = lambda x: calculate_william(df_selected, x, period=william_period)
        output = [process(symbol) for symbol in chunk]

        log.info('Inserting into db ...')
        insert_on_conflict_do_update(pd.concat(output), table_name=f'\"{table}\"', schema='public', batch=5000)


def add_roc(table='', symbols=None, mode='full', batch_size=100):
    log.info('Rate of change')
    for chunk in chunks(symbols, batch_size=batch_size):
        log.info('Get data ...')
        symbols_string_list = ", ".join("'{0}'".format(s) for s in chunk)

        log.info('Get data ...')
        if mode == 'full':
            df_selected = read_from_sql_statement(
                f'''SELECT datetime, symbol, close FROM "{table}" WHERE symbol in ({symbols_string_list})''')
            date_cols = get_past_date(df_selected)
            # Add columns to table
            for col in date_cols:
                execute_sql_statement(
                    ADD_COL.format(table_name=f'"{table}"', col_name='roc_' + col, data_type='numeric'))
        else:
            # Determine the last n null rows
            offset = read_from_sql_statement(
                NUMBER_OF_ROWS_TO_UPDATE.format(table_name=table,
                                                symbols=symbols_string_list, non_null_col='roc_last_day'))
            if offset['max'].empty:
                offset = '7d'
            else:
                offset = offset['max'].iloc[0]
                offset = '{}d'.format(offset + 7)

            df_selected = read_from_sql_statement(
                SELECT_LAST_N_ROWS.format(table_name=table, n_rows=1, symbols=symbols_string_list))

            date_cols = get_past_date(df_selected)
            extra_filter = filter_date_range_query(df_selected, date_cols=date_cols, offset=offset)
            query = SELECT_SYMBOL_WITH_FILTER.format(table=table, symbol_list=symbols_string_list,
                                                     cols='datetime, symbol, close',
                                                     extra_filter=f'AND ({extra_filter})')
            df_past_date = read_from_sql_statement(query)
            df_selected = pd.concat([df_selected, df_past_date])
            df_selected = df_selected[['datetime', 'symbol', 'close']]
            get_past_date(df_selected)

        for col in date_cols:
            df_roc = calculate_roc(df_selected, past_date_col=col)
            log.info('Inserting into db ...')
            insert_on_conflict_do_update(df_roc, table_name=f'\"{table}\"', schema='public', batch=5000)


if __name__ == '__main__':
    for message in consumer:
        try:
            message = message.value
            log.info(message)
            table = message.get('table')
            symbols = message.get('symbols')
            period = message.get('period')
            mode = message.get('mode')
        except Exception as e:
            log.debug(e)
            log.debug('Message error')
            continue

        batch_size = 50
        # add_indicators(table=table, symbols=symbols, mode=mode, batch_size=batch_size)

        if table == '1d':
            add_roc(table=table, symbols=symbols, mode=mode, batch_size=batch_size)

        log.info('Finish')
