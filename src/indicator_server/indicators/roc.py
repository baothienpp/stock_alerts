import pandas as pd
import datetime


def filter_date_range_query(df, date_cols, offset='7d'):
    date_condition = []
    for col in date_cols:
        date = pd.to_datetime(df[col][0]).date()
        date_condition.append(f"""(datetime >= '{date}'::date - interval '{offset}' and datetime <= '{date}'::date)""")
    return ' OR '.join(date_condition)


# query = '''SELECT {cols} FROM public."{table}"
#           WHERE symbol IN ({symbol_list}) {extra_filter}'''
# query = query.format(cols='datetime,symbol,close', extra_filter=f'AND ({filter_date_range_query(df, date_cols)})',
#                      table='1d', symbol_list="'BIZD','HBI'")


def get_past_date(df, return_date_cols=True):
    df['last_year'] = df['datetime'].apply(lambda x: datetime.date(day=31, month=12, year=x.year - 1))
    df['last_quarter'] = df['datetime'].apply(lambda x: (x - pd.tseries.offsets.QuarterEnd()).date())
    df['last_6_months'] = df['datetime'].apply(lambda x: (x - pd.tseries.offsets.DateOffset(months=6)).date())
    df['last_month'] = df['datetime'].apply(lambda x: (x - pd.tseries.offsets.BMonthEnd()).date())
    df['last_friday'] = df['datetime'].apply(
        lambda x: x - datetime.timedelta(days=x.weekday()) + datetime.timedelta(days=4, weeks=-1))
    df['last_day'] = df['datetime'].apply(lambda x: (x - pd.tseries.offsets.BDay()).date())
    if return_date_cols:
        return ['last_year', 'last_quarter', 'last_6_months', 'last_month', 'last_friday', 'last_day']


def calculate_roc(df, past_date_col='last_day'):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df[past_date_col] = pd.to_datetime(df[past_date_col])
    df['close'] = pd.to_numeric(df['close'])
    lastprice_df = pd.merge_asof(df[['datetime', 'symbol', 'close', past_date_col]].sort_values(past_date_col),
                                 df[['datetime', 'symbol', 'close']].sort_values('datetime'),
                                 left_on=past_date_col,
                                 right_on='datetime',
                                 by='symbol',
                                 suffixes=('', '_y'))

    def roc(row):
        try:
            return ((row['close'] - row['close_y']) / row['close_y']) * 100
        except Exception:
            return 0

    lastprice_df['roc_' + past_date_col] = lastprice_df.apply(roc, axis=1)
    lastprice_df.drop(columns=[past_date_col, 'datetime_y', 'close_y'], inplace=True)
    return lastprice_df
