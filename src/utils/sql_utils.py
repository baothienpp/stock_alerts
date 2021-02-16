from contextlib import contextmanager
import sqlalchemy
import numpy as np
import pandas as pd

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine

db_secret = 'stock'
db_host = 'localhost'
db_port = 54329

db_uri = f'postgresql://{db_secret}:{db_secret}@{db_host}:{db_port}/{db_secret}'
print(f"product_domain: {db_uri}")

engine = create_engine(db_uri, pool_pre_ping=True)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def read_from_sql_statement(statement: str) -> pd.DataFrame:
    with session_scope() as session:
        result = session.execute(statement)
        columns = result.cursor.description
        row = [e.values() for e in result.fetchall()]
        df = pd.DataFrame(row, columns=[c.name for c in columns])
    return df


def execute_sql_statement(statement: str) -> None:
    with session_scope() as session:
        session.execute(statement)


def isTableExist(table, schema='public'):
    df = read_from_sql_statement(f"""SELECT EXISTS (SELECT FROM pg_tables
                                               WHERE  schemaname = '{schema}'
                                               AND    tablename  = '{table}')""")
    return df.iloc[0][0]


def isColumnExist(col, table, schema='public'):
    df = read_from_sql_statement(f"""SELECT EXISTS (SELECT column_name 
                                    FROM information_schema.columns 
                                    WHERE table_name='{table}' AND table_schema='{schema}' AND column_name='{col}')""")

    return df.iloc[0][0]


def get_primary_keys_of_table(table_name: str) -> list:
    table_name = table_name.replace('"', '')
    statement = f"select \
                    a.attname as column_name \
                    from \
                        pg_class t, \
                        pg_class i, \
                        pg_index ix, \
                        pg_attribute a \
                    where \
                        t.oid = ix.indrelid \
                        and i.oid = ix.indexrelid \
                        and a.attrelid = t.oid \
                        and a.attnum = ANY(ix.indkey) \
                        and t.relkind = 'r' \
                        and t.relname = '{table_name}'"

    keys = read_from_sql_statement(statement=statement)
    keys.drop_duplicates(inplace=True)
    return keys['column_name'].to_list()


def build_values(entry):
    values_clean = []
    for value in entry:
        if value is not None:
            try:
                values_clean.append(value.replace("'", "").replace(':', '\\:'))  # escape colon (binding parameter)
            except Exception:
                values_clean.append(value)
        else:
            values_clean.append(value)
    values = (", ".join(["'{e}'".format(e=e) for e in values_clean]))
    klam_values = "({})".format(values)
    return klam_values


def insert_on_conflict_do_increment(df: pd.DataFrame, table_name, schema='public', count_col='count', batch=10000):
    df = df.replace({np.nan: 'null'})
    insert_dict = df.values.tolist()
    column_names_list = list(df.columns.values)
    column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])
    check_cols = get_primary_keys_of_table(table_name=table_name)

    for chunk in chunks(list_obj=insert_dict, batch_size=batch):
        insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                              table_name=table_name,
                                                                                              column_names=column_names_str)

        values = []
        for entry in chunk:
            values.append(build_values(entry))
        values = ','.join(values)
        values = values.strip('[]')
        values = values.replace('"', '')
        insert_stmt_str = "{} {}".format(insert_stmt_str, values)

        # get primary keys of table

        pkey = (", ".join(["{e}".format(e=e) for e in check_cols]))
        excluded = 'SET "{col_name_left_side}"=EXCLUDED."{col_name_right_side}" + 1,'.format(
                                                                                        col_name_left_side=count_col,
                                                                                        col_name_right_side=count_col)

        excluded = excluded[:-1]
        insert_stmt_str = '{insert} ON CONFLICT ({pkey}) DO UPDATE {excluded_stmt};'.format(insert=insert_stmt_str,
                                                                                            pkey=pkey,
                                                                                            excluded_stmt=excluded)
        insert_stmt_str = insert_stmt_str.replace("'null'", "null")
        with session_scope() as session:
            session.execute(sqlalchemy.text(insert_stmt_str))


def insert_on_conflict_do_update(df: pd.DataFrame, table_name, schema='public', batch=10000):
    df = df.replace({np.nan: 'null'})
    insert_dict = df.values.tolist()
    column_names_list = list(df.columns.values)
    column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])
    check_cols = get_primary_keys_of_table(table_name=table_name)

    for chunk in chunks(list_obj=insert_dict, batch_size=batch):
        insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                              table_name=table_name,
                                                                                              column_names=column_names_str)
        values = []
        for entry in chunk:
            values.append(build_values(entry))
        values = ','.join(values)
        values = values.strip('[]')
        values = values.replace('"', '')
        insert_stmt_str = "{} {}".format(insert_stmt_str, values)

        # get primary keys of table

        pkey = (", ".join(["{e}".format(e=e) for e in check_cols]))
        excluded = " Set "
        for col in column_names_list:
            if col in check_cols:
                continue
            excluded = '{excluded} "{col_name_left_side}"=EXCLUDED."{col_name_right_side}",'.format(
                excluded=excluded,
                col_name_left_side=col,
                col_name_right_side=col)

        excluded = excluded[:-1]
        insert_stmt_str = '{insert} ON CONFLICT ({pkey}) DO UPDATE {excluded_stmt};'.format(insert=insert_stmt_str,
                                                                                            pkey=pkey,
                                                                                            excluded_stmt=excluded)
        insert_stmt_str = insert_stmt_str.replace("'null'", "null")
        with session_scope() as session:
            session.execute(sqlalchemy.text(insert_stmt_str))


def insert_on_conflict_ignore(df: pd.DataFrame, table_name, schema='public', batch=10000):
    df = df.replace({np.nan: 'null'})
    insert_dict = df.values.tolist()
    column_names_list = list(df.columns.values)
    column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])
    check_cols = get_primary_keys_of_table(table_name=table_name)

    for chunk in chunks(list_obj=insert_dict, batch_size=batch):
        insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                              table_name=table_name,
                                                                                              column_names=column_names_str)
        values = []
        for entry in chunk:
            values.append(build_values(entry))
        values = ','.join(values)
        values = values.strip('[]')
        values = values.replace('"', '')
        insert_stmt_str = "{} {}".format(insert_stmt_str, values)

        # get primary keys of table

        pkey = (", ".join(["{e}".format(e=e) for e in check_cols]))

        insert_stmt_str = '{insert} ON CONFLICT ({pkey}) DO NOTHING;'.format(insert=insert_stmt_str,
                                                                             pkey=pkey)
        insert_stmt_str = insert_stmt_str.replace("'null'", "null")
        with session_scope() as session:
            session.execute(sqlalchemy.text(insert_stmt_str))


def insert_return_ids(df: pd.DataFrame, table_name, schema='public', sequencial_key='id'):
    df = df.replace({np.nan: 'null'})
    insert_dict = df.values.tolist()
    column_names_list = list(df.columns.values)
    column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])

    insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                          table_name=table_name,
                                                                                          column_names=column_names_str)
    values = []
    for entry in insert_dict:
        values.append(build_values(entry))
    values = ','.join(values)
    values = values.strip('[]')
    values = values.replace('"', '')
    insert_stmt_str = "{} {}".format(insert_stmt_str, values)

    insert_stmt_str = '{insert} RETURNING {sequencial_key}'.format(insert=insert_stmt_str,
                                                                   sequencial_key=sequencial_key)
    insert_stmt_str = insert_stmt_str.replace("'null'", "null")
    with session_scope() as session:
        ids = session.execute(insert_stmt_str).cursor.fetchall()
    return [val for sublist in ids for val in sublist]


def add_columns_if_not_exist(table, column, data_type):
    ADD_COL = """ALTER TABLE {table_name} 
                 ADD COLUMN IF NOT EXISTS {col_name} {data_type}"""
    with session_scope() as session:
        cursor = session.execute(
            ADD_COL.format(table_name=f'"{table}"', col_name=column, data_type=data_type))
    return cursor


def chunks(list_obj, batch_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(list_obj), batch_size):
        yield list_obj[i:i + batch_size]
