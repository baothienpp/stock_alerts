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
    return keys['column_name'].to_list()


def build_values(entry):
    values_clean = []
    for value in entry:
        if value is not None:
            try:
                values_clean.append(value.replace("'", ""))
            except Exception:
                values_clean.append(value)
        else:
            values_clean.append(value)
    values = (", ".join(["'{e}'".format(e=e) for e in values_clean]))
    klam_values = "({})".format(values)
    return klam_values


def insert_on_conflict_do_update(df: pd.DataFrame, table_name, schema='public', batch=10):
    with session_scope() as session:
        df = df.replace({np.nan: 'null'})
        insert_dict = df.values.tolist()
        column_names_list = list(df.columns.values)
        column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])
        check_cols = get_primary_keys_of_table(table_name=table_name)

        for chunk in chunks(list_obj=insert_dict, batch_size=batch):
            insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                                  table_name=table_name,
                                                                                                  column_names=column_names_str)
            # clean values
            # num_cores = multiprocessing.cpu_count()
            # values = jb.Parallel(n_jobs=num_cores)(
            #     jb.delayed(build_values)(entry) for entry in chunk)  # prefer="threads"
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
                excluded = '{excluded} "{col_name_left_side}"=EXCLUDED."{col_name_right_side}",'.format(
                    excluded=excluded,
                    col_name_left_side=col,
                    col_name_right_side=col)

            excluded = excluded[:-1]
            insert_stmt_str = '{insert} ON CONFLICT ({pkey}) DO UPDATE {excluded_stmt};'.format(insert=insert_stmt_str,
                                                                                                pkey=pkey,
                                                                                                excluded_stmt=excluded)
            insert_stmt_str = insert_stmt_str.replace("'null'", "null")

            session.execute(sqlalchemy.text(insert_stmt_str))


def chunks(list_obj, batch_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(list_obj), batch_size):
        yield list_obj[i:i + batch_size]
