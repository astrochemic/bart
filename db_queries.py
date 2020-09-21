import os
import time
import numpy as np
import pandas as pd
from mysql import connector
import psycopg2

from helper import CACHE_DIR, SQL_DIR, DTFormat


def query_postgresql(query, verbose=True):
    if verbose: print(query+'\n')
    connection_string = os.getenv('CONNECTION_STRING')
    cur = psycopg2.connect(connection_string).cursor()
    try:
        start_time = time.time()
        cur.execute(query)
        end_time = time.time()
        if verbose:
            print('Query execution took {0:.1f} seconds\n\n\n'.format(end_time-start_time))
    except Exception as e:
        if verbose: print('Query failed: {}\n\n\n'.format(e))
        return dict(), []
    colnames = [desc[0] for desc in cur.description]
    raw_data = cur.fetchall()
    cur.close()
    return raw_data, colnames


def query_sam(query, verbose=True):
    if verbose: print(query+'\n')
    cnx = connector.connect(user=os.getenv('SAM_USER'),
                            password=os.getenv('SAM_PASS'),
                            host=os.getenv('SAM_HOST'))
    cur = cnx.cursor()
    try:
        start_time = time.time()
        cur.execute(query)
        end_time = time.time()
        if verbose:
            print('Query execution took {0:.1f} seconds\n\n\n'.format(end_time-start_time))
    except Exception as e:
        if verbose: print('Query failed: {}\n\n\n'.format(e))
        return dict(), []
    colnames = [desc[0] for desc in cur.description]
    raw_data = cur.fetchall()
    cur.close()
    cnx.close()
    return raw_data, colnames


def query_mcb(query, verbose=True):
    if verbose: print(query+'\n')
    cnx = connector.connect(user=os.getenv('MCB_USER'),
                            password=os.getenv('MCB_PASS'),
                            host=os.getenv('MCB_HOST'))
    cur = cnx.cursor()
    try:
        start_time = time.time()
        cur.execute(query)
        end_time = time.time()
        if verbose:
            print('Query execution took {0:.1f} seconds\n\n\n'.format(end_time-start_time))
    except Exception as e:
        if verbose: print('Query failed: {}\n\n\n'.format(e))
        return dict(), []
    colnames = [desc[0] for desc in cur.description]
    raw_data = cur.fetchall()
    cur.close()
    cnx.close()
    return raw_data, colnames


def _get_from_db(country_code, start_date, end_date, query_function, query='', sql_file='', cache_prefix='', cache=True, verbose=True, min_trx_id=0):
    data_file_path = '{}/{}_{}_{}_{}.csv'.format(
        CACHE_DIR,
        cache_prefix,
        country_code,
        start_date.strftime(DTFormat.Y_M_D),
        end_date.strftime(DTFormat.Y_M_D)
    )
    if cache and os.path.exists(data_file_path):
        try:
            df = pd.read_csv(data_file_path, low_memory=False)
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    if query == '' and sql_file == '':
        pd.DataFrame().to_csv(data_file_path, index=False)
        return pd.DataFrame()

    if query == '':
        with open(SQL_DIR + '/{}'.format(sql_file)) as f:
            query = f.read()
        query = query.format(
            country_code=country_code,
            start_date=start_date,
            end_date=end_date,
            sam_database='{}db'.format(country_code.lower()),
            min_trx_id=min_trx_id
        )

    raw_data, colnames = query_function(query, verbose=verbose)

    df = pd.DataFrame(raw_data, columns=colnames)
    if cache:
        df.to_csv(data_file_path, index=False)
    return df


def _get_platform_active(country, start_date, end_date, cache=True):
    df = pd.DataFrame()

    df_sam = _get_from_db(country, start_date, end_date, query_sam,
                          sql_file='sam-platform_active.sql',
                          cache_prefix='sam_active', cache=cache)
    if not df_sam.empty:
        df = df.append(df_sam, sort=True)

    df_mcb = _get_from_db(country, start_date, end_date, query_mcb,
                          sql_file='mcb_active.sql',
                          cache_prefix='mcb_active', cache=cache)
    if not df_mcb.empty:
        df_mcb['status'].replace({'active': 'A', 'inactive': 'I'}, inplace=True)
        df = df.append(df_mcb, sort=True)

    if df.empty:
        return df

    for col in ['accountid', 'msisdn', 'serviceid', 'service_identifier1', 'service_identifier2']:
        if df[col].dtype not in [np.dtype('O'), np.dtype('float')]:
            df[col] = df[col].astype(int).map(str)
        else:
            df[col] = df[col].astype(str)

    return df


def _get_platform_schedule(country, start_date, end_date, cache=True):
    df = pd.DataFrame()

    df_sam = _get_from_db(country, start_date, end_date, query_sam,
                          sql_file='sam-platform_schedule.sql',
                          cache_prefix='sam_schedule', cache=cache)
    if not df_sam.empty:
        df_sam['platform'] = 'sam'
        df = df.append(df_sam, sort=True)
    else:
        for col in ['serviceid', 'operator_code', 'tariff', 'billing_days']:
            df[col] = []

    for col in ['operator_code']:
        if col in df:
            df[col] = df[col].fillna('Unknown')
    for col in ['scheduleid', 'serviceid', 'service_identifier1', 'service_identifier2', 'tariff']:
        if col in df and df[col].dtype != np.dtype('O'):
            df[col] = df[col].astype(int).map(str)
    if 'updatedate' in df:
        df['updatedate'] = pd.to_datetime(df['updatedate'])

    return df


def _get_redshift_transactions(country, start_date, end_date, cache=True):
    df = _get_from_db(country, start_date, end_date, query_postgresql,
                      sql_file='redshift_trx_per_user.sql',
                      cache_prefix='red_trx_per_user', cache=cache)
    for col in ['msisdn', 'service_identifier1', 'service_identifier2', 'tariff']:
        if col in df and df[col].dtype != np.dtype('O'):
            df[col] = df[col].astype(int).map(str)
    return df
