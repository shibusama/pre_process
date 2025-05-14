import sqlite3
import pandas as pd
import re
from datetime import date
from config import *


def get_tradedate_list(current_date):
    sql = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 10;"
    return pd.read_sql_query(sql, conn_sys)['tradingday'].tolist()


def gen_dm_db(current_date):
    def only_letters(s):
        return ''.join(re.findall(r'[A-Za-z]+', s))

    conn_his.create_function("only_letters", 1, only_letters)

    sql = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 10;"
    trade_date_list = pd.read_sql_query(sql, conn_sys)['tradingday'].tolist()
    sql = f"SELECT contract FROM facTag WHERE tradingday = {trade_date_list[0]}"
    target_contract = pd.read_sql_query(sql, conn_tmp)['contract'].tolist()

    for contract in target_contract:
        sql = f"SELECT tradingday,code FROM TraderOvk WHERE prefix = '{contract}' and tradingday >= {trade_date_list[-1]} ORDER BY tradingday DESC"
        df = pd.read_sql_query(sql, conn_tmp)
        tradedate_code_dict = df.set_index('tradingday')['code'].to_dict()
        sql = f"SELECT tradingday,prefix contract,accfactor FROM TraderOvk where prefix = '{contract}' and tradingday >= {trade_date_list[-1]} "
        df_accfactor = pd.read_sql_query(sql, conn_tmp)
        data_list = []
        for date in trade_date_list:
            sql = f"""
                            SELECT code,only_letters(code) contract,tradingday,timestamp,closeprice
                            FROM "bf{date}"
                            WHERE timestamp>='09:01:00' and timestamp<='14:45:00'
                            and code = '{tradedate_code_dict[date]}'
                          """
            df = pd.read_sql_query(sql, conn_his)
            data_list.append(df)
        data = pd.concat(data_list)
        data['tradingday'] = pd.to_datetime(data['tradingday'], format='%Y-%m-%d').dt.strftime('%Y%m%d')
        data = data.merge(df_accfactor, on=['tradingday', 'contract'], how='left')
        data.sort_values(by=['tradingday', 'timestamp'], ascending=True, inplace=True, ignore_index=True)
        data['real_close'] = data['closeprice'] / data['accfactor']
        data['ret'] = data['real_close'].pct_change()
        data.to_sql(contract, conn_dm, if_exists='replace', index=False)


def get_target_contract_info(date):
    """
    获取每日目标交易合约
    :return:
    """
    date = get_tradedate_list(current_date)[0]
    sql = f"SELECT contract,Fac,slc FROM facTag WHERE tradingday = '{date}'"
    contract_list = pd.read_sql_query(sql, conn_tmp)
    contract_list[['Rs', 'Rl']] = contract_list['slc'].str.split('-', expand=True)

    ass = f"""('{"','".join(contract_list['contract'].tolist())}')"""
    sql = f"SELECT code,multiple FROM futureinfo where code in {ass}"
    df = pd.read_sql_query(sql, conn_sys)
    contract_list = contract_list.merge(df, left_on='contract', right_on='code', how='left')
    contract_list = contract_list[['contract', 'multiple', 'Fac', 'Rs', 'Rl']]

    sql = f"SELECT prefix,code,accfactor FROM TraderOvk where prefix in {ass} and tradingday = '{date}'"
    df = pd.read_sql_query(sql, conn_tmp)
    contract_list = contract_list.merge(df, left_on='contract', right_on='prefix', how='left')
    contract_list = contract_list[['contract', 'multiple', 'Fac', 'Rs', 'Rl', 'code', 'accfactor']]
    contract_list.to_sql('futureinfo', conn_dm, if_exists='replace', index=False)


if __name__ == "__main__":
    # 1. 连接 SQLite 数据库
    conn_his = sqlite3.connect(his_db_path)
    conn_sys = sqlite3.connect(system_db_path)
    conn_tmp = sqlite3.connect(tmp_db_path)
    conn_dm = sqlite3.connect(dm_db_path)
    current_date = '20250426'

    # gen_dm_db(current_date)
    get_target_contract_info(current_date)

    # 6. 关闭连接
    conn_his.close()
    conn_sys.close()
    conn_tmp.close()
    conn_dm.close()
