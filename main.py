import sqlite3
import pandas as pd
import re
from datetime import date

# 1. 连接 SQLite 数据库
conn_his = sqlite3.connect('his2025.db')
conn_sys = sqlite3.connect('system.db')
conn_tmp = sqlite3.connect('tmp.db')
conn_mydata = sqlite3.connect('mydata.db')

current_date = '20250426'
_current_date = date.today().strftime('%Y%m%d')


def only_letters(s):
    return ''.join(re.findall(r'[A-Za-z]+', s))


conn_his.create_function("only_letters", 1, only_letters)


def get_target_code(date):
    """
    获取每日（需要交易）主力合约代码
    :param date:
    :return:
    """
    sql = f"SELECT code FROM TraderOvk WHERE tradingday = {date} AND prefix in (SELECT contract FROM facTag WHERE tradingday = {date})"

    return pd.read_sql_query(sql, conn_tmp)['code'].tolist()


def get_trade_day_series(current_date):
    """
    获取近current_date天交易日序列
    :param current_date:
    :return:
    """
    sql = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 10;"
    return pd.read_sql_query(sql, conn_sys)['tradingday'].tolist()


def get_target_contract(date):
    """
    获取每日目标交易合约
    :return:
    """
    sql = f"SELECT contract FROM facTag WHERE tradingday = '{date}'"
    # print(sql)
    return pd.read_sql_query(sql, conn_tmp)['contract'].tolist()


tradeDate = get_trade_day_series(current_date)

"""
step1:确定需要多少天的交易日序列
step2:把当天需要交易的合约算出来
step3:确定当前天需要交易合约的历史主力合约代码
"""

###
# for date in tradeDate:
#     print(date)
target_contract = get_target_contract(tradeDate[0])
# target_contract = ['AP']
for contract in target_contract:
    print(contract)
    sql = f"SELECT tradingday,code FROM TraderOvk WHERE prefix = '{contract}' and tradingday >= {tradeDate[-1]} ORDER BY tradingday DESC"
    df0 = pd.read_sql_query(sql, conn_tmp)
    tradedate_code_dict = df0.set_index('tradingday')['code'].to_dict()
    data_list = []
    for date in tradeDate:
        sql = f"""
                     SELECT code,only_letters(code) contract,tradingday,timestamp,closeprice
                     FROM "bf{date}"
                     WHERE timestamp>='09:01:00' and timestamp<='14:45:00'
                     and code = '{tradedate_code_dict[date]}'
                   """
        df = pd.read_sql_query(sql, conn_his)
        df['tradingday'] = pd.to_datetime(df['tradingday'], format='%Y-%m-%d').dt.strftime('%Y%m%d')
        data_list.append(df)
    data = pd.concat(data_list)
    data.to_sql(contract, conn_mydata, if_exists='replace', index=False)

# 6. 关闭连接
conn_his.close()
conn_sys.close()
conn_tmp.close()
conn_mydata.close()
