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
    sql = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 5;"
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
# for date in tradeDate:
#     target_code = get_target_code(date)
#     code_str = f"""('{"','".join(target_code)}')"""
#     sql = f"""
#              SELECT code,only_letters(code) contract,tradingday,timestamp,closeprice
#              FROM "bf{date}"
#              WHERE timestamp>='09:01:00' and timestamp<='14:45:00'
#              and code in {code_str}
#            """
#     data = pd.read_sql_query(sql, conn_his)
#     print(data)
#     data.to_sql(f"bf{date}", conn_mydata, if_exists='replace', index=False)

# 获取每日交易合约
print(tradeDate[0])
target_contract = get_target_contract(tradeDate[0])  # 要把今天需要交易的合约的历史数据找出来
print(target_contract)

# print(1 / 0)
#
# ## 获取累计复权因子
# sql_acc_factor = f"SELECT tradingday,prefix,code,accfactor FROM TraderOvk WHERE tradingday = {current_date}"
#
# sql_list = []
# contract_data = []
# tar_contract = ['rb']
# for contract in tar_contract:
#     for date in tradeDate:
#         sql = f"""
#                  SELECT code,only_letters(code) contract,tradingday,timestamp,closeprice
#                  FROM "bf{date}"
#                  WHERE timestamp>='09:01:00' and timestamp<='14:45:00'
#                  and contract = '{contract}'
#                  """
#         pd.read_sql_query(sql, conn_his)
#         contract_data.append(pd.read_sql_query(sql, conn_his))
#     data = pd.concat(contract_data)
#     print(data)
#
# print(data)
# print(len(tradeDate))
# print(len(tar_contract))
# print(len(sql_list))
# df = pd.read_sql_query(sql_list[0], conn_his)
# print(df)
#
# print(1 / 0)

# 6. 关闭连接
conn_his.close()
conn_sys.close()
conn_tmp.close()
conn_mydata.close()
