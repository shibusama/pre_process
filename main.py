import sqlite3
import pandas as pd
import re
from datetime import date

# 1. 连接 SQLite 数据库
conn_his = sqlite3.connect('his2025.db')
conn_sys = sqlite3.connect('system.db')
conn_tmp = sqlite3.connect('tmp.db')

current_date = '20250426'
_current_date = date.today().strftime('%Y%m%d')


# def only_letters(s):
#     return ''.join(re.findall(r'[A-Za-z]+', s))
#
# ## 获取目标交易合约
# conn_his.create_function("only_letters", 1, only_letters)
# sql_tar_con = "SELECT contract FROM facTag WHERE tradingday = ( SELECT max( tradingday ) FROM facTag )"
# tar_contract = pd.read_sql_query(sql_tar_con, conn_tmp)['contract'].tolist()


def get_day_code(date):
    """
    获取每日主力合约代码
    :param date:
    :return:
    """
    sql = f"SELECT code FROM TraderOvk WHERE tradingday = {date}"

    return {date: pd.read_sql_query(sql, conn_tmp)['code'].tolist()}


def get_trade_day_series(current_date):
    """
    获取近current_date天交易日序列
    :param current_date:
    :return:
    """
    sql = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 5;"
    return pd.read_sql_query(sql, conn_his)['tradingday'].tolist()

def get_target_contract(date):
    """
    获取每日目标交易合约
    :return:
    """
    sql = f"SELECT contract FROM facTag WHERE tradingday = '{date}'"
    return pd.read_sql_query(sql, conn_tmp)['contract'].tolist()


tradeDate = get_trade_day_series(current_date)

print(1 / 0)

## 获取累计复权因子
sql_acc_factor = f"SELECT tradingday,prefix,code,accfactor FROM TraderOvk WHERE tradingday = {current_date}"

sql_list = []
contract_data = []
tar_contract = ['rb']
for contract in tar_contract:
    for date in tradeDate:
        sql = f"""
                 SELECT code,only_letters(code) contract,tradingday,timestamp,closeprice 
                 FROM "bf{date}" 
                 WHERE timestamp>='09:01:00' and timestamp<='14:45:00'
                 and contract = '{contract}'
                 """
        pd.read_sql_query(sql, conn_his)
        contract_data.append(pd.read_sql_query(sql, conn_his))
    data = pd.concat(contract_data)
    print(data)

print(data)
print(len(tradeDate))
print(len(tar_contract))
print(len(sql_list))
df = pd.read_sql_query(sql_list[0], conn_his)
print(df)

print(1 / 0)

# 6. 关闭连接
conn_his.close()
conn_sys.close()
conn_tmp.close()
