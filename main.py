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


def only_letters(s):
    return ''.join(re.findall(r'[A-Za-z]+', s))


## 获取目标交易合约
conn_his.create_function("only_letters", 1, only_letters)
sql_tar_con = "SELECT contract FROM facTag WHERE tradingday = ( SELECT max( tradingday ) FROM facTag )"
tar_contract = pd.read_sql_query(sql_tar_con, conn_tmp)['contract'].tolist()

##  获取近60天交易日序列
sql_latest_60 = f"select * from tradeday where tradingday < {current_date} order by tradingday DESC Limit 5;"
tradeDate = pd.read_sql_query(sql_latest_60, conn_sys)['tradingday'].tolist()

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
