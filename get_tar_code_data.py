"""
把数据分门别类
"""
import sqlite3
from datetime import date

import pandas as pd

conn_mydata = sqlite3.connect('mydata.db')
conn_sys = sqlite3.connect('system.db')
conn_dm = sqlite3.connect('dm.db')


current_date = '20250426'
_current_date = date.today().strftime('%Y%m%d')

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
    return pd.read_sql_query(sql, conn_mydata)['contract'].tolist()


tradeDate = get_trade_day_series(current_date)

target_contract = get_target_contract(tradeDate[0])

for contract in target_contract:
    sql = f"""
    SELECT t1.*,t2.accfactor FROM {contract} t1 LEFT JOIN TraderOvk t2 on t1.tradingday = t2.tradingday and t1.contract = t2.prefix
    """
    data = pd.read_sql_query(sql, conn_mydata)
    data.sort_values(by=['tradingday','timestamp'], ascending=True, inplace=True,ignore_index=True)
    data['real_close'] = data['closeprice'] / data['accfactor']
    data['ret'] = data['real_close'].pct_change()
    data.to_sql(f'{contract}', conn_dm, if_exists='replace', index=False)



conn_dm.close()
conn_mydata.close()
conn_sys.close()
