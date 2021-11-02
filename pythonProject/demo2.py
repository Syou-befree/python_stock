import pymysql
import tushare as ts
import time
import pandas as pd
from sqlalchemy import create_engine

pro = ts.pro_api('ac630b40e4ece72cb0685c2f85e25fc7a5a5e8778a7e88ca63058099')
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')
today = time.strftime("%Y%m%d", time.localtime())
stock_list = pd.read_sql('select * from stock', connect)


def get_today():
  data = pro.daily(**{
    "ts_code": "",
    "trade_date": '20210917',
    "start_date": "",
    "end_date": "",
    "offset": "",
    "limit": ""
  }, fields=[
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "change",
    "pct_chg",
    "vol",
    "amount"
  ])

  data.columns = ["stock_code", "date", "open", "high", "low", "close", "price_change", "p_change", "volume", "amount"]

  data['stock_code'] = data['stock_code'].astype('string')
  data['date'] = data['date'].astype('string')
  data['open'] = data['open'].astype('float')
  data['open'] = data['open'].astype('float')
  data['high'] = data['high'].astype('float')
  data['close'] = data['close'].astype('float')
  data['low'] = data['low'].astype('float')
  data['price_change'] = data['price_change'].astype('string')
  data['p_change'] = data['p_change'].astype('string')
  data['volume'] = data['volume'].astype('float')
  data['amount'] = data['amount'].astype('float')
  print('输入中。。。。')
  data.to_sql('history_data', engine, if_exists='append', index=False)
  # data['stock_code'] = data.apply(lambda x: reset_tock_code(x['stock_code']), axis=1)
  # print(data)
  # exit()
  # data['stock_name'] = get_stock_name(data['stock_code'])
  #
  # print(data)


# def reset_tock_code(stock_code):
#   code = stock_code.split('.')[0]
#   return code


# def get_stock_name(stock_code):
#   row = stock_list.loc[stock_list['stock_no'] == stock_code]
#   if row['stock_name'].find('*') :
#     row['stock_name'] = row['stock_name'].replace('*', '')
#   return row['stock_name'].replace('*', '')


if __name__ == '__main__':
  get_today()
