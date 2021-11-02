# 将文件路径下的所有csv文件插入数据库
import os

import pandas as pd
import pymysql
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')
file_path = 'C:\myapp\python-project\pythonProject\DATA'
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")


def readAllFiles(file_path):
  stock_list = getStockList()
  fileList = os.listdir(file_path)
  for file in fileList:
    print('输入中。。。。', file)
    path = os.path.join(file_path, file)
    # data = pd.read_csv(path, header=None, skiprows=1)
    data = pd.read_csv(path)

    frame = pd.DataFrame(data)
    frame.insert(0, 'stock_name', file.split('.')[0])
    for stock in stock_list:
      if stock[1] == file.split('.')[0]:
        frame.insert(0, 'stock_code', stock[2])

    # print(frame.dtypes)
    # exit()

    frame['open'] = frame['open'].astype('float')
    frame['high'] = frame['high'].astype('float')
    frame['close'] = frame['close'].astype('float')
    frame['low'] = frame['low'].astype('float')
    frame['volume'] = frame['volume'].astype('int')

    frame['ma5'] = frame['ma5'].astype('float')
    frame['ma10'] = frame['ma10'].astype('float')
    frame['ma20'] = frame['ma20'].astype('float')

    frame['v_ma5'] = frame['v_ma5'].astype('int')
    frame['v_ma10'] = frame['v_ma10'].astype('int')
    frame['v_ma20'] = frame['v_ma20'].astype('int')

    # frame['turnover'] = frame['turnover'].astype('float')

    frame.to_sql('history_data', engine, if_exists='append', index=False)


def getStockList():
  # 连接数据库
  # connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
  # 创建一个游标对象:有两种创建方法
  cursor = connect.cursor()
  cursor.execute("SELECT * FROM stock")
  stock_list = cursor.fetchall()
  cursor.close()
  connect.close()
  return stock_list


if __name__ == '__main__':
  readAllFiles(file_path)
