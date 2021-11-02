# 获取所有股票历史数据CSV
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import tushare as ts
import pymysql
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
# 创建一个游标对象:有两种创建方法
cursor = connect.cursor()
# 或：cursor=pymysql.cursors.Cursor(connect)
# 使用游标的execute()方法执行sql语句
cursor.execute("SELECT * FROM stock")
# 使用fetchall()获取全部数据
stockList = cursor.fetchall()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()

for stock in stockList:
  name = stock[1]
  code = stock[2]
  if name.find('ST') < 0:
    df = ts.get_hist_data(code)
    df['code'] = code
    df['name'] = name
    print(df)
    # df.to_csv(f'{name}.csv')
    df.to_sql('history_data', engine, if_exists='append', index=True, index_label='date')
    df
