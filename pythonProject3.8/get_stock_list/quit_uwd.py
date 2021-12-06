# 获取所有股票历史数据CSV
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pymysql
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
# 创建一个游标对象:有两种创建方法
cursor = connect.cursor()
# 或：cursor=pymysql.cursors.Cursor(connect)
# 使用游标的execute()方法执行sql语句
cursor.execute("SELECT * FROM stock_list")
# 使用fetchall()获取全部数据
stockList = cursor.fetchall()
for stock in stockList:
    # print(type(stock[1]))
    # exit()
    a = stock[2].replace('-', '')
    b = a.replace('W', '')
    c = b.replace('D', '')
    d = c.replace('U', '')
    sql = 'update stock_list set name = %s where code = %s' % ("'" + d + "'", "'" + stock[3] + "'")
    print(sql)
    cursor.execute(sql)
    connect.commit()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()
