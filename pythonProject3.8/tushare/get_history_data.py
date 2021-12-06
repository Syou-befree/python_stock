# 获取所有股票历史数据CSV
import io
import sys
import pymysql
import tushare as ts
from sqlalchemy import create_engine

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')
ts.set_token('301962b3cf206d63d987bdbf1b996c02dfe1f1ca762ac56e8d6bfdcb')

# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
# 创建一个游标对象:有两种创建方法
cursor = connect.cursor()
# 或：cursor=pymysql.cursors.Cursor(connect)
# 使用游标的execute()方法执行sql语句
cursor.execute("SELECT * FROM stock_list ORDER BY list_id ASC")
# 使用fetchall()获取全部数据
stockList = cursor.fetchall()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()

for stock in stockList:
    df = ts.get_hist_data(stock[3])

    if not (df is None):
        df.drop(['ma5', 'ma10', 'ma20', 'v_ma5', 'v_ma10', 'v_ma20'], axis=1, inplace=True)
        try:
            df['code'] = str(stock[3])
            df['name'] = str(stock[2])
            # print(df['code'], type(df['code']), df['name'], type(df['name']))
            print('next...')
            df.to_sql('history_data', engine, if_exists='append', index=True, index_label='date')
        except:
            print('报错了' + stock[2] + ' ' + stock[3])
