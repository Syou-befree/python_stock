import pymysql
import pandas as pd
import time
from threading import Thread
from dbutils.pooled_db import PooledDB
import numpy as np

connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
today_date = time.strftime("%Y-%m-%d", time.localtime())
stock_list = pd.read_sql('select * from stock_list', connect)

stock_list_1 = stock_list[stock_list['list_id'] <= 500]
stock_list_2 = stock_list[(stock_list['list_id'] > 500) & (stock_list['list_id'] <= 1000)]
stock_list_3 = stock_list[(stock_list['list_id'] > 1000) & (stock_list['list_id'] <= 1500)]
stock_list_4 = stock_list[(stock_list['list_id'] > 1500) & (stock_list['list_id'] <= 2000)]
stock_list_5 = stock_list[(stock_list['list_id'] > 2000) & (stock_list['list_id'] <= 2500)]
stock_list_6 = stock_list[(stock_list['list_id'] > 2500) & (stock_list['list_id'] <= 3000)]
stock_list_7 = stock_list[(stock_list['list_id'] > 3000) & (stock_list['list_id'] <= 3500)]
stock_list_8 = stock_list[(stock_list['list_id'] > 3500) & (stock_list['list_id'] <= 4000)]
stock_list_9 = stock_list[stock_list['list_id'] > 4000]

pool = PooledDB(
    pymysql,
    10,  # 最大连接数
    host='localhost',
    user='root',
    port=3306,
    passwd='root',
    db='stock',
    use_unicode=True)


def do_thread():
    t1 = Thread(target=get_weekly, args=(stock_list_1,))
    t2 = Thread(target=get_weekly, args=(stock_list_2,))
    t3 = Thread(target=get_weekly, args=(stock_list_3,))
    t4 = Thread(target=get_weekly, args=(stock_list_4,))
    t5 = Thread(target=get_weekly, args=(stock_list_5,))
    t6 = Thread(target=get_weekly, args=(stock_list_6,))
    t7 = Thread(target=get_weekly, args=(stock_list_7,))
    t8 = Thread(target=get_weekly, args=(stock_list_8,))
    t9 = Thread(target=get_weekly, args=(stock_list_9,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()


def get_weekly(s_list):
    print('thread start...')
    for row in s_list.itertuples():
        sql = "select * from history_data where code = %s order by date desc" % (repr(getattr(row, 'code')))
        stock_data = pd.read_sql(sql, pool.connection())
        print(getattr(row, 'code') + '----' + getattr(row, 'name'))

        start = 0
        for i in range(1, len(stock_data)):

            date1 = time.strptime(str(stock_data.loc[i - 1]['date']), "%Y-%m-%d")
            date2 = time.strptime(str(stock_data.loc[i]['date']), "%Y-%m-%d")
            # date1 = time.strftime("%Y-%m-%d", date1)
            # date2 = time.strftime("%Y-%m-%d", date2)
            # date1 = time.strptime(date1, "%Y-%m-%d")
            # date2 = time.strptime(date2, "%Y-%m-%d")
            timestamp_day1 = int(time.mktime(date1))
            timestamp_day2 = int(time.mktime(date2))

            result = (timestamp_day1 - timestamp_day2) // 60 // 60 // 24

            if result > 2:
                code = stock_data.loc[start]['code']
                name = stock_data.loc[start]['name']
                week_start = stock_data.loc[i - 1]['date']  # 实际开始日
                week_end = stock_data.loc[start]['date']  # 实际截止日
                week_open = stock_data.loc[i - 1]['open']  # 开盘价
                week_close = stock_data.loc[start]['close']  # 收盘价
                week_high = np.max(stock_data['high'][start:i])  # 最高价
                week_low = np.min(stock_data['low'][start:i])  # 最低价
                week_volume = stock_data['volume'][start: i].sum()

                if not stock_data['amount'][start:i].isnull().any():
                    week_amount = stock_data['amount'][start: i].sum()
                    sql = 'INSERT INTO weekly_data \
                                 (`start_date`, `end_date`, `open`, high, `close`, `low`, `volume`, `code`, `name`, `amount`, `insert_date`) \
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                          % (repr(week_start), repr(week_end), week_open, week_high, week_close, week_low, week_volume,
                             repr(code), repr(name), week_amount, repr(today_date))
                else:
                    sql = 'INSERT INTO weekly_data \
                                 (`start_date`, `end_date`, `open`, high, `close`, `low`, `volume`, `code`, `name`, `insert_date`) \
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                          % (repr(week_start), repr(week_end), week_open, week_high, week_close, week_low, week_volume,
                             repr(code), repr(name), repr(today_date))
                con_to_db(sql)
                start = i


def con_to_db(sql):
    con = pool.connection()
    cur = con.cursor()
    cur.execute(sql)
    cur.connection.commit()
    cur.close()
    con.close()


if __name__ == '__main__':
    do_thread()
