from selenium import webdriver
import pymysql
import requests
import pandas as pd
import time
from threading import Thread
from dbutils.pooled_db import PooledDB

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

driver = webdriver.Safari()
_cookie = ''
driver.delete_all_cookies()
driver.get('https://xueqiu.com/')
for c in driver.get_cookies():
    _cookie = _cookie + c['name'] + '=' + c['value'] + '; '
_cookie = _cookie[:-2]
driver.close()

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                  'Version/15.1 Safari/605.1.15',
    'Cookie': repr(_cookie)
}

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
    t1 = Thread(target=update_st, args=(stock_list_1,))
    t2 = Thread(target=update_st, args=(stock_list_2,))
    t3 = Thread(target=update_st, args=(stock_list_3,))
    t4 = Thread(target=update_st, args=(stock_list_4,))
    t5 = Thread(target=update_st, args=(stock_list_5,))
    t6 = Thread(target=update_st, args=(stock_list_6,))
    t7 = Thread(target=update_st, args=(stock_list_7,))
    t8 = Thread(target=update_st, args=(stock_list_8,))
    t9 = Thread(target=update_st, args=(stock_list_9,))

    t1.start()
    time.sleep(1)
    t2.start()
    time.sleep(1)
    t3.start()
    time.sleep(1)
    t4.start()
    time.sleep(1)
    t5.start()
    time.sleep(1)
    t6.start()
    time.sleep(1)
    t7.start()
    time.sleep(1)
    t8.start()
    time.sleep(1)
    t9.start()


def update_st(s_list):
    print('thread start...')
    for row in s_list.itertuples():
        stock_name = getattr(row, 'name')
        code = getattr(row, 'code')
        print(code + '---' + stock_name)
        if getattr(row, '_2') == '上证' or getattr(row, '_2') == '科创':
            s_type = 'SH'
        else:
            s_type = 'SZ'

        response = requests.get('https://xueqiu.com/stock/industry/stockList.json?code=' + s_type
                                + code + '&type=1&size=100', headers=headers, timeout=None)

        if response.json():
            json = response.json()
        else:
            continue

        bankuai = json['industryname']

        con = pool.connection()
        cur = con.cursor()

        sql_update = "update stock_list set bankuai = %s where code = %s" \
                     % (repr(bankuai), repr(code))

        cur.execute(sql_update)
        cur.connection.commit()
        cur.close()
        con.close()


if __name__ == '__main__':
    do_thread()
