from selenium import webdriver
import pymysql
import requests
import pandas as pd
import time
from threading import Thread
import threading
from dbutils.pooled_db import PooledDB

# from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()


# proxy_pool = [
#     {'http': '140.227.122.55:59394',
#      'https': '140.227.122.55:59394'},
#     {'http': '58.185.160.30:8080',
#      'https': '58.185.160.30:8080'},
# ]

connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
reset_sql = 'UPDATE stock_list set market_capital = NULL'
cur = connect.cursor()
cur.execute(reset_sql)
cur.connection.commit()
cur.close()

today_date = time.strftime("%Y-%m-%d", time.localtime())
stock_list = pd.read_sql('select * from stock_list where market_capital is null', connect)

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

lock = threading.Lock()
i = 0

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
    t1 = Thread(target=get_today, args=(stock_list_1,))
    t2 = Thread(target=get_today, args=(stock_list_2,))
    t3 = Thread(target=get_today, args=(stock_list_3,))
    t4 = Thread(target=get_today, args=(stock_list_4,))
    t5 = Thread(target=get_today, args=(stock_list_5,))
    t6 = Thread(target=get_today, args=(stock_list_6,))
    t7 = Thread(target=get_today, args=(stock_list_7,))
    t8 = Thread(target=get_today, args=(stock_list_8,))
    t9 = Thread(target=get_today, args=(stock_list_9,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()


def get_today(s_list):
    print('thread start...')
    for row in s_list.itertuples():
        stock_name = getattr(row, 'name')
        code = getattr(row, 'code')
        print(code + '---' + stock_name)
        if getattr(row, '_2') == '上证' or getattr(row, '_2') == '科创':
            s_type = 'SH'
        else:
            s_type = 'SZ'

        try:
            response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
                                    + code + '&extend=detail', headers=headers, timeout=None)

            if response.json():
                json = response.json()
            else:
                continue

            open_price = json['data']['quote']['open']
            if open_price is None:
                continue
            high = json['data']['quote']['high']
            close = json['data']['quote']['current']
            low = json['data']['quote']['low']
            volume = json['data']['quote']['volume'] / 100
            price_change = json['data']['quote']['chg']
            p_change = json['data']['quote']['percent']
            turnover = json['data']['quote']['turnover_rate']
            amount = json['data']['quote']['amount']

            # 股票基础信息更新用的数据
            market_capital = json['data']['quote']['market_capital']  # 总市值
            float_market_capital = json['data']['quote']['float_market_capital']  # 流通市值
            eps = json['data']['quote']['eps']  # 每股收益
            pb = json['data']['quote']['pb']  # 市净率
            pe_ttm = json['data']['quote']['pe_ttm']  # 市盈率(TTM)
            pe_lyr = json['data']['quote']['pe_lyr']  # 市盈率(静)
            pe_forecast = json['data']['quote']['pb']  # 市盈率(动)

            sql = 'INSERT INTO history_data \
                        (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`, `insert_date`) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                  % (repr(today_date), open_price, high, close, low, volume, price_change, p_change,
                     turnover, repr(code), repr(stock_name), amount, repr(today_date))
            con_to_db(sql)

            sql_update = "update stock_list set market_capital = %s, float_market_capital = %s, eps = %s, pb = %s, " \
                         "pe_ttm = %s, pe_lyr = %s, pe_forecast = %s where code = %s" \
                         % (market_capital, float_market_capital, eps, pb, pe_ttm, pe_lyr, pe_forecast, repr(code))
            con_to_db(sql_update)

            time.sleep(0.5)
        except Exception as e:
            print(e)
            continue


def con_to_db(sql):
    con = pool.connection()
    cur = con.cursor()
    cur.execute(sql)
    cur.connection.commit()
    cur.close()
    con.close()


if __name__ == '__main__':
    do_thread()
