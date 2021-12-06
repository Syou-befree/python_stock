from selenium import webdriver
import pymysql
import requests
import pandas as pd
import time

connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
today_date = time.strftime("%Y-%m-%d", time.localtime())
stock_list = pd.read_sql('select * from stock_list where market_capital is NULL', connect)

proxy_pool = [
    {'http': '140.227.122.55:59394'},
    {'http': '85.26.146.169:80'},
    {'http': '58.185.160.30:8080'}
]


def get_cookie():
    driver = webdriver.Safari()
    driver.get('https://xueqiu.com/')
    time.sleep(5)
    _cookie = ''

    for cookie in driver.get_cookies():
        _cookie = _cookie + cookie['name'] + '=' + cookie['value'] + '; '

    _cookie = _cookie[:-2]
    driver.close()
    return _cookie


def get_today():
    cur = connect.cursor()
    i = 0

    cookie = get_cookie()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                      'Version/15.1 Safari/605.1.15',
        'Cookie': repr(cookie)
    }

    for row in stock_list.itertuples():
        i = i + 1
        print(i)

        # if i == 2501:
        #     cookie = get_cookie()
        #     headers = {
        #         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, '
        #                       'like Gecko) Version/15.1 Safari/605.1.15',
        #         'Cookie': repr(cookie)
        #     }

        stock_name = getattr(row, 'name')
        code = getattr(row, 'code')
        if getattr(row, '_2') == '上证' or getattr(row, '_2') == '科创':
            s_type = 'SH'
        else:
            s_type = 'SZ'

        # proxies = random.choice(proxy_pool)
        response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
                                + code + '&extend=detail', headers=headers, timeout=None)

        if response.json():
            json = response.json()
        else:
            continue

        open_price = json['data']['quote']['open']
        high = json['data']['quote']['high']
        close = json['data']['quote']['current']
        low = json['data']['quote']['low']
        if json['data']['quote']['volume']:
            volume = json['data']['quote']['volume'] / 100
        else:
            volume = 0
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

        if not (open_price is None):
            sql = 'INSERT INTO history_data \
                        (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`, `insert_date`) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                  % (repr('2021-11-23'), open_price, high, close, low, volume, price_change, p_change,
                     turnover, repr(code), repr(stock_name), amount, repr(today_date))

            cur.execute(sql)
            cur.connection.commit()
            sql_update = "update stock_list set market_capital = %s, float_market_capital = %s, eps = %s, pb = %s, " \
                         "pe_ttm = %s, pe_lyr = %s, pe_forecast = %s where code = %s" \
                         % (market_capital, float_market_capital, eps, pb, pe_ttm, pe_lyr, pe_forecast, repr(code))

            cur.execute(sql_update)
            cur.connection.commit()

            time.sleep(0.1)

    cur.close()


if __name__ == '__main__':
    get_today()
