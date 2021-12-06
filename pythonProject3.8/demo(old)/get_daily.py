import pymysql
import tushare as ts
import time
import pandas as pd
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/80.0.3987.149 Safari/537.36 ',
    'Cookie': 'device_id=317999456f5dcce56fd1ad7ef29c4f7d; s=c012mcdp4h; Hm_lvt_1db88642e346389874251b5a1eded6e3=1637413195,1637488474,1637519161,1637537648; xq_a_token=69220b628949c65e60028ad4a3ea1202d39be4f7; xqat=69220b628949c65e60028ad4a3ea1202d39be4f7; xq_r_token=be68a75415a31df640f482746fc8520e56b49cad; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg1NjUyNjQwNjUsImlzcyI6InVjIiwiZXhwIjoxNjM5OTgzMDA5LCJjdG0iOjE2Mzc1Mzc2NzAzODMsImNpZCI6ImQ5ZDBuNEFadXAifQ.M_kdrYx84L2QFkLoTtkxxPxXTCQEHBhM6-t9JnECeg6kgh-CzKeIztO2cEaBBV-igc-b9HEZ4IoNi2lN5A5x-524RdBsNJEH3J9A09_1tj-E2wOkAPz7jsoQHueUjlAjDoKpw1qwndGYStfkN9mHVsOUEb9IK9zYyNLGbD5QyWH1tMGAFO3Gm_j5o9Ml7kXMlnBNVkTCwW78Hu4ZjaMDi4kxKeFP6OdmnZEESL2gSYzhVEGqnRx8vJ9OXd8kFzsJw3HhnwbNOtS-mKPn6zhSlccAVLP6z97ub9ZJCngflvqvkV7lu6xRp8bUSVCr8o5mjt1ek4Xg-J9yCGSe1Aictw; xq_is_login=1; u=8565264065; snbim_minify=true; bid=8f290f34a382a6c24995fa0fd605cc00_kw9wx2zr; is_overseas=1; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1637576223'
}

pro = ts.pro_api('ac630b40e4ece72cb0685c2f85e25fc7a5a5e8778a7e88ca63058099')
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
today_date = time.strftime("%Y-%m-%d", time.localtime())
stock_list = pd.read_sql('select * from stock_list where list_id > 4271', connect)
# # 创建好session对象
# session = requests.Session()
# # 第一次使用session捕获且存储cookie,猜测对雪球网的首页发起的请求可能会产生cookie
# main_url = "https://xueqiu.com"
# session.get(main_url, headers=headers)  # 捕获且存储cookie


def get_today():
    cur = connect.cursor()
    i = 0

    for row in stock_list.itertuples():
        i = i + 1
        print(i)
        stock_name = getattr(row, 'name')
        code = getattr(row, 'code')
        if getattr(row, '_2') == '上证' or getattr(row, '_2') == '科创':
            s_type = 'SH'
        else:
            s_type = 'SZ'

        response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
                                + code + '&extend=detail', headers=headers, timeout=None)

        if response.json():
            json = response.json()
        else:
            continue

        # while response.json()['data']['quote'] is None:
        #     response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
        #                             + code + '&extend=detail', headers=headers, verify=False)
        #     json = response.json()

        open_price = json['data']['quote']['open']
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

        if not (open_price is None):
            sql = 'INSERT INTO history_data \
                        (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`, `insert_date`) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                  % (repr(today_date), open_price, high, close, low, volume, price_change, p_change,
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
