import pymysql
import tushare as ts
import time
import pandas as pd
from sqlalchemy import create_engine
import requests

headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/95.0.4638.69 Safari/537.36 ',
    'Cookie': 'device_id=317999456f5dcce56fd1ad7ef29c4f7d; s=c712q4ufud; bid=8f290f34a382a6c24995fa0fd605cc00_kvjm0x9w; '
              'xq_a_token=e2cc583accc7b1e0776b346c44c2cee192647cb3; xqat=e2cc583accc7b1e0776b346c44c2cee192647cb3; '
              'xq_r_token=c89f9b73cd5ef5ca8ac5499a39fe4cfc80f6cfb9; '
              'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg1NjUyNjQwNjUsImlzcyI6InVjIiwiZXhwIjoxNjM4NTk1MzU5LCJjdG0iOjE2MzYxNjQ2OTE5MjgsImNpZCI6ImQ5ZDBuNEFadXAifQ.WJZgxx8QJRFjzDAPl17P40NvcFJlW-efmEJ1dQkjSpcxWI6fUljPgONpZl6i5pc6EGtPUaGq5LwEV4V4Iswu6dzRehatanm5xv_pl85Ce0ulyn2xcaDMuRGTCO3ePmjxtcfuo-0cUVIGuwvfAB-RY_-YZqHZvywjRc69zBdaluyPPdh8TO3pm4oaqGaY5A8KNHuaOCw3H79OWQK7_fpWz6mBPRcZlKtiIBN8A9qH15J8Jl9A16LWdGS8cgu5c_q6j4zDkZImKggA4iD-bZ5uNvwFaIafTvYguRmPzAOG9LdxvBgl4suRYXyB6ek2-mM3z_Jxz1JwB1eepw9QDGLZVA; xq_is_login=1; '
              'u=8565264065; Hm_lvt_1db88642e346389874251b5a1eded6e3=1636331471,1636409657,1636604672,1636658420; '
              'snbim_minify=true; is_overseas=1; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1636751231'
}

pro = ts.pro_api('ac630b40e4ece72cb0685c2f85e25fc7a5a5e8778a7e88ca63058099')
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

today = time.strftime("%Y%m%d", time.localtime())
today_date = time.strftime("%Y-%m-%d", time.localtime())

stock_list = pd.read_sql('select * from stock_list', connect)


def get_today():
    # data = pro.daily(**{
    #     "ts_code": "",
    #     "trade_date": today,
    #     "start_date": "",
    #     "end_date": "",
    #     "offset": "",
    #     "limit": ""
    # }, fields=[
    #     "ts_code",
    #     "trade_date",
    #     "open",
    #     "high",
    #     "low",
    #     "close",
    #     "change",
    #     "pct_chg",
    #     "vol",
    #     "amount"
    # ])
    #
    # data.to_csv('./today.csv', index=False)
    # exit()

    data = pd.read_csv('./today.csv')

    data.loc[:, 'ts_code'] = data.loc[:, 'ts_code'].str.replace('.SH', '', regex=True)
    data.loc[:, 'ts_code'] = data.loc[:, 'ts_code'].str.replace('.SZ', '', regex=True)
    data.loc[:, 'ts_code'] = data.loc[:, 'ts_code'].str.replace('.BJ', '', regex=True)
    data.loc[:, 'trade_date'] = today_date

    cur = connect.cursor()
    for row in data.itertuples():
        cur.execute('SELECT DISTINCT class, name, code FROM stock_list WHERE code = %s', getattr(row, 'ts_code'))
        stock = cur.fetchone()

        if not stock:
            print('...')
        else:
            stock_name = stock[1]
            code = getattr(row, 'ts_code')
            if stock[0] == '上证' or stock[0] == '科创':
                s_type = 'SH'
            else:
                s_type = 'SZ'

            print('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
                  + getattr(row, 'ts_code') + '&extend=detail')
            exit()
            response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=' + s_type
                                    + getattr(row, 'ts_code') + '&extend=detail', headers=headers)
            json = response.json()
            open_price = json['data']['quote']['open']
            high = json['data']['quote']['high']
            close = json['data']['quote']['current']
            low = json['data']['quote']['low']
            volume = json['data']['quote']['volume']
            price_change = json['data']['quote']['chg']
            p_change = json['data']['quote']['percent']
            turnover = json['data']['quote']['turnover_rate']
            amount = json['data']['quote']['amount']

            sql = 'INSERT INTO history_data \
                            (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                  % (repr(today_date), open_price, high, close, low, volume, price_change, p_change,
                     turnover, repr(code), repr(stock_name), amount)

            # if turnover:
            #     sql = 'INSERT INTO history_data \
            #     (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`) \
            #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
            #           % (repr(today_date), open, high, close, low, volume, price_change, p_change,
            #              turnover, repr(code), repr(stock_name), amount)
            # else:
            #     sql = 'INSERT INTO history_data \
            #                     (`date`, `open`, high, `close`, `low`, `volume`, `price_change`, `p_change`, `turnover`, `code`, `name`, `amount`) \
            #                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
            #           % (repr(today_date), open, high, close, low, volume, price_change, p_change,
            #              turnover, repr(code), repr(stock_name), amount)

            cur.execute(sql)
            cur.connection.commit()

    cur.close()


if __name__ == '__main__':
    get_today()
