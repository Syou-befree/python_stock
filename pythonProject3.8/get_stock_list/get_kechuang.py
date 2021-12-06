import requests
import pandas as pd
import pymysql

connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
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


def get_kechuang():
    print('获取科创板股票名称')
    stock_type = '科创'
    data = pd.read_csv('./today.csv')

    list_id = 4223

    data.loc[:, 'ts_code'] = data.loc[:, 'ts_code'].str.replace('.SH', '', regex=True)
    data.loc[:, 'ts_code'] = data.loc[:, 'ts_code'].str.replace('.SZ', '', regex=True)

    cur = connect.cursor()

    for row in data.itertuples():
        if getattr(row, 'ts_code')[0: 2] == '68':
            response = requests.get('https://stock.xueqiu.com/v5/stock/quote.json?symbol=SH'
                                    + getattr(row, 'ts_code') + '&extend=detail', headers=headers)
            json = response.json()
            name = json['data']['quote']['name']
            if name:
                cur.execute(
                    'INSERT INTO stock_list \
                    (`list_id`, `class`, `name`, `code`, `status`) \
                    VALUES (%s, %s, %s, %s, %s)'
                    % (repr(list_id), repr(stock_type), repr(name), getattr(row, 'ts_code'), '0'))
                list_id = list_id + 1
                cur.connection.commit()

    cur.close()


if __name__ == '__main__':
    get_kechuang()
