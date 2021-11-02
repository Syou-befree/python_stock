import requests
import pprint
import time

url = 'https://xueqiu.com/service/v5/stock/screener/quote/list?page=1&size=90&order=desc&orderby=percent&order_by' \
      '=percent&market=CN&type=sh_sz&_=1632134908732 '

headers = {
  'Accept': '*/*',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/92.0.4515.159 Safari/537.36 '
}

response = requests.get(url=url, headers=headers)
json_data = response.json()
data_list = json_data['data']['list']

for data in data_list:
  date = time.strftime("%Y%m%d", time.localtime())
  stock_code = data['symbol']
  stock_name = data['name']
  current_price = data['current']
  price_change = data['chg']
  percent_change = data['percent']
  # 年初至今涨跌幅
  percent_change_this_year = data['current_year_percent']
  # 成交量
  volume = data['volume']
  # 成交额
  amount = data['amount']
  # 量比
  volume_ratio = data['volume_ratio']
  # 换手率
  turnover_rate = data['turnover_rate']
  # 市盈率（TTM）
  pe_ttm = data['pe_ttm']
  # 股息率
  guxilv = data['dividend_yield']
  # 总市值
  market_capital = data['market_capital']
  # 流通市值
  float_market_capital = data['float_market_capital']
  # 雪球关注人数
  followers = data['followers']
  # 总股本
  total_shares = data['total_shares']
  # 流通股
  float_shares = data['float_shares']
  # 振幅
  amplitude = data['amplitude']
  # 每股收益
  eps = data['eps']
  # 市净率
  pb = data['pb']

