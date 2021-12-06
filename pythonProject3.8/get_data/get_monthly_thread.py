import pymysql
import pandas as pd
from sqlalchemy import create_engine

connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
stock_list = pd.read_sql('select * from stock_list', connect)
engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')


def get_monthly(s_list):
    print('thread start...')
    for row in s_list.itertuples():
        sql = "select * from history_data where code = %s order by date desc" % (repr(getattr(row, 'code')))
        stock_data = pd.read_sql(sql, connect)
        print(getattr(row, 'code') + '----' + getattr(row, 'name'))

        # 设定转换周期period_type  转换为周是'W',月'M',季度线'Q',五分钟'5min',12天'12D'
        stock_data["date"] = pd.to_datetime(stock_data["date"])
        period_type = 'M'

        stock_data.set_index('date', inplace=True)

        # 进行转换，周线的每个变量都等于那一周中最后一个交易日的变量值
        period_stock_data = stock_data.resample(period_type).last()

        # stock_data.reset_index(inplace=True)
        #
        # period_stock_data['start_date'] = stock_data['date'].resample(period_type).first()
        #
        # period_stock_data['end_date'] = stock_data['date'].resample(period_type).last()

        # 周线的open等于那一周中第一个交易日的open
        period_stock_data['open'] = stock_data['open'].resample(period_type).first()

        # 周线的high等于那一周中的high的最大值
        period_stock_data['high'] = stock_data['high'].resample(period_type).max()

        # 周线的low等于那一周中的low的最大值
        period_stock_data['low'] = stock_data['low'].resample(period_type).min()

        period_stock_data['close'] = stock_data['close'].resample(period_type).last()

        # 周线的volume和money等于那一周中volume和money各自的和
        period_stock_data['volume'] = stock_data['volume'].resample(period_type).sum()

        period_stock_data['price_change'] = period_stock_data['close'] - period_stock_data['open']

        period_stock_data['p_change'] = (period_stock_data['close'] - period_stock_data['open']) / period_stock_data['open'] * 100

        # period_stock_data['money'] = stock_data['money'].resample(period_type,how='sum')

        # 股票在有些周一天都没有交易，将这些周去除

        # period_stock_data = period_stock_data[period_stock_data['volume'].notnull()]

        period_stock_data.reset_index(inplace=True)

        period_stock_data.drop(['id', 'turnover', 'amount', 'insert_date'], axis=1, inplace=True)

        period_stock_data.to_sql('monthly_data', engine, if_exists='append', index=False)


if __name__ == '__main__':
    # do_thread()
    get_monthly(stock_list)
