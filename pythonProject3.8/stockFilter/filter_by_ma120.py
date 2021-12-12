import pymysql
import pandas as pd
from sqlalchemy import create_engine
from xml.dom import minidom
import time
import os


today_date = time.strftime("%Y-%m-%d", time.localtime())

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
# 创建一个游标对象:有两种创建方法
cursor = connect.cursor()
# 使用游标的execute()方法执行sql语句
cursor.execute("SELECT * FROM stock_list where status = '0'")
# 使用fetchall()获取全部数据
stock_list = cursor.fetchall()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()


def do_filter():
    stock_xml = ''
    for stock in stock_list:
        sql = 'select * from history_data where code = %s order by date DESC' % (repr(stock[3]))
        data = pd.read_sql(sql, engine)

        if len(data) > 125:

            ma120 = data[0: 120]['close'].sum() / 120
            ma60 = data[0: 60]['close'].sum() / 60
            ma30 = data[0: 30]['close'].sum() / 30
            ma20 = data[0: 20]['close'].sum() / 20

            ma120_5 = data[5: 125]['close'].sum() / 120
            ma60_5 = data[5: 65]['close'].sum() / 60
            ma30_5 = data[5: 35]['close'].sum() / 30
            ma20_5 = data[5: 25]['close'].sum() / 20

            if ma120 < ma60 < data.loc[0]['close'] < ma30 < ma20 and ma60_5 < ma120_5 < ma30_5 < ma20_5:
                print(data.loc[0]['code'] + '____' + data.loc[0]['name'])
                if data.loc[0]['code'][0] == '6':
                    stock_xml = stock_xml + '0,'
                else:
                    stock_xml = stock_xml + '1,'
                stock_xml = stock_xml + data.loc[0]['code'] + ';'

    make_xml(stock_xml)


def make_xml(stock_xml):
    # 创建Document
    xml = minidom.Document()

    # 创建root节点
    root = xml.createElement('自选股')
    xml.appendChild(root)

    child = xml.createElement('List')
    child.setAttribute('Name', today_date + '_ma120')
    child.setAttribute('Key', today_date + '_ma120')
    child.setAttribute('Stock', stock_xml)
    root.appendChild(child)

    if not os.path.exists('/Users/zhangzoushin/Documents/xml/ma120'):
        os.mkdir('/Users/zhangzoushin/Documents/xml/ma120')

    fp = open('/Users/zhangzoushin/Documents/xml/ma120/' + today_date + '_ma120.xml', 'w', encoding='gbk')
    xml.writexml(fp, indent='	', addindent='	', newl='\n')


if __name__ == '__main__':
    do_filter()
