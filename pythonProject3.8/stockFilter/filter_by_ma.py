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

        if len(data) > 338:
            data144 = data[0: 144]
            data169 = data[0: 169]
            data288 = data[0: 288]
            data338 = data[0: 338]

            ma144 = data144['close'].sum() / 144
            ma169 = data169['close'].sum() / 169
            ma288 = data288['close'].sum() / 288
            ma338 = data338['close'].sum() / 338

            if data.loc[0]['close'] < ma169 < ma144 < ma288 < ma338 and stock[5] < 5000000000:
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
    child.setAttribute('Name', today_date + '_ma')
    child.setAttribute('Key', today_date + '_ma')
    child.setAttribute('Stock', stock_xml)
    root.appendChild(child)

    if not os.path.exists('/Users/zhangzoushin/Documents/xml/ma'):
        os.mkdir('/Users/zhangzoushin/Documents/xml/ma')

    fp = open('/Users/zhangzoushin/Documents/xml/ma/' + today_date + '_ma.xml', 'w', encoding='gbk')
    xml.writexml(fp, indent='	', addindent='	', newl='\n')


if __name__ == '__main__':
    do_filter()
