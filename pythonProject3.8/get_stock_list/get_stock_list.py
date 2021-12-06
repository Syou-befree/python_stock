from selenium import webdriver

import pymysql

import re


def main():
    urls = ['https://www.banban.cn/gupiao/list_sh.html',
            'https://www.banban.cn/gupiao/list_sz.html',
            'https://www.banban.cn/gupiao/list_cyb.html']

    classes = ['上证', '深证', '创业']

    driver = webdriver.Safari()

    conn = pymysql.connect(
        host='127.0.0.1',
        user='root'
        , password='root',
        port=3306,
        db='stock',
        charset='utf8'
    )

    cur = conn.cursor()

    list_id = 1

    for index in range(len(urls)):
        class_ = classes[index]
        driver.get(urls[index])
        contents = driver.find_elements_by_xpath('//*[@id="ctrlfscont"]/ul/li')
        for content in contents:
            content = content.text
            cur.execute('insert into stock_list values("%s","%s","%s","%s",%s)' % (
                list_id, class_, content.split('(')[0], re.compile(r'\d+').findall(content)[0], 0))
            list_id += 1
    cur.connection.commit()
    cur.close()
    driver.close()


if __name__ == '__main__':
    main()
