# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 14:59:39 2021

@author: 04144
"""

import pandas as pd
import time
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pyodbc


class CompanyQuery:
    def __init__(self, server, db, uid, psw, base_table, updt_table, date):
        self.server = server
        self.db = db
        self.uid = uid
        self.psw = psw
        self.base_table = base_table
        self.updt_table = updt_table
        self.date = self.date

    def retrieve_updated_data(self):
        """ Retrieve data as data frame

        :return: updated data frame
        """
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
                server, db, uid, pwd))
            cursor = conn.cursor()
            query_command = '''
              SELECT* FROM 
              (SELECT Keyword_Level2 FROM {}
              WHERE Dept_ID='12'
              AND ACTIVE='1') L
              FULL JOIN 
              (SELECT STOCK_ID,stock_nm FROM {}) R
              ON L.Keyword_Level2=R.stock_id
              WHERE L.Keyword_Level2<>R.stock_id
              '''.format(self.base_table, self.updt_table)
            cursor.execute(query_command)
            conn.commit()
            new_updated_df = pd.read_sql(query_command, conn)
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)

        return new_updated_df

    def delete_data(self, delete_list):
        """

        :param delete_list:
        """
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
                server, db, uid, pwd))
            cursor = conn.cursor()
            query_command = '''
              DELETE FROM {}
                  WHERE stock_id in ({})
              '''.format(self.base_table, delete_list)
            cursor.execute(query_command)
            conn.commit()
        except Exception as e:
            print(e)


def get_stock_info(stock_list):
    """ Get xxx stock info

    :param stock_list: the list of stock ticker/number
    :return: stock info DataFrame
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    # ua=UserAgent()
    chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0')
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.get('https://mops.twse.com.tw/mops/web/t05st03')  # 股票資訊
    stock_name_chinese = []
    stock_id = []
    comp_cat = []   # what is comp_cat
    category = []   # what kind of category
    for i in stock_list:
        input = browser.find_element_by_xpath('//*[@id="co_id"]')
        time.sleep(random.randint(2, 5))
        input.send_keys(i)
        # input.send_keys(3710)股票代號
        time.sleep(random.randint(2, 5))
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        time.sleep(random.randint(2, 5))
        browser.find_element_by_xpath('//*[@id="co_id"]').clear()
        # WebDriverWait(browser,3)
        # print([n.get_text() for n in soup.select('div#autoCompilete-dbody1')][0].split(' ')[1])
        if [n.get_text() for n in soup.select('div#autoCompilete-dbody1')] == []:
            stock_id.append(i)
            stock_name_chinese.append('')
            comp_cat.append('')
            category.append('')
        else:
            stock_id.append(i)
            stock_name_chinese.append([n.get_text() for n in soup.select('div#autoCompilete-dbody1')][0].split(' ')[1])
            comp_cat.append([i.get_text() for i in soup.select('li#auto-title')][0])
            category.append([i.get_text()[:2] for i in soup.select('li#auto-title')][0])
        print(i)
        # print(stock_nm_ch)
    browser.quit()
    key_stock_info = pd.DataFrame({
        'stock_id': stock_id,
        'stock_nm': stock_name_chinese,
        'comp_cat': comp_cat,
        'category': category})
    try:
        conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
        server, db, uid, pwd))
        cursor = conn.cursor()
        for index, row in key_stock_info.iterrows():
            cursor.execute(
                "INSERT INTO dbo.TWSE_STOCK_REF([stock_id],[stock_nm],[category_desc],[category])" "VALUES(?,?,?,?)",
                (row['stock_id'],
                 row['stock_nm'],
                 row['comp_cat'],
                 row['category']))
            conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)

    return key_stock_info


if __name__ == '__main__':

    server = '10.129.7.155'
    db = 'db'
    uid = 'uid'
    psw = 'psw'
    base_table =
    updated_table =
    company_query = CompanyQuery(server, db, uid, psw, base_table, updated_table, date)
    updated_stock_df = company_query.retrieve_updated_data()
    if len(updated_stock_df) == 0:
        pass
    elif updated_stock_df['Keyword_Level2'].isnull().value.any():
        add = updated_stock_df[updated_stock_df['stock_id'].isnull()]['Keyword_Level2'].to_list()
        key_stock_info = get_stock_info(add)
    elif updated_stock_df['stock_id'].isnull().value.any():
        delete_list = updated_stock_df[updated_stock_df['Keyword_Level2'].isnull()]['stock_id'].to_list()
        CompanyQuery.retrieve_updated_data

