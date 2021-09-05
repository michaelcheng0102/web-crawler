# -*- coding: utf-8 -*-
"""
Created on Thu May  6 10:01:00 2021

@author: 04144
"""

import pandas as pd
import time
import datetime
from datetime import timedelta, date
import random
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from fake_useragent import UserAgent
import numpy as np
import math
import jieba
import pyodbc
import smtplib
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header

# set stock_list
key_stock_info = pd.read_csv('D:/04144_Profile/Desktop/crawl/project01_twse/key_stock_info_a.csv')


# key_stock_info['stock_id']=key_stock_info['stock_id'].apply(lambda x: str(x).strip())
# key_stock_info['stock_nm']=key_stock_info['stock_nm'].apply(lambda x: str(x).strip())


def mail_send_by_smtp(recipient_list, sender, subject, head='', body='', filepath=''):
    # create mail body
    msg = MIMEMultipart()
    if head:
        msg['From'] = Header(head, 'utf-8')
    else:
        msg['From'] = sender
    msg['To'] = ';'.join(recipient_list)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # create attachment if needed
    if filepath:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(filepath, "rb").read())
        encoders.encode_base64(part)
        filename = Path(filepath).name
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    # Send the message via topco-global SMTP server
    s = smtplib.SMTP('spam.topco-global.com', 25)
    s.sendmail(sender, recipient_list, msg.as_string())
    s.quit()


# stock_list=pd.read_csv('D:/04144_Profile/Desktop/crawl/project01_twse/stock_list.csv')
# stock information/when stock_list changed

# key_stock_info['category']=key_stock_info['comp_cat'].apply(lambda x: x[:2])
# function for title sequence
def get_actual_sequence(seq_list):
    output_seq = []
    temp_seq = []
    prev_added = True
    for i in range(len(seq_list)):
        cur = int(seq_list[i])
        if i == 0 or (prev_added and (output_seq[-1] + 1) == cur):
            output_seq.append(i)
        elif prev_added and temp_seq == []:  # found a '1'
            prev_added = False
        elif cur == (output_seq[-1] + 1):  # found next num
            if temp_seq != [] or cur > (int(seq_list[i - 1]) + 1):
                # if the next num has already been added
                # this is the num that we should be adding to the output list
                temp_seq = []
                output_seq.append(i)
                prev_added = True
            else:
                # if not, then we should add to temp list
                temp_seq.append(i)
        elif temp_seq != [] and cur == (temp_seq[-1] + 1):
            temp_seq.append(i)
    return output_seq + temp_seq


# new words function
class NewWord(object):
    def __init__(self, max_len_word, radio, freq, dop_base, left_free_base, right_free_base, left_right_diff):
        self.max_len_word = max_len_word
        self.radio = radio
        self.words = {}
        self.freq = freq
        self.dop_base = dop_base
        self.left_free_base = left_free_base
        self.right_free_base = right_free_base
        self.left_right_diff = left_right_diff

    def find_words(self, doc):
        '''
        find all possible words
        :param doc:
        :param max_len_word:
        :return:
        '''
        len_doc = len(doc)
        for i in range(len_doc):
            for j in range(i + 1, i + self.max_len_word + 1):
                if doc[i:j] in self.words:
                    self.words[doc[i:j]]['freq'] += 1
                else:
                    self.words[doc[i:j]] = {}
                    self.words[doc[i:j]]['freq'] = 1

    def dop(self):
        '''
        calculate: if p(ab)>>>p(a)*p(b)
        :param words:
        :return:
        '''
        len_words = len(self.words)
        for k, v in self.words.items():
            self.words[k]['freq_radio'] = self.words[k]['freq'] / (5 * len_words)
        for k, v in self.words.items():
            dop = []
            l = len(k)
            if l == 1:
                self.words[k]['dop'] = 0
            else:
                for i in range(1, l):
                    word = self.words[k[0:i]]['freq_radio'] * self.words[k[i:l]]['freq_radio']
                    dop.append(word)
                dop = sum(dop)
                self.words[k]['dop'] = math.log(self.words[k]['freq_radio'] / dop)

    def left_free(self, doc):
        '''

        :param words:
        :return:
        '''
        for k, v in self.words.items():
            left_list = [m.start() for m in re.finditer(k, doc) if m.start() != 1]
            len_left_list = len(left_list)
            left_item = {}
            for li in left_list:
                if doc[li - 1] in left_item:
                    left_item[doc[li - 1]] += 1
                else:
                    left_item[doc[li - 1]] = 1
            left = 0
            for _k, _v in left_item.items():
                left += abs((left_item[_k] / len_left_list) * math.log(1 / len(left_item)))
            self.words[k]['left_free'] = left

    def right_free(self, doc):
        '''
        entropy
        :param words:
        :return:
        '''
        for k, v in self.words.items():
            right_list = [m.start() for m in re.finditer(k, doc) if m.start() < len(doc) - 5]
            len_right_list = len(right_list)
            right_item = {}
            for li in right_list:
                if doc[li + len(k)] in right_item:
                    right_item[doc[li + len(k)]] += 1
                else:
                    right_item[doc[li + len(k)]] = 1
            right = 0
            for _k, _v in right_item.items():
                right += abs((right_item[_k] / len_right_list) * math.log(1 / len(right_item)))
            self.words[k]['right_free'] = right

    def get_df(self):
        df = pd.DataFrame(self.words)
        df = df.T
        df['score'] = df['dop'] + df['left_free'] + df['right_free']
        df = df.sort_values(by='score', ascending=False)
        df = df[df['score'] > self.radio]
        df = df[df['freq'] > self.freq]
        df = df[df['dop'] > self.dop_base]
        df = df[df['left_free'] > self.left_free_base]
        df = df[df['right_free'] > self.right_free_base]
        df = df[df['left_free'] < self.left_right_diff * df['right_free']]
        return df

    def run(self, doc):
        # doc = re.sub('[,，.。"“”‘’\';；:：、？?！（）!\n\[\]\(\)\\/a-zA-Z0-9]', '', doc)
        doc = ''.join(re.findall(r'[\u4e00-\u9fa5]', doc))  # only take chinese into consider
        self.find_words(doc)
        self.dop()
        self.left_free(doc)
        self.right_free(doc)
        df = self.get_df()
        return df


# get all information from twse
def get_twse_info_df(key_stock_info):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    # ua=UserAgent()
    # chrome_options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0')
    chrome_options.add_argument('UserAgent().firefox')
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.get('https://mops.twse.com.tw/mops/web/t05sr01_1')  # 即時重大訊息頁面
    browser.find_element_by_xpath("//form[input/@value='前一日資料（市場別：全體公司）']").click()  # 前一日重大消息
    # browser.find_element_by_xpath("//*[@id='table01']/form[3]/table/tbody/tr/td[2]/input").click() #前一日資料
    # time.sleep(10)
    # browser.find_element_by_xpath("//*[@id='table01']/form[3]/table/tbody/tr/td[2]/input").click() #前一日資料
    soup = BeautifulSoup(browser.page_source, 'html.parser')
    announce_dt = []
    announce_tm = []
    actual_dt = []
    stock_id = []
    comp_nm = []
    title = []
    description = []
    law_type = []
    # non_track_list=[]
    for i in range(len([i.get('value') for i in
                        soup.find_all('input', attrs={'name': re.compile(r'\bh\d+1\b')}, type='hidden')])):
        if [i.get('value') for i in soup.find_all('input', attrs={'name': re.compile(r'\bh\d+1\b')}, type='hidden')][
            i] in list(key_stock_info['stock_id']):
            announce_dt.append([datetime.datetime.strptime(i.get('value'), '%Y%m%d').date() for i in
                                soup.find_all('input', attrs={'name': re.compile(r'\bh\d+2\b')}, type='hidden')][i])
            announce_tm.append([datetime.datetime.strptime(i.get('value'), '%H%M%S').time() for i in
                                soup.find_all('input', attrs={'name': re.compile(r'\bh\d+3\b')}, type='hidden')][i])
            actual_dt.append([datetime.datetime.strptime(i.get('value'), '%Y%m%d').date() for i in
                              soup.find_all('input', attrs={'name': re.compile(r'\bh\d+7\b')}, type='hidden')][i])
            stock_id.append(int([i.get('value') for i in
                                 soup.find_all('input', attrs={'name': re.compile(r'\bh\d+1\b')}, type='hidden')][i]))
            comp_nm.append([i.get('value') for i in
                            soup.find_all('input', attrs={'name': re.compile(r'\bh\d+0\b')}, type='hidden')][i])
            title.append([i.get('value').replace('\n', '').replace('&#21173', '券') for i in
                          soup.find_all('input', attrs={'name': re.compile(r'\bh\d+4\b')}, type='hidden')][i])
            description.append([i.get('value') for i in
                                soup.find_all('input', attrs={'name': re.compile(r'\bh\d+8\b')}, type='hidden')][i])
            law_type.append([i.get('value') for i in
                             soup.find_all('input', attrs={'name': re.compile(r'\bh\d+5\b')}, type='hidden')][i])

    browser.quit()
    twse_info_df = pd.DataFrame({
        'announce_dt': announce_dt,
        'announce_tm': announce_tm,
        # 'announce_dttm':''.join(announce_dt,announce_tm)
        'actual_dt': actual_dt,
        'stock_id': stock_id,
        'comp_nm': comp_nm,
        'title': title,
        'description': description,
        'law_type': law_type,
        'updt_dt': datetime.datetime.now().strftime('%Y/%m/%d')
        # 昨日(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    })
    regex = r'[1-9]\d?\.{1}[^:：%$；。.]+:|[1-9]\d?\.{1}[^：：%$；。.]+：'
    twse_info_df['description_title_list'] = twse_info_df['description'].apply(
        lambda x: [i.replace('\n', '') for i in re.findall(regex, x.replace(' ', ''))])
    # twse_info_df['twse_info_num']=twse_info_df['description_title_list'].apply(lambda x: [int(re.findall(r'\d+',i)[0]) for i in x])
    # twse_info_df['title_seq']=twse_info_df['twse_info_num'].apply(lambda x :get_actual_sequence(x))

    twse_info_df['description_a'] = twse_info_df['description'].apply(lambda x: [i.replace('\n', '') if re.findall(
        '：|:|\(1\)|\(一\)|\(a\)', i) == [] else i.replace('\t', '').replace('\u3000', '').lstrip('\n') for i in
                                                                                 re.split(regex, x.replace(' ', ''))][
                                                                                1:])

    a = twse_info_df.apply(lambda x: pd.Series(x['description_a']), axis=1).stack().reset_index(level=1, drop=True)
    a.name = 'description_a'
    desc_df = twse_info_df.drop('description_a', axis=1).join(a)
    desc_df = desc_df[['announce_dt', 'stock_id', 'comp_nm', 'title', 'description_a']]
    q = pd.DataFrame(
        twse_info_df.apply(lambda x: pd.Series(x['description_title_list']), axis=1).stack().reset_index(level=1,
                                                                                                         drop=True)).rename(
        columns={0: 'description_title'})
    desc_df = pd.concat([desc_df, q], axis=1)
    desc_df['q_order'] = desc_df['description_title'].apply(lambda x: [int(i) for i in re.findall(r'\d+', x)][0])
    desc_df['description_title_a'] = desc_df['description_title'].apply(
        lambda x: re.split(r'\d+\.', x)[1].replace(':', '').replace('：', ''))

    # no_announcement_comp=key_stock_info[twse_info_df.merge(key_stock_info,left_on = 'stock_id',right_on = 'stock_id',how='right')['stock_id'].isnull()]

    return twse_info_df, desc_df


# defined category group
def get_group(twse_info_df):
    description_title = pd.DataFrame()

    for i in range(len(twse_info_df['description_title_list'])):
        df1 = pd.DataFrame(pd.get_dummies(twse_info_df['description_title_list'][i]).sum(axis=0)).rename(columns={0: i})
        description_title = pd.concat([description_title, df1], axis=1).fillna(0)
    description_title = description_title.T

    cos_sim_matrix = np.zeros((description_title.shape[0], description_title.shape[0]))
    for i in range(0, description_title.shape[0]):
        for j in range(0, description_title.shape[0]):
            if i != j:
                dot = sum(description_title.iloc[i] * description_title.iloc[j])
                norm_a = sum(description_title.iloc[i] * description_title.iloc[i]) ** 0.5
                norm_b = sum(description_title.iloc[j] * description_title.iloc[j]) ** 0.5
                cos_sim_matrix[i, j] = float(dot / (norm_a * norm_b))
    cos_sim_df = pd.DataFrame(cos_sim_matrix)
    group = dict()
    # group['股東會召開']=description_title[description_title['2.股東會召開日期:']==1].index.to_list()
    # group['傳播媒體報導']=description_title[description_title['6.報導內容:']==1].index.to_list()
    for i in range(len(cos_sim_df)):
        if len([key for key, list_of_values in group.items() for t in
                cos_sim_df[cos_sim_df.iloc[i] > 0.1].index.to_list() if t in list_of_values]) == 0:
            group[i] = cos_sim_df[cos_sim_df.iloc[i] > 0.3].index.to_list()
            group[i].append(i)
        if len([key for key, list_of_values in group.items() for t in
                cos_sim_df[cos_sim_df.iloc[i] > 0.1].index.to_list() if t in list_of_values]) > 0 and len(
                [key for key, list_of_values in group.items() for t in
                 cos_sim_df[cos_sim_df.iloc[i] > 0.1].index.to_list() if t in list_of_values]) != 0 and [key for
                                                                                                         key, list_of_values
                                                                                                         in
                                                                                                         group.items()
                                                                                                         if
                                                                                                         i in list_of_values] == []:
            group[[key for key, list_of_values in group.items() for t in
                   cos_sim_df[cos_sim_df.iloc[i] > 0.1].index.to_list() if t in list_of_values][0]].append(i)
        else:
            pass
    twse_info_df = twse_info_df.reset_index()
    twse_info_df['category'] = twse_info_df['index'].apply(
        lambda x: [str(key) for key, list_of_values in group.items() if x in list_of_values][0])
    return cos_sim_df, twse_info_df


# defined category tags
def cat_nm_fun(twse_info_df):
    # loadoriginal stop words
    stop_words = [line.strip() for line in open('D:/04144_Profile/Desktop/crawl/stopwords.txt').readlines()]
    # create user dict
    doc = ''.join(twse_info_df['title'])
    nw = NewWord(max_len_word=5, radio=6, freq=1, dop_base=0, left_free_base=0, right_free_base=0, left_right_diff=2)
    df = nw.run(doc)
    user_dict_newwords = df.index.tolist()
    new_stop_words = df[df['freq'] / len(twse_info_df) > 0.5].index.tolist()  # over 50% tile got this words
    jieba.load_userdict(user_dict_newwords)
    cat_dict = dict()
    for i in range(len(twse_info_df)):
        if twse_info_df['category'][i] in cat_dict:
            cat_dict[twse_info_df['category'][i]].append([twse_info_df['title'][i]])
        else:
            cat_dict[twse_info_df['category'][i]] = [twse_info_df['title'][i]]
    cat_ref_df = pd.DataFrame(cat_dict.items(), columns=['category', 'text'])
    category_tag = []
    for key in cat_dict.keys():
        # print(key)
        title_words = ''.join([''.join(i) for i in cat_dict[key]])
        tags = dict()
        for w in jieba.cut(title_words):
            if w not in stop_words and bool(re.match(r'[\u4e00-\u9fa5]', w)) and w not in new_stop_words and len(w) > 1:
                if w not in tags:
                    tags[w] = 1
                else:
                    tags[w] += 1
                temp_df = pd.DataFrame(tags.items(), columns=['word', 'tlt_cnt'])
                tag_l = [temp_df.iloc[i]['word'] for i in temp_df['tlt_cnt'].argsort()[::-1][0:3].tolist()]
        category_tag.append('/'.join(tag_l))
    cat_ref_df['tags'] = category_tag
    return cat_ref_df


# all data setup write into mssql
def df_org(twse_info_df, desc_df, key_stock_info, cat_ref_df):
    twse_info_df = twse_info_df.merge(cat_ref_df, on='category', how='inner')
    twse_info_df = twse_info_df[
        ['announce_dt', 'stock_id', 'comp_nm', 'title', 'actual_dt', 'updt_dt', 'category', 'tags']]
    desc_df = desc_df[
        ['announce_dt', 'stock_id', 'comp_nm', 'title', 'description_title_a', 'description_a', 'q_order']]
    twse_info_df['announce_dt'] = pd.to_datetime(twse_info_df['announce_dt'])
    twse_info_df['actual_dt'] = pd.to_datetime(twse_info_df['actual_dt'])
    twse_info_df['updt_dt'] = pd.to_datetime(twse_info_df['updt_dt'])
    desc_df['announce_dt'] = pd.to_datetime(desc_df['announce_dt'])

    start_time_twse_info_df = time.time()
    print(start_time_twse_info_df)
    # sql connect
    server = '10.129.7.155'
    db = 'db'
    uid = 'uid'
    pwd = 'pwd'
    conn = pyodbc.connect(
        'DRIVER={SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (server, db, uid, pwd))
    cursor = conn.cursor()
    for index, row in twse_info_df.iterrows():
        try:
            cursor.execute(
                "INSERT INTO dbo.TWSE_INFO([announce_dt],[stock_Id],[comp_Nm],[title],[actual_dt],[updt_dt],[category],[tags])" "values (?,?,?,?,?,?,?,?)",
                (row['announce_dt'],
                 row['stock_id'],
                 row['comp_nm'],
                 row['title'],
                 row['actual_dt'],
                 row['updt_dt'],
                 row['category'],
                 row['tags'])
                )
            conn.commit()
        except:
            print(row)
    # see total time to do insert
    print("%s seconds ---" % (time.time() - start_time_twse_info_df))
    start_time_desc_df = time.time()
    for index, row in desc_df.iterrows():
        try:
            cursor.execute(
                "INSERT INTO dbo.TWSE_INFO_DETAIL([announce_dt],[stock_id],[comp_nm],[title],[title_order],[content_title],[content_text])" "VALUES(?,?,?,?,?,?,?)",
                (row['announce_dt'],
                 row['stock_id'],
                 row['comp_nm'],
                 row['title'],
                 row['q_order'],
                 row['description_title_a'],
                 row['description_a'])
                )
            conn.commit()
        except:
            print(row)

    # see total time to do insert
    print("%s seconds ---" % (time.time() - start_time_desc_df))
    cursor.close()
    conn.close()

    return twse_info_df, desc_df


# command_path = pd.read_csv('D:/04144_Profile/Desktop/crawl/project01_twse/command_path.txt', sep="=",header=None).set_index(0)
def get_sql_info():
    try:
        server = '10.129.7.155'
        db = 'db'
        uid = 'uid'
        pwd = 'pwd'
        # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' %(command_path.loc['server'], command_path.loc['db'], command_path.loc['uid'], command_path.loc['pwd']))
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
            server, db, uid, pwd))
        cursor = conn.cursor()
        query_command = '''
        select * from [TOPCO_WARROOM].[dbo].[TWSE_STOCK_REF]
        '''
        cursor.execute(query_command)
        conn.commit()
        df = pd.read_sql(query_command, conn)
        cursor.close()
        conn.close()
    except:
        print('No list be found')
    return df


if __name__ == '__main__':
    key_stock_info = get_sql_info()
    # key_stock_info=get_stock_info(stock_list) #run if stock_list chage
    twse_info_df, desc_df = get_twse_info_df(key_stock_info)
    if len(twse_info_df) == 0:
        recipient_list = ['chelsie.chang@topco-global.com']
        # ,'john.chao@topco-global.com','yiching.chen@topco-global.com']
        subject = '關注客戶今日(' + datetime.datetime.now().strftime('%Y/%m/%d') + ')無新增的重大訊息'
        head = '公開資訊觀測站訊息'
        body = """\
        今日{date}無新增的重大訊息

        查看他日報表內容請點選下方連結：
        https://app.powerbi.com/groups/5d3a5b9d-c75b-4033-b9df-5b711a47ef75/list
        """.format(date=datetime.datetime.now().strftime('%Y/%m/%d'))
        mail_send_by_smtp(recipient_list=recipient_list, sender='chelsie.chang@topco-global.com', subject=subject,
                          head=head, body=body)

        # mail_send_by_smtp(recipient_list=recipient_list,sender='chelsie.chang@topco-global.com',subject=subject,head = head)
        print('No announcement for today')
        pass
    else:
        cos_sim_df, twse_info_df = get_group(twse_info_df)
        cat_ref_df = cat_nm_fun(twse_info_df)
        twse_info_df, desc_df = df_org(twse_info_df, desc_df, key_stock_info, cat_ref_df)
        print('There is ' + str(len(twse_info_df)) + ' announcement for today')

# twse_info_df.to_csv('D:/04144_Profile/Desktop/crawl/project01_twse/package_data_test/twse_info_df.csv',encoding='utf_8_sig')
# desc_df.to_csv('D:/04144_Profile/Desktop/crawl/project01_twse/package_data_test/desc_df.csv',encoding='utf_8_sig')