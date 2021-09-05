from fake_useragent import UserAgent
import pandas as pd
import time
import datetime
import random
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import numpy as np
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
import jieba
import jieba.posseg as pseg
import math
import pyodbc
import requests
import Public_Tool
import sys
import traceback


# news_keyword_list=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/keywords.csv')
# news_keyword_list['words'][23]='奕斯伟'
# news_keyword_list=news_keyword_list.reset_index().rename(columns={0:'Index'})
# tmp_q=random.sample(news_keyword_list['words'].to_list(),30)
# news_keyword_list.to_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/news_keyword_list.csv',encoding='utf-8-sig')


# query=['台積電','記憶體','英特爾']
# source=['Digitimes','工商時報','經濟日報','科技新報']


# News Source
def get_news_url_list(query, source):
    # create a list of the values we want to assign for each condition
    start_time = time.time()
    source_list = []
    # keyword_list=[]
    urls_list = []
    block_typ = []
    for s in source:
        # print(s)
        if s == 'Digitimes':
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                'Cookie': 'vid=d92c229453eb0825; _ga=GA1.3.1213608194.1619578236; new_a1=new_a1; _gcl_au=1.1.1695377058.1624518223; DT%5Fmemip%5Fnew=Y; ASPSESSIONIDAQDDBQQT=NGDHIPNALLHDEJIOCADHGHCN; _gid=GA1.3.678270841.1625452334; occupation=30; promotype=N; MyPwd=BU9YR7F8EXsAMOYpPXAZIo6VEQAJdOCEN8DHn6OB278SG1SHDYSASHA309C0Y7G7312JIYFOBWLBAL1Z53K5JECX47J; MyName%5Fcn=; ASPSESSIONIDAUDDBQQT=JGGHIPNADFGDOLKMMOIPKBAC; mem%5Fpro=N; justLogout=Y; ASPSESSIONIDAQFDBSSR=CBMJKPNAOHDAMGLIBJBIKIOJ; sQuery4gb=%B1R%B6V; ASPSESSIONIDAWBBBSTR=NOMMBCOALDFBPFCIEMEBIDHK; ASPSESSIONIDAWBAARQS=GLKKOIEBNBFBENEANIEDHLOI; ASPSESSIONIDAUCBBTTR=HJIKOIEBFJHHACAEPPAKGCPN; ASPSESSIONIDCUBABRRS=KEFAPIEBCJBFGBMCMPKBJFDG; MemSID=344917230; sLgnTime=2021%2F7%2F5+%A4U%A4%C8+02%3A14%3A57; DownMYID=chelsie%2Echang%40topco%2Dglobal%2Ecom; MyID=BdRg9Yf3YMkEEA8t345Y5hCIZZGLfQ0SODML%2DD8M9UQHOdMGD3EFZX4gQV3ICZAM5DbVQCAQJ0DIGVm7ZBG2XBCXV1UhTGWP6TSBQA0WJ%3FFA842CE4YBTZAWuOZS8GITHRKXFDLGnANDKY1DDBW9HWNNLqU14MXQGM158KSJHSDbAU2SKOS52OGD5NAZ4Ep9GT79DI5M5WXN1GYT30%2CJQMKDSYGGP3JVM2VB57JhDZTOHI1TSR3HINJ1EEZPMkBYZKL22AODVYK1ZNP60BR4p53WN5S7Q3OHC3H4PJN8M5WBa3ZZP9OMRG7VWRIK3GR5N1IN2bAGY9R292BU0W0JPNDRB1MURZGkUL7TX5D3YP5K82SXP9N96MC400%2FS1SER0JIJGZZXE7GBSCBC8Q8FRBbDQ12GHSYJCEYG2RC5MHDQ6902OI3pILAKZQD47KN26MWAY8HNQEML23QYClO2XLKOLJ8EBRFFQCJTNIDFKR432ZIZ; MemRights=KPS; yUID=chelsie%2Echang%40topco%2Dglobal%2Ecom; MyName=%B1i%AEa%B7O; sShow=Y; MemberIDPDF=XAB02110; SSLUID=XAA22001; UserIDPDF=XAA22001; sPreLgnTime=2021%2F7%2F5+%A4W%A4%C8+10%3A57%3A21; NewMemRights=1%2C9%2C3%2C4%2C10%2C13%2C17%2C99; DownMemID=XAB02110; DownUID=XAA22001'
            }
            # 半導體/零組件
            session = requests.Session()
            response = session.get('https://www.digitimes.com.tw/tech/dt/SubChannel_1_40.asp?CnlID=1&cat=40',
                                   headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            urls_list.extend(
                ['https://www.digitimes.com.tw' + i.get('href') for i in soup.select('div.col-md-6.col-sm-12>div>a')])
            source_list.extend(
                ['Digitimes'] * len([i.get('href') for i in soup.select('div.col-md-6.col-sm-12>div>a')]))
            block_typ.extend(['Big'] * len([i.get('href') for i in soup.select('div.col-md-6.col-sm-12>div>a')]))
            urls_list.extend(['https://www.digitimes.com.tw' + i.get('href') for i in
                              soup.select('div.col-md-12.col-sm-12.col-xs-12>p>a')])
            source_list.extend(
                ['Digitimes'] * len([i.get('href') for i in soup.select('div.col-md-12.col-sm-12.col-xs-12>p>a')]))
            block_typ.extend(
                ['Big'] * len([i.get('href') for i in soup.select('div.col-md-12.col-sm-12.col-xs-12>p>a')]))
            # 新聞頭條
            session = requests.Session()
            response = session.get('https://www.digitimes.com.tw/tech/default.asp?', headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            urls_list.extend(['https://www.digitimes.com.tw' + i.get('href') for i in
                              soup.select('div.col-md-8.col-sm-8.col_left>a')])
            source_list.extend(
                ['Digitimes'] * len([i.get('href') for i in soup.select('div.col-md-8.col-sm-8.col_left>a')]))
            block_typ.extend(['Big'] * len([i.get('href') for i in soup.select('div.col-md-8.col-sm-8.col_left>a')]))
            # 崇越熱門
            session = requests.Session()
            response = session.get('https://www.digitimes.com.tw/' +
                                   [i.get('href') for i in soup.select('div.col-md-8.col-sm-8.col_left>a')][0],
                                   headers=headers)
            # response = session.get('https://www.digitimes.com.tw/tech/dt/n/shwnws.asp?id=0000616964_IPY8H7O67RUMX33EIS29Z&ct=a', headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            urls_list.extend(['https://www.digitimes.com.tw' + i.get('href') for i in
                              soup.select('div.col-md-12.m-b-20.txt-16>div#pop_hot>ul>li>a')])
            source_list.extend(['Digitimes'] * len(['https://www.digitimes.com.tw' + i.get('href') for i in
                                                    soup.select('div.col-md-12.m-b-20.txt-16>div#pop_hot>ul>li>a')]))
            block_typ.extend(['TSC'] * len(['https://www.digitimes.com.tw' + i.get('href') for i in
                                            soup.select('div.col-md-12.m-b-20.txt-16>div#pop_hot>ul>li>a')]))
        # elif s=='科技新報':
        #   session = requests.Session()
        #   headers={'user-Agent':UserAgent().chrome}
        #   response = session.get('https://technews.tw/', headers=headers)
        #   soup=BeautifulSoup(response.text,'html.parser')
        #   for i,tag in enumerate(soup.select('li.block2014>div.cat01')):
        #       if  tag.get_text() in ['晶片','晶圓']:
        #           urls_list.extend([i.get('href') for i in soup.select('li.block2014')[i].find_all('a')])
        #           source_list.extend(['科技新報']*len([i.get('href') for i in soup.select('li.block2014')[0].find_all('a')]))
        #           block_typ.extend(['']*len([i.get('href') for i in soup.select('li.block2014')[0].find_all('a')]))
        #       else:
        #            pass
        #   chip_request=requests.get('https://technews.tw/category/component/chip/',headers=headers)
        #   chip_soup= BeautifulSoup(chip_request.text,'html.parser')
        #   time_tmp=[datetime.datetime.strptime(re.findall(r'\d{4}.+\d{2}.+\d{2}',i.get_text().split('日')[0])[0], "%Y 年 %m 月 %d").date() for i in chip_soup.select('span.body') if re.findall(r'\d{4}.+\d{2}.+\d{2}',i.get_text())!=[]]
        #   for i,t in enumerate(time_tmp):
        #       if t>=datetime.datetime.now().date()-datetime.timedelta(days=1):
        #           urls_list.append([i.get('href') for i in chip_soup.select('h1.entry-title>a')][i])
        #           source_list.append('科技新報')
        #           block_typ.append('')
        else:
            pass
        for q in range(len(query)):
            # print(q)
            time.sleep(random.randint(15, 25))
            try:
                if s == '工商時報':
                    options = Options()
                    options.add_argument('--headless')  # --incognito #--headless
                    options.add_argument(UserAgent().chrome)
                    browser = webdriver.Chrome(options=options)
                    browser.get('https://ctee.com.tw/fulltextsearch')
                    search_input = browser.find_element_by_xpath('//*[@id="kw"]')
                    search_input.send_keys(query[q])
                    # search_input.send_keys('台積電')
                    browser.find_element_by_xpath('//*[@id="btnAjax"]').click()
                    time.sleep(2)
                    soup = BeautifulSoup(browser.page_source, 'html.parser')
                    news_time = ['2021/' + i.get_text().lstrip() for i in
                                 soup.find('div', class_='wpb_wrapper').select('time.post-published.updated')]
                    correct_url = []
                    for t in range(len(news_time)):
                        # print(datetime.datetime.strptime(re.findall(r'\d{2}\/\d{2}',news_time[t])[0],'%m/%d').strftime('%m/%d'))
                        if re.findall(r'\d{4}\/\d{2}\/\d{2}', news_time[t])[0] == datetime.datetime.strftime(
                                datetime.date.today() - datetime.timedelta(days=1), '%Y/%m/%d') or \
                                re.findall(r'\d{4}\/\d{2}\/\d{2}', news_time[t])[0] == datetime.datetime.strftime(
                                datetime.date.today(), '%Y/%m/%d'):
                            correct_url.append([i.get('href') for i in soup.select('h2.title>a')][t])
                            # print(len(correct_url))
                    regex = re.compile(r'\S+./share$')
                    tmp_len = [x for x in correct_url if not regex.match(x)]
                    urls_list.extend(tmp_len)
                    source_list.extend(['工商時報'] * len(tmp_len))
                    block_typ.extend([''] * len(tmp_len))
                    # keyword_list.extend([q]*len(tmp_len))
                    browser.quit()

                    # elif s=='科技新報':
                #    options = Options()
                #    options.add_argument('--headless')#--incognito #--headless
                #    options.add_argument(UserAgent().chrome)
                #    browser = webdriver.Chrome(options=options)
                #    browser.get('https://technews.tw/google-search/?googlekeyword={}'.format(query[q])) #科技新報
                #    #browser.get('https://technews.tw/google-search/?googlekeyword=%E7%A0%B7%E5%8C%96%E9%8E%B5')
                #    time.sleep(5)
                #    browser.find_element_by_xpath('//*[@id="___gcse_0"]/div/div/div/div[3]/table/tbody/tr/td[2]/div/div[2]').click()
                #    browser.find_element_by_xpath('//*[@id="___gcse_0"]/div/div/div/div[3]/table/tbody/tr/td[2]/div/div[2]/div[2]/div[2]/div').click()
                #    time.sleep(3)
                #    soup = BeautifulSoup(browser.page_source, 'html.parser')
                #    #browser.get('https://technews.tw/google-search/?googlekeyword=%E5%8F%B0%E7%A9%8D%E9%9B%BB')
                #    tmp_time=[re.findall(r'^\d+\s小時前|^\d+\s分鐘前',i.get_text())[0] if re.findall(r'^\d+\s小時前|^\d+\s分鐘前',i.get_text())!=[] else '' for i in soup.select('div.gs-bidi-start-align.gs-snippet')][0:10]
                #    page=1
                #    while True:
                #        correct_url=[]
                #        regex_web = re.compile(r'\S+.technews.tw/$')
                #        regex_finance = re.compile(r'^https://finance.technews.tw/')
                #        regex_author = re.compile(r'\S+.technews.tw/[A-Za-z]+')
                #        #time.sleep(random.randint(2,5))
                #        if any(len(i)>0 for i in tmp_time):
                #            page+=1
                #            #print(page)
                #            urls_tmp0=[]
                #            for i in range(len(tmp_time)):
                #                if tmp_time[i]!='':
                #                    urls_tmp0.append([url.get('href') for url in soup.select('div.gsc-thumbnail-inside>div.gs-title>a.gs-title')][i])
                #            urls_tmp1 = list(filter(None.__ne__, urls_tmp0))
                #            correct_url.extend([x for x in urls_tmp1 if not regex_web.match(x) and not regex_finance.match(x) and not regex_author.match(x) and x not in urls_list])
                #            #urls_list.extend(correct_url)#remove .technews.tw
                #            # more news check
                #            browser.find_element_by_xpath('//*[@id="___gcse_0"]/div/div/div/div[5]/div[2]/div/div/div[2]/div/div[{}]'.format(page)).click()
                #            soup = BeautifulSoup(browser.page_source, 'html.parser')
                #            tmp_time=[re.findall(r'^\d+\s小時前|^\d+\s分鐘前',i.get_text())[0] if re.findall(r'^\d+\s小時前|^\d+\s分鐘前',i.get_text())!=[] else '' for i in soup.select('div.gs-bidi-start-align.gs-snippet')][0:10]
                #            time.sleep(2)
                #        else:
                #            urls_tmp0=[]
                #            for i in range(len(tmp_time)):
                #                if tmp_time[i]!='':
                #                    urls_tmp0.append([url.get('href') for url in soup.select('div.gsc-thumbnail-inside>div.gs-title>a.gs-title')][i])
                #            urls_tmp1 = list(filter(None.__ne__, urls_tmp0))
                #            correct_url.extend([x for x in urls_tmp1 if not regex_web.match(x) and not regex_finance.match(x) and not regex_author.match(x)  and x not in urls_list])#remove .technews.tw
                #            #urls_list.extend(correct_url)
                #            break
                #    urls_list.extend(correct_url)
                #    source_list.extend(['科技新報']*len(correct_url))
                #    block_typ.extend(['']*len(correct_url))
                #    #keyword_list.extend([q]*len(correct_url))
                #    browser.quit()

                elif s == '經濟日報':
                    headers = {
                        'content-type': 'text/html; charset=UTF-8',
                        'user-agent': UserAgent().chrome
                    }
                    response = requests.get('https://money.udn.com/search/result/1001/{}'.format(query[q]),
                                            headers=headers)
                    # browser.get('https://money.udn.com/search/result/1001/{}'.format(query[q]))
                    # response=requests.get('https://money.udn.com/search/result/1001/%E6%A2%AD%E6%84%8F%E7%A7%91',headers=headers)
                    # webpage=requests.get('https://money.udn.com/search/result/1001/{}'.format('梭意科'))
                    soup = BeautifulSoup(response.text, 'html.parser')
                    if '共找到 0筆' in [i.get_text() for i in soup.select('div#search_info')][0]:
                        pass
                    else:
                        last_item_time = datetime.datetime.strptime(
                            [re.findall(r'\d{4}\/\d{2}\/\d{2}', i.get_text()) for i in soup.select('span.cat')][-1][0],
                            '%Y/%m/%d')
                        time_tmp = [re.findall(r'\d{4}\/\d{2}\/\d{2}', i.get_text())[0] for i in
                                    soup.select('span.cat')]
                        new_updt_tm = datetime.datetime.today() - datetime.timedelta(days=1)
                        page = 1
                        correct_url = []
                        while True:
                            if last_item_time >= new_updt_tm:
                                page += 1
                                time.sleep(1)
                                # js="var q=document.documentElement.scrollTop=4000"
                                # browser.execute_script(js)
                                # time.sleep(1)
                                response = requests.get(
                                    'https://money.udn.com/search/result/1001/{}/{}'.format(query[q], page),
                                    headers=headers)
                                time.sleep(1)
                                # browser.find_element_by_xpath('//*[@id="result_list"]/div[4]/gopage/a[{}]'.format(page)).click()
                                soup = BeautifulSoup(response.text, 'html.parser')
                                urls_list.extend([i.get('href') for i in soup.select('div#search_content>dl>dt>a')])
                                last_item_time = datetime.datetime.strptime(
                                    [re.findall(r'\d{4}\/\d{2}\/\d{2}', i.get_text()) for i in soup.select('span.cat')][
                                        -1][0], '%Y/%m/%d')
                                time_tmp = [re.findall(r'\d{4}\/\d{2}\/\d{2}', i.get_text())[0] for i in
                                            soup.select('span.cat')]
                                correct_url.extend(time_tmp)
                            else:
                                for t in range(len(time_tmp)):
                                    if datetime.datetime.strptime(time_tmp[t], '%Y/%m/%d') >= new_updt_tm:
                                        correct_url.append(t)
                                        urls_list.append(
                                            [i.get('href') for i in soup.select('div#search_content>dl>dt>a')][t])
                                    else:
                                        pass
                                break
                        source_list.extend(['經濟日報'] * (len(correct_url)))
                        block_typ.extend([''] * (len(correct_url)))
                        browser.quit()
                        # keyword_list.extend([q]*(len(correct_url)))
                elif s == 'Digitimes':
                    options = Options()
                    options.add_argument('--headless')  # --incognito #--headless
                    options.add_argument(UserAgent().chrome)
                    browser = webdriver.Chrome(options=options)
                    browser.get('https://www.digitimes.com.tw/tech/searchdomain/srchlst_main.asp#SrchContent')
                    # browser.find_element_by_xpath('//*[@id="menu-item-189687"]/a/i').click()
                    browser.find_element_by_xpath('//*[@id="query_word"]').clear()
                    search_input = browser.find_element_by_xpath('//*[@id="query_word"]')
                    search_input.send_keys(query[q])
                    js = "var q=document.documentElement.scrollTop=29"
                    browser.execute_script(js)
                    time.sleep(1)
                    # search_input.send_keys('台積電')
                    browser.find_element_by_xpath(
                        '//*[@id="myform"]/div[1]/div[1]/div[2]/div[1]/div[1]/input[2]').click()
                    time.sleep(3)
                    soup = BeautifulSoup(browser.page_source, 'html.parser')
                    item_time = [i.get_text() for i in soup.select('div.col-md-2.col-sm-2.col-xs-2.hide-on-smartphone')]
                    new_updt_tm = datetime.datetime.today() - datetime.timedelta(days=1)
                    page = 0
                    cnt = 0
                    correct_url = []
                    while True:
                        if len(item_time) > 0 and datetime.datetime.strptime(item_time[-1], '%Y/%m/%d') >= new_updt_tm:
                            page += 1
                            correct_url = ['https://www.digitimes.com.tw/' + i.get('href').split('&query')[0] for i in
                                           soup.select('div.col-md-6.col-sm-9.col-xs-10>a')]
                            urls_list.extend([url for url in correct_url if url not in urls_list])
                            browser.find_element_by_xpath(
                                '//*[@id="tab_all_dt"]/table/tbody/tr/td/div/div[4]/div/div/a[{}]'.format(page)).click()
                            soup = BeautifulSoup(browser.page_source, 'html.parser')
                            last_item_time = \
                            [i.get_text() for i in soup.select('div.col-md-2.col-sm-2.col-xs-2.hide-on-smartphone')][-1]
                        else:
                            for t in range(len(item_time)):
                                tmp_url = ['https://www.digitimes.com.tw/' + i.get('href').split('&query')[0] for i in
                                           soup.select('div.col-md-6.col-sm-9.col-xs-10>a')]
                                if datetime.datetime.strptime(item_time[t], '%Y/%m/%d') >= new_updt_tm and tmp_url[
                                    t] not in urls_list:
                                    cnt += 1
                                    urls_list.append(tmp_url[t])
                            break
                    source_list.extend(['Digitimes'] * (len(correct_url) + cnt))
                    block_typ.extend([''] * (len(correct_url) + cnt))
                    # keyword_list.extend([q]*(len(correct_url)+cnt))
                    browser.quit()
            except Exception as e:
                print(s + '_' + str(q), e)

    print("%s seconds ---" % (time.time() - start_time))
    news_list = pd.DataFrame({
        'source': source_list,
        'block_typ': block_typ,
        'url': urls_list,
        'updt_dt': datetime.datetime.now().strftime('%Y/%m/%d')})

    return news_list


def news_content_f(news_list):
    # print(news_list['url'])
    # drop duplicate

    # options = webdriver.ChromeOptions()
    # options.add_argument('--headless')#--incognito #--headless
    # options.add_argument(UserAgent().chrome)
    # browser = webdriver.Chrome(options=options)
    headers = {
        'content-type': 'text/html; charset=UTF-8',
        'user-agent': UserAgent().chrome
    }
    # browser.get('https://readers.ctee.com.tw/cm/20210627/A09AA9/1132149/share')
    response = requests.get(news_list['url'], headers=headers)
    # response=requests.get('https://ctee.com.tw/news/tech/499620.html',headers=headers)
    # browser.get(news_list['url'])
    content_soup = BeautifulSoup(response.text, 'html.parser')
    time.sleep(random.randint(10, 15))
    # if 'https://m.ctee.com.tw/' in news_list['url']:
    # news_list['title']=''.join([i.get_text() for i in content_soup.select('h1.poput_txt_h1')])
    # sub_title=[i.get_text() for i in content_soup.select('h2.poput_txt_h2.p_bottom_15')]
    # real_content=[i.get_text() for i in content_soup.select('p.poput_div.poput_txt')]
    # news_list['content']=''.join(sub_title+real_content)
    # img_source=[i.get('src') for i in content_soup.select('img.p_sm_top_10.img-responsive.btn-block')]
    # news_list['img_source']='' if img_source==[] else img_source
    # news_list['tag']=''
    # news_list['published_dt']=[i.get_text() for i in content_soup.select('div.post-meta-date')]
    try:
        if 'ctee.com.tw/' in news_list['url'] and 'https://view.ctee.com.tw/' not in news_list['url']:
            news_list['title'] = ''.join([i.get_text() for i in content_soup.select('span.post-title')])
            sub_title = [i.get_text() for i in content_soup.select('h2.has-luminous-vivid-orange-color.has-text-color')]
            real_content = [i.get_text() + '\n' for i in
                            content_soup.select('div.entry-content.clearfix.single-post-content > p')]
            news_list['content'] = ''.join(sub_title + real_content).split('延伸閱讀')[
                0] if '延伸閱讀\n' in real_content else ''.join(sub_title + real_content)
            news_list['tag'] = '/'.join(
                [i.get_text() for i in content_soup.select('div.entry-terms.post-tags.clearfix.style-24>a')])
            news_list['published_dt'] = [i.get_text() for i in content_soup.select('div.post-meta-date')][
                0] if content_soup.select('div.post-meta-date') != [] else \
            [i.get_text() for i in content_soup.select('time.post-published.updated')][0]
            # img_source=[i.get('data-src') for i in content_soup.find('div',class_='single-featured').find_all('img')] if content_soup.find('div',class_='single-featured')!=None else []
            img_source = [i.get('src') if i.get('src') != None else i.get('data-src') for i in content_soup.select(
                'div.single-container>article>div>div>figure>img')] if content_soup.select(
                'div.single-container>article>div>div>figure>img') != None else []
            news_list['img_source'] = ','.join(img_source)

        elif 'https://technews.tw/' in news_list['url']:
            news_list['title'] = ''.join([i.get_text() for i in content_soup.select('h1.entry-title')])
            news_list['content'] = ''.join(
                [i.get_text() + '\n' if content_soup.find('div', class_='indent') != None else '' for i in
                 content_soup.find('div', class_='indent').find_all(['h3', 'p'])][:-1])
            news_list['tag'] = '/'.join([i.get_text() for i in content_soup.select('span.body>a')][1:-1])
            img_source = [i.get('src') for i in content_soup.select('div.entry-content>div>img')] if content_soup.find(
                'div', class_='bigg') != None else []
            news_list['img_source'] = '' if img_source == [] else ','.join(img_source)
            news_list['published_dt'] = [i.get_text() for i in content_soup.select('span.body')][1]

        elif 'https://money.udn.com/' in news_list['url']:
            news_list['title'] = ''.join([i.get_text() for i in content_soup.select('h2#story_art_title')])
            news_list['content'] = ''.join(
                ['' if i.get_text() == '\n' else i.get_text() for i in content_soup.select('div#article_body>p')])
            news_list['tag'] = '/'.join([i.get_text() for i in content_soup.select('div#story_tags>a')])
            news_list['published_dt'] = [i.get_text() for i in content_soup.select('div.shareBar__info--author>span')][
                0]
            news_list['img_source'] = ','.join(
                [i.get('src') for i in content_soup.select('div#article_body>p>figure>a>img')]) if content_soup.select(
                'div#article_body>p>figure>a>img') != None else ''

        elif 'https://www.digitimes.com.tw/' in news_list['url']:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                'Cookie': 'vid=d92c229453eb0825; _ga=GA1.3.1213608194.1619578236; new_a1=new_a1; _gcl_au=1.1.1695377058.1624518223; DT%5Fmemip%5Fnew=Y; ASPSESSIONIDAQDDBQQT=NGDHIPNALLHDEJIOCADHGHCN; _gid=GA1.3.678270841.1625452334; occupation=30; promotype=N; MyPwd=BU9YR7F8EXsAMOYpPXAZIo6VEQAJdOCEN8DHn6OB278SG1SHDYSASHA309C0Y7G7312JIYFOBWLBAL1Z53K5JECX47J; MyName%5Fcn=; ASPSESSIONIDAUDDBQQT=JGGHIPNADFGDOLKMMOIPKBAC; mem%5Fpro=N; justLogout=Y; ASPSESSIONIDAQFDBSSR=CBMJKPNAOHDAMGLIBJBIKIOJ; sQuery4gb=%B1R%B6V; ASPSESSIONIDAWBBBSTR=NOMMBCOALDFBPFCIEMEBIDHK; ASPSESSIONIDAWBAARQS=GLKKOIEBNBFBENEANIEDHLOI; ASPSESSIONIDAUCBBTTR=HJIKOIEBFJHHACAEPPAKGCPN; ASPSESSIONIDCUBABRRS=KEFAPIEBCJBFGBMCMPKBJFDG; MemSID=344917230; sLgnTime=2021%2F7%2F5+%A4U%A4%C8+02%3A14%3A57; DownMYID=chelsie%2Echang%40topco%2Dglobal%2Ecom; MyID=BdRg9Yf3YMkEEA8t345Y5hCIZZGLfQ0SODML%2DD8M9UQHOdMGD3EFZX4gQV3ICZAM5DbVQCAQJ0DIGVm7ZBG2XBCXV1UhTGWP6TSBQA0WJ%3FFA842CE4YBTZAWuOZS8GITHRKXFDLGnANDKY1DDBW9HWNNLqU14MXQGM158KSJHSDbAU2SKOS52OGD5NAZ4Ep9GT79DI5M5WXN1GYT30%2CJQMKDSYGGP3JVM2VB57JhDZTOHI1TSR3HINJ1EEZPMkBYZKL22AODVYK1ZNP60BR4p53WN5S7Q3OHC3H4PJN8M5WBa3ZZP9OMRG7VWRIK3GR5N1IN2bAGY9R292BU0W0JPNDRB1MURZGkUL7TX5D3YP5K82SXP9N96MC400%2FS1SER0JIJGZZXE7GBSCBC8Q8FRBbDQ12GHSYJCEYG2RC5MHDQ6902OI3pILAKZQD47KN26MWAY8HNQEML23QYClO2XLKOLJ8EBRFFQCJTNIDFKR432ZIZ; MemRights=KPS; yUID=chelsie%2Echang%40topco%2Dglobal%2Ecom; MyName=%B1i%AEa%B7O; sShow=Y; MemberIDPDF=XAB02110; SSLUID=XAA22001; UserIDPDF=XAA22001; sPreLgnTime=2021%2F7%2F5+%A4W%A4%C8+10%3A57%3A21; NewMemRights=1%2C9%2C3%2C4%2C10%2C13%2C17%2C99; DownMemID=XAB02110; DownUID=XAA22001'
            }
            response = requests.get(news_list['url'], headers=headers)
            time.sleep(random.randint(15, 20))
            # response=requests.get('https://www.digitimes.com.tw/tech/dt/n/shwnws.asp?id=0000616595_9O06OD1Y8T8W7T64U1QHO&ct=a',headers=headers)
            content_soup = BeautifulSoup(response.text, 'html.parser')
            time.sleep(random.randint(3, 5))
            titlea = [i.get_text().replace('\u3000', '') for i in content_soup.select('p.txt-blue2.txt-bold.m-b-10')]
            titleb = [i.get_text() for i in content_soup.select('p.article_header')]
            if titlea != []:
                news_list['title'] = ''.join(
                    [i.get_text().replace('\u3000', '') for i in content_soup.select('p.txt-blue2.txt-bold.m-b-10')])
            else:
                news_list['title'] = ''.join(titleb)
            news_list['content'] = ''.join([i.get_text().split('點擊圖片放大觀看')[0].strip().replace('\t',
                                                                                              '') + '\n' if '點擊圖片放大觀看' in i.get_text() else i.get_text().replace(
                '\t', '') + '\n' for i in content_soup.select('p.main_p')])
            news_list['tag'] = '/'.join([i.get_text() for i in content_soup.select('table#keyword>tbody>tr>td>a')])
            news_list['published_dt'] = [i.get_text() for i in content_soup.select('time')][0]
            # if content_soup.find('p',class_='img_half')!=None:
            news_list['img_source'] = ','.join([i.get('src') for i in content_soup.select('a.fancybox2>img')])
            # elif content_soup.select('div.img_full>a>img')!=None:
            #   news_list['img_source']=[i.get('src') for i in content_soup.select('div.img_full>a>img')][0]
            # else:
            # news_list['img_source']=''
        else:
            print(news_list['url'])
    except Exception as e:
        print('error:' + news_list['url'], e)
    return news_list


# news_list=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_list_log.csv')
# news_content=news_list[0:5].apply(lambda x: news_content_f(x),axis=1)


class NewWord(object):
    def __init__(self, max_len_word, radio, freq, dop_base, left_free_base, right_free_base):
        self.max_len_word = max_len_word
        self.radio = radio
        self.words = {}
        self.freq = freq
        self.dop_base = dop_base
        self.left_free_base = left_free_base
        self.right_free_base = right_free_base

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


def cosine_similarity_matrix(df):
    cos_sim_matrix = np.zeros((df.shape[1], df.shape[1]))
    for i in range(0, df.shape[1]):
        for j in range(0, df.shape[1]):
            if i != j:
                dot = sum(df[i] * df[j])
                norm_a = sum(df[i] * df[i]) ** 0.5
                norm_b = sum(df[j] * df[j]) ** 0.5
                if norm_a == 0 or norm_b == 0:
                    0
                else:
                    cos_sim_matrix[i, j] = float(dot / (norm_a * norm_b))
    cos_sim_df = pd.DataFrame(cos_sim_matrix)
    return cos_sim_df


def similarity_filter(news_content, news_keyword_list, source):
    news_content1 = news_content.dropna(subset=['title', 'content'])
    # preoprocessing
    news_content2 = news_content1.reset_index()
    news_content2['content'] = news_content2['content'].apply(
        lambda x: re.sub(u"\（.*?\）|\{.*?\}|\[.*?\]|\【.*?\】|\［.*?\］", "", x))
    news_content2['published_dt'] = news_content2['published_dt'].apply(
        lambda x: re.findall(r'\d{4}\.\d{2}\.\d{2}', re.sub('年|月|日|-', '.', x).replace(' ', ''))[0])
    # text cut
    stop_words = [line.strip() for line in open('D:/04144_Profile/Desktop/crawl/stopwords.txt').readlines()]
    new_stop_words = ['不過', '由於', '除了', '已經', '尤其', '非常', '不斷', '則是', '準備', '進行', '表示', '參考', '方面', '仍有', '一個', '一度']
    new_stop_words = stop_words + new_stop_words
    doc = ''.join(news_content1['content'])
    for w in new_stop_words:
        if len(w) > 1 and bool(re.match(r'[\u4e00-\u9fa5]', w)):
            doc = doc.replace(w, '')

    nw = NewWord(max_len_word=5, radio=10.05, freq=2, dop_base=0, left_free_base=0, right_free_base=0)
    df = nw.run(doc)
    user_dict_newwords = df.index.tolist()
    user_dict_newwords.extend(news_keyword_list['words'].to_list())
    jieba.load_userdict(user_dict_newwords)
    content_corpus = []
    test_weight = pd.DataFrame()
    for s in source:
        df_tmp = news_content2[news_content2['source'] == s]
        if len(df_tmp) > 0:
            df_tmp = df_tmp.reset_index(drop=True)
            content_corpus = []
            for i in range(len(df_tmp)):
                # print(i)
                content_simplify = []
                for w in pseg.cut(df_tmp['content'][i]):
                    if w.word not in stop_words and w.word not in new_stop_words and len(w.word) > 1 and bool(
                            re.match(r'[\u4e00-\u9fa5|a-zA-Z]', w.word)) and (
                            w.word in user_dict_newwords or bool(re.match(r'[a-zA-Z]', w.word)) or w.flag in ['n', 'v',
                                                                                                              'eng']):
                        content_simplify.append(w.word)
                content_corpus.append(' '.join(content_simplify))
            # word vectorize
            vectorizer = CountVectorizer()
            transformer = TfidfTransformer()
            tfidf = transformer.fit_transform(
                vectorizer.fit_transform(content_corpus))  # 第一個fit_transform是計算tf-idf，第二個fit_transform是將文字轉為詞頻矩陣
            word = pd.DataFrame(vectorizer.get_feature_names()).rename(columns={0: 'word'})
            weight = pd.DataFrame(tfidf.toarray())
            weight = weight.join(df_tmp['index']).set_index('index')
            for col in range(len(word)):
                weight = weight.rename(columns={col: word['word'][col]})
            test_weight = pd.concat([test_weight, weight], axis=0)
        else:
            pass
    test_weight = test_weight.fillna(0)
    words_selected = pd.DataFrame()
    for col in test_weight.columns:
        words_selected[col] = test_weight[col].apply(lambda x: 1 if x > 0 else 0)
    words_percent = words_selected.sum(axis=0) / len(test_weight)  # calculate every words covered by how many news
    word_sel_weight = test_weight[words_percent[(words_percent > 0.03) & (words_percent < 0.97)].index]  # at least 10%
    words_selected = word_sel_weight.columns
    # 取/len(news_count)的字才納入關鍵字
    key_num = round(word_sel_weight.shape[1] / 10)
    sim_content_detect_df = pd.DataFrame()
    content_keyword = []
    for i in range(len(word_sel_weight)):
        content_keyword.append(list([w for w in
                                     word_sel_weight.iloc[i][word_sel_weight.iloc[i] > 0].sort_values()[::-1][
                                     :key_num].index]))  # 取前n個非0關鍵字
        top_50_similar = pd.DataFrame({
            'words': [w for w in
                      word_sel_weight.iloc[i][word_sel_weight.iloc[i] > 0].sort_values()[::-1][:key_num].index],
            i: 1}).set_index('words')
        sim_content_detect_df = sim_content_detect_df.join(top_50_similar, how='outer').fillna(0)
    cos_sim_df = cosine_similarity_matrix(sim_content_detect_df)
    # sim_check=pd.DataFrame(news_content2['title']).join(cos_sim_df)
    # remove>0.5simlilarity
    drop_row = []
    for i in range(len(cos_sim_df)):
        if len(cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5]) != 0 and news_content2['block_typ'][i] != '' and \
                [cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5].index[0]][0] > i:
            drop_row.extend(cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5].index.tolist())
        elif len(cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5]) > 1 and \
                [cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5].index[0]][0] > i and news_content2['block_typ'][i] == '':
            same_index = cos_sim_df.iloc[i][cos_sim_df.iloc[i] > 0.5].index.tolist()
            same_index.append(i)
            keep_row_index = np.argmax([len(news_content2['content'][i]) for i in same_index])
            same_index.pop(keep_row_index)
            drop_row.extend(same_index)

        else:
            pass
    # make key
    news_content_df = news_content2.drop(drop_row, axis=0).drop(['index'], axis=1).reset_index(drop=True)
    news_content_df = news_content_df.reset_index()
    news_content_df['news_key'] = news_content_df.apply(
        lambda x: str(x['index']) + datetime.datetime.now().strftime('%Y%m%d%H%M%S'), axis=1)

    print('drop similar data raw!')
    return news_content_df


# news_content=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_content_log.csv')

def content_tag(news_content_df, news_keyword_list):
    # keywords in tile and content
    keyword_df = pd.DataFrame()
    kw_tag = []
    kw_tag_key = []
    user_dict = news_keyword_list['words']
    for i in news_content_df.index:
        content = ''.join(news_content_df['title'][i] + news_content_df['content'][i])
        content_simplify_tag = []
        news_key_tmp = []
        if any(kw in content for kw in user_dict):
            # kw_tag=pd.concat([kw_tag,pd.DataFrame(['/'.join([kw for kw in user_dict if kw in content])])],axis=0)
            kw_tag.extend(['/'.join([kw for kw in user_dict if kw in content])])
            kw_tag_key.append(news_content_df['news_key'][i])
            # print([kw for kw in user_dict if kw in content])
            content_simplify_tag.extend([kw for kw in user_dict if kw in content])
            news_key_tmp.extend([news_content_df['news_key'][i]] * len(content_simplify_tag))
            keyword_dftmp = pd.DataFrame({
                'keyword_tag': content_simplify_tag,
                'news_key': news_key_tmp})
        else:
            keyword_dftmp = pd.DataFrame()
            pass
        keyword_df = pd.concat([keyword_df, keyword_dftmp], axis=0)
    kw_tag_df = pd.DataFrame({
        'kw_tag': kw_tag,
        'kw_tag_key': kw_tag_key})
    news_content_df = news_content_df.merge(kw_tag_df, left_on='news_key', right_on='kw_tag_key', how='left')
    news_content_df = news_content_df.drop('kw_tag_key', axis=1)
    news_content_df = news_content_df.fillna('')
    filt = (news_content_df['block_typ'] == '') & (news_content_df['source'] != '科技新報') & (
                news_content_df['kw_tag'] == '')
    news_content_df = news_content_df.loc[~filt]
    print('keyword tag')
    return keyword_df, news_content_df


def df_news_org(server):
    start_time_keyword_list = time.time()
    print(start_time_keyword_list)
    # sql connect
    db = 'TOPCO_WARROOM'
    uid = 'k2admin'
    pwd = 'k2admin'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
        server, db, uid, pwd))
    # keyword_detail insert iinto sql
    # for index,row in news_keyword_list.iterrows():
    #    cursor.execute("INSERT INTO dbo.NEWS_DEP2_KEYWORD_LIST([_index],[_key],[words])" "values (?,?,?)",
    #                    (
    #                    row['index'],
    #                   row['key'],
    #                   row['words'])
    #                   )
    #   conn.commit()

    # print("%s seconds ---" % (time.time() - start_time_keyword_list))
    # news_info_detail insert iinto sql
    start_time_news_detail = time.time()
    print(start_time_news_detail)
    for index, row in news_content_df.iterrows():
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO dbo.NEWS_DEP2_DETAIL([news_key],[title],[released_dt],[_source],[block_typ],[_url],[content],[web_tag],[kw_tag],[img_src],[updt_dt])" "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (row['news_key'],
                 row['title'],
                 row['published_dt'],
                 row['source'],
                 row['block_typ'],
                 row['url'],
                 row['content'],
                 row['tag'],
                 row['kw_tag'],
                 row['img_source'],
                 row['updt_dt'])
                )
            conn.commit()
        except Exception as e:
            error_class = e.__class__.__name__  # 取得錯誤類型
            detail = e.args[0]  # 取得詳細內容
            cl, exc, tb = sys.exc_info()  # 取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
            lineNum = lastCallStack[1]  # 取得發生的行號
            funcName = lastCallStack[2]  # 取得發生的函數名稱
            errMsg = "line {}, in {}: [{}] {}".format(lineNum, funcName, error_class, detail)
            print(errMsg)
            print('index_' + str(index) + 'too many words')
    print("%s seconds ---" % (time.time() - start_time_news_detail))

    # group_ref_df insert iinto sql
    start_time_keyword_tag_df = time.time()
    print(start_time_keyword_tag_df)
    for index, row in keyword_df.iterrows():
        cursor.execute("INSERT INTO dbo.NEWS_DEP2_KEYWORD_TAG([news_key],[kw_tag])" "VALUES(?,?)",
                       (row['news_key'],
                        row['keyword_tag'])
                       )
        conn.commit()

    cursor.close()
    conn.close()
    print("%s seconds ---" % (time.time() - start_time_keyword_list))
    print('DB INSERT')


def db_check_announce(recipient_list, sender, subject):
    server = '10.129.7.155'
    db = 'TOPCO_WARROOM'
    uid = 'uid'
    pwd = 'pwd'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;' % (
        server, db, uid, pwd))
    cursor = conn.cursor()
    command = 'select news_key from [TOPCO_WARROOM].[dbo].[NEWS_DEP2_DETAIL] group by news_key having count(*)>1'
    cursor.execute(command)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    if result != []:
        Public_Tool.TOPCO_SMTP(recipient_list=recipient_list, sender=sender, subject=subject)
    else:
        print('DB succeed!')


def sql_detail_info(server):
    db = 'TUSC_DA'
    uid = 'uid'
    pwd = 'pwd'
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;Mars_Connection=yes;' % (
            server, db, uid, pwd))
        cursor = conn.cursor()
        query_command = '''
          select a.Keyword_Level1,a.Keyword_Level2
          from  [TUSC_DA].[dbo].[Keywords] a
          inner join [TUSC_DA].[dbo].[Depts] b
          on a.Dept_ID=b.ID
          where b.Dept_Nm='晶圓營二部'
          and a.Active='1'
        '''
        cursor.execute(query_command)
        news_keyword_list = pd.read_sql(query_command, conn)
        cursor.close()
        conn.close()
    except:
        print('No key founded')
    return news_keyword_list


def db_drop_duplicate(server):
    db = 'TOPCO_WARROOM'
    uid = 'k2admin'
    pwd = 'k2admin'
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s; Trusted_Connection=no;Mars_Connection=yes;' % (
            server, db, uid, pwd))
        cursor = conn.cursor()
        dropdupli_command = '''
          with temp as(
            SELECT *, ROW_NUMBER() over(partition by _url order by _url) as rnk
            FROM [TOPCO_WARROOM].[dbo].[NEWS_DEP2_DETAIL]
            WHERE updt_dt>=dateadd(DAY,-2,GETDATE())
            )
            delete temp where rnk NOT IN (Select min(rnk) From temp Group By _url)
        '''
        cursor.execute(dropdupli_command)
        cnt = cursor.rowcount
        print('drop duplictaes:', cnt)
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    server = '10.129.7.155'

    # news_keyword_list=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/keywords.csv')
    # news_keyword_list['words'][23]='奕斯伟'
    news_keyword_list = sql_detail_info(server)
    news_keyword_list = news_keyword_list.rename(columns={'Keyword_Level1': 'key', 'Keyword_Level2': 'words'})
    query = news_keyword_list['words'].to_list()
    # query=['台積電','力積電','矽晶圓']
    # tmp_q=random.sample(news_keyword_list['words'].to_list(),3)
    source = ['Digitimes', '工商時報', '經濟日報']
    news_list = get_news_url_list(query, source)
    # news_list=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_list_log.csv',encoding='utf-8-sig')
    news_list.to_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_list_log.csv',
                     encoding='utf-8-sig')
    print('news list data get!')
    news_list = news_list.drop_duplicates(subset=['url'])
    start = time.time()
    time.sleep(random.randint(120, 150))
    news_content = news_list.apply(lambda x: news_content_f(x), axis=1)
    # news_content=pd.read_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_content_log.csv',encoding='utf-8-sig')
    news_content.to_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_content_log.csv',
                        encoding='utf-8-sig')
    print(time.time() - start)
    news_content_df = similarity_filter(news_content, news_keyword_list, source)
    keyword_df, news_content_df = content_tag(news_content_df, news_keyword_list)
    print(len(news_content_df))
    df_news_org()
    db_check_announce(recipient_list='chelsie.chang@topco-global.com', sender='chelsie.chang@topco-global.com',
                      subject='二本部新聞搜集器資料重複')
    # Round2
    source_2 = ['工商時報', '經濟日報']
    time.sleep(random.randint(300, 600))
    news_list_v2 = get_news_url_list(query, source_2)
    news_list_v2 = news_list_v2.drop_duplicates(subset=['url'])
    news_list_v2.to_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_list_log2.csv',
                        encoding='utf-8-sig')
    print('news list data get!')
    news_list_add = [i for i in news_list_v2['url'] if i not in news_list['url'].to_list()]
    news_added = news_list_v2[news_list_v2.url.isin(news_list_add)]
    news_content_v2 = news_added.apply(lambda x: news_content_f(x), axis=1)
    news_content_v2.to_csv('D:/04144_Profile/Desktop/crawl/project04_NewsSearch_Dep2/log/news_content_log2.csv',
                           encoding='utf-8-sig')
    news_v2 = pd.concat([news_content, news_content_v2], axis=0)
    news_add = similarity_filter(news_v2, news_keyword_list, source_2)
    news_add_df = news_content_v2[news_content_v2.url.isin(news_add['url'].to_list())]
    news_add_df = pd.merge(news_add_df, news_add[['url', 'news_key']], how='inner', on=['url'])
    keyword_df, news_content_df = content_tag(news_add_df, news_keyword_list)
    df_news_org()
    print('finished')
    db_drop_duplicate(server)