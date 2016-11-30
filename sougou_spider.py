#coding=utf-8
import sys, time, re
import requests, urllib, traceback
import json, traceback, bs4
from datetime import datetime as dt
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup as bs
from requests.exceptions import (
    ProxyError,
    Timeout,
    ConnectionError,
    ConnectTimeout,
)
from abuyun_proxy import (
    gen_abuyun_proxy, 
    change_tunnel,
    get_current_ip
)
reload(sys)
sys.setdefaultencoding('utf-8')

SUCCESSED = 1
FAILED = -1
PARSE_ERROR = -2
ACCESS_URL_ERROR = -3
WRITE_DB_ERROR = -4
SYNTAX_ERROR = -5
IGNORE_RECORD = -6

QUERY_URL_DICT = {
    "week": """http://weixin.sogou.com/weixin?oq=&query={kw}&_sug_type_=&tsn=2&sut=331&sourceid=inttime_week&ri=0&_sug_=n&type=2&interation=&ie=utf8&sst0=1478500034818&interV=kKIOkrELjboJmLkElbYTkKIKmbELjbkRmLkElbk%3D_1893302304""",
    "day": """http://weixin.sogou.com/weixin?oq=&query={kw}&_sug_type_=&tsn=1&sut=331&sourceid=inttime_day&ri=0&_sug_=n&type=2&interation=&ie=utf8&sst0=1478500034818&interV=kKIOkrELjboJmLkElbYTkKIKmbELjbkRmLkElbk%3D_1893302304""",
    "month": """http://weixin.sogou.com/weixin?oq=&query={kw}&_sug_type_=&tsn=3&sut=331&sourceid=inttime_month&ri=0&_sug_=n&type=2&interation=&ie=utf8&sst0=1478500034818&interV=kKIOkrELjboJmLkElbYTkKIKmbELjbkRmLkElbk%3D_1893302304""",
}

def handle_sleep(seconds):
    print "Sleeping %d seconds " % seconds, 'zZ'*10
    time.sleep(seconds)

def parse_sougou_results(keyword, num_tries=3, wait_time=10):
    """
    Given keyword, form the Sougou search url and parse the search results page
    param keywords:list of keywords
    return : {err_no: , err_msg: , data: 
        { uri: , createdate:, search_url:, }}
    """
    print "Sougou searching for ", keyword
    wx_article_urls = []
    proxy = gen_abuyun_proxy()
    err_no = SUCCESSED; err_msg = "Successed"; data = {}
    for attempt in range(1, num_tries+1):
        try:
            try:
                kw = urllib.quote(keyword.encode('utf8'))
            except (UnicodeEncodeError, KeyError) as e:
                print "Quoting Chinese keyword %s to url string FAILED" % keyword
                kw = keyword
            url = QUERY_URL.format(kw=kw)
            # url = QUERY_URL_DICT['week'].format(kw=keyword)
            r =requests.get(url, proxies=proxy, timeout=wait_time)
            parser = bs(r.text, "html.parser")
            if len(parser.find_all()) < 2:
                print "Dammit, Sleep %d seonds, cuz Sogou send wrong message to you..." % (3*attempt)
                handle_sleep(3*attempt)
                continue
            for a_tag in parser.find_all("a", {
                "id": re.compile(r"sogou.*title"), 
                "href": re.compile("http://mp.weixin.qq.com*")
            }):
                # the keyword was embeded in text in red color
                wx_article_urls.append(a_tag.get("href", ""))
            data = { "createdate": dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
                      "uri": url, "search_url": url, 
                      "search_keyword": keyword, "urls": wx_article_urls}
            break # success and jump out of loop
        except Timeout as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Timeout", 
            handle_sleep(5*attempt)
        except ConnectionError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Connect error", 
            handle_sleep(5*attempt)
        except ProxyError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Proxy error",
            handle_proxy_error(5*attempt)
        except Exception as e:
            traceback.print_exc()
            err_no = FAILED
            err_msg = e.message
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Parsed topic %s Failed..." % keyword
            break  # unkown error should interrupt loop
    return {"err_no": err_no, "err_msg": err_msg, "data": data}


def str_2_int(num_str):
    try:
        return int(num_str.replace(',', ''))
    except Exception as e:
        traceback.print_exc()
        return -1


def get_sougou_top_result(keyword, date_range, num_tries=4, wait_time=10):
    """
    Given keyword, form the Sougou search url and parse the search results page
    param keywords:list of keywords
    return : {err_no: , err_msg: , data: 
        { uri: , createdate:, search_url:, }}
    """
    print "Sougou searching for ", keyword, "in 1 ", date_range
    # proxy = gen_abuyun_proxy()
    proxy = {}
    err_no = SUCCESSED; err_msg = "Successed"
    # url = QUERY_URL_DICT[date_range].format(kw=urllib.quote(keyword))
    url = QUERY_URL_DICT[date_range].format(kw=keyword)
    print url
    data = { "createdate": dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
              "uri": url, "search_url": url, 
              "search_keyword": keyword,
              "date_range": date_range,
              "hit_num": -1,
              "top_url": '',
              "top_title": '',
               }
    for attempt in range(1, num_tries+1):
        try:
            r =requests.get(url, proxies=proxy, timeout=wait_time)
            parser = bs(r.text, "html.parser")
            if len(parser.find_all()) < 20:
                change_tunnel()
                print "Dammit, Sleep %d seonds, and change IP to %s" % (3**(attempt-1), get_current_ip())
                handle_sleep(3**(attempt-1))
                continue
            a_tags = parser.find_all("a", {
                "id": re.compile(r"sogou.*title"), 
                "href": re.compile("http://mp.weixin.qq.com*")
            })
            if a_tags:
                a_tag = a_tags[0]
                data["top_url"] = a_tag.get("href", "")
                data["top_title"] = a_tag.text
                resume_tag = parser.find("resnum", {"id": "scd_num"})
                if resume_tag:
                    data["hit_num"] = str_2_int(resume_tag.text)
                else:
                    data["hit_num"] = len(a_tags)
            elif len(a_tags)==0 and parser.find("div", {"class": "no-sosuo"}):
                # really no result
                data["hit_num"] = 0
            elif parser.find('div', {'id': 'noresult_part1_container'}):
                data['hit_num'] = 0
            else:  # no result
                with open('%s_%s.html' % (dt.now().strftime('%Y-%m-%d-%H:%M:%S'), keyword.encode('utf8')), 'w') as fw:
                    # null_parser = BS(open('2016-11-15 16:11:06.167554_朴施妍1114生日快乐.html', 'r').read(), "html.parser")
                    print >>fw, r.text.encode('utf8')  # save the unknow html
            break # success and jump out of loop
        except Timeout as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Timeout", 
            handle_sleep(5*attempt)
        except ConnectionError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Connect error", 
            handle_sleep(5*attempt)
        except ProxyError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Proxy error",
            handle_proxy_error(5*attempt)
        except Exception as e:
            traceback.print_exc()
            err_no = FAILED
            err_msg = e.message
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Parsed topic %s Failed..." % keyword
            break  # unkown error should interrupt loop
    return {"err_no": err_no, "err_msg": err_msg, "data": data}


def test_parse_sougou_results():
    abuyun_proxy = gen_abuyun_proxy()
    list_of_kw = ["特朗普", "45任总统", "暴走大事件", "王尼玛", "阴阳师"]
    for date_range in ['week', 'day', 'month']:
        # print parse_sougou_results(kw, abuyun_proxy)
        print get_sougou_top_result("特朗普", date_range)
