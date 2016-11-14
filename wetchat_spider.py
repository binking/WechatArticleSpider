#coding=utf-8
import sys, time
import requests, urllib, signal
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
from abuyun_proxy import change_tunnel
reload(sys)
sys.setdefaultencoding('utf-8')


SUCCESSED = 1
FAILED = -1
PARSE_ERROR = -2
ACCESS_URL_ERROR = -3
WRITE_DB_ERROR = -4
SYNTAX_ERROR = -5
IGNORE_RECORD = -6


WX_CURL = """curl 'http://mp.weixin.qq.com/mp/getcomment?src={src}&ver={ver}&timestamp={timestamp}&signature={signature}&&uin=&key=&pass_ticket=&wxtoken=&devicetype=&clientversion=0&x5=0' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36' -H 'Accept: */*' -H 'Referer: {article_url}' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"""

def handle_sleep(seconds):
    print "Sleeping %d seconds " % seconds
    time.sleep(seconds)

def handle_proxy_error(seconds):
    print "Sleep %d seconds " % seconds, 
    handle_sleep(seconds)
    changed_proxy = change_tunnel()
    if changed_proxy:
        print "and change IP to %s " % changed_proxy.get("ip_addr")
    else:
        print "but Change Proxy Error"


def curl_str2post_data(wx_url):
    url = ""
    post_data = {}
    # Get query string from url
    query_string = parse_qs(urlparse(wx_url).query)
    # Format api access
    curl_str = WX_CURL.format(
        src=query_string.get("src", [""])[0],
        ver=query_string.get("ver", [""])[0],
        timestamp=query_string.get("timestamp", [""])[0],
        article_url=wx_url,
        signature=urllib.quote(query_string.get("signature", [""])[0]),
    )
    tokens = curl_str.split("'")
    try:
        for i in range(0, len(tokens)-1, 2):
            if tokens[i].startswith("curl"):
                url = tokens[i+1]
            elif "-H" in tokens[i]:
                attr, value = tokens[i+1].split(": ")  # be careful space
                post_data[attr] = value
    except Exception as e:
        print "!"*20, "Parsed cURL Failed"
        traceback.print_exc()
    return url, post_data


def get_like_vote_nums(url, data, proxy={}, num_tries=5, wait_time=10):
    """
    Given the url of wechat article and proxies of Abuyun,
    get the numbers of praise and read throught Web Api json
    param wx_url(str): the url of wechat
    param proxy(dict, optional): the Abutun proxies
    """
    like_num = -1
    read_num = -1
    if not(url and data):  # transfer curl string failed.
        return like_num, read_num
    for attempt in range(1, num_tries+1):
        try:
            r = requests.get(url, params=data, proxies=proxy, timeout=wait_time)
            response_dict = json.loads(r.text)
            like_num = int(response_dict.get('like_num', -1))
            read_num = int(response_dict.get('read_num', -1))
            break
        except ValueError as e:
            if not len(r.text):
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"), url, "returns none, try again after",
            handle_sleep(5*attempt)
        except Timeout as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Timeout",
            handle_sleep(5*attempt)
        except ConnectionError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Connect error"
            handle_sleep(5*attempt)
        except ProxyError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Proxy error"
            handle_proxy_error(5*attempt)
        except Exception as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
            print "Get the numbers of like and read FAILED"
            break  # unkown error should interrupt loop
    return like_num, read_num


def get_article_content(wx_url, proxy={}, num_tries=3, wait_time=5):
    """
    Given a url of wechat article, parse the HTML source code and get text
    param wx_url(str): url string
    param proxy(dict, optional): the proxy of Abuyun
    return content(str): long Chinese text
    """
    content = ""
    for attempt in range(1, num_tries+1):
        try:
            r = requests.get(wx_url, proxies=proxy, timeout=wait_time)
            parser = bs(r.text, "html.parser")
            content_div = parser.find("div", attrs={"class":"rich_media_content"})
            # read_span = parser.find("span", attrs={"id": "sg_readNum3"})  No read num
            # praise_span = parser.find("span", attrs={"id": "sg_likeNum3"})  No like num
            if content_div:
                for child in content_div.children:
                    if isinstance(child, bs4.element.Tag):
                        content += child.text
            break
        except Timeout as e:  # ConnectTimeout and ReadTimeout is its child class
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
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Parsed Content Failed..."
            break
    return content


def get_article_info(sougou_url, wx_url, search_word='', proxy={}):
    """
    Given the wetchar article url, call get_like_vote_nums() and get_article_content()
    param wx_url(str): the url of wetchat article
    param proxy(dict, optional): the Abuyun proxies
    return article_info(dict): {
        content: article text, 
        read_num: the number of read, 
        like_num: the number of praise,
    }
    """
    if search_word:
        print "Parsing Wetchat article about %s: %s;" % (search_word, wx_url),
    else:
        print "Parsing Wetchat article : ", wx_url
    err_no = 0
    err_msg = ""
    article_info = {}  # form the info dict of article
    article_info['createdate'] = dt.now().strftime('%Y-%m-%d %H:%M:%S')  # 2016-11-07 12:23:45
    article_info['search_url'] = sougou_url
    article_info['uri'] = wx_url
    article_info['article_url'] = wx_url
    # trasfer curl string to requests send data
    api_url, post_data = curl_str2post_data(wx_url)
    like_num, read_num = get_like_vote_nums(api_url, post_data, proxy)
    content = get_article_content(wx_url, proxy)
    article_info["read_num"] = read_num
    article_info["like_num"] = like_num
    article_info["content"] = content
    print "it has %d like, %d read and %d characters\n" % (
            like_num, read_num, len(content))
    if read_num < 0:
        err_no = IGNORE_RECORD  # if failed in getting read_num, not write into db
    return dict(err_no=err_no, err_msg=err_msg, data=article_info)


def test_get_article_info():
    test_url_1 = """http://mp.weixin.qq.com/s?src=3&timestamp=1478570647&ver=1&signature=KS8paN*5j4sfXIFoIJPsTcQNnt7NttrL3H2d0os6iPB9EqSymvwhtDEYIC1UFambJTwmBuNsom8r6-5LATw3B6XGcNG6BDp-Fv389JRixjYBZNWiWjpsruwKJA02nMJouDiN8CjC*oR4fc8If8YxoNRJeGDJUi1YGpfMb8VKDNA="""
    test_url_2 = """http://mp.weixin.qq.com/s?src=3&timestamp=1478570647&ver=1&signature=iSeOhDyVHhgA*iduVnOdEDhRB*erUhkmIvidSc-OdvoYdMeiVDNrT24WR90Id3PExZJD4vsPwgkSCXgvziRXmM4krD-yVq9URFPC*JeMR2dw-8Okq12aA0yKHJaUvAGbWUqEegOzcdulrFCWh02V4vA5S8tRV7m5oxV-BzveWdc="""
    test_url_3 = """http://mp.weixin.qq.com/s?src=3&timestamp=1478570647&ver=1&signature=eVL09gtr0sC4W8lm6scLNSUQeD1CPSJe9A6wXPetzSwBYDZSl*pCCblckZ*PfzCsJJsTcMPVmObBlHfjh9d1kiByIuzT-QKYCYqZXlfBnXL8ZY9MxRsGiLoLLg3ydiIcI*r0WA1shdNYuIk5mEpv63Hd9Zqu0Vvp8smp1oWmNP4="""
    print get_article_info(test_url_1)
    print get_article_info(test_url_2)
    print get_article_info(test_url_3)

