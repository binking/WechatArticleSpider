#coding=utf-8
import sys
import requests, urllib, json, traceback, bs4
import MySQLdb as mdb
from datetime import datetime as dt
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup as bs
reload(sys)
sys.setdefaultencoding('utf-8')


SUCCESSED = 1
FAILED = -1
PARSE_ERROR = -2
ACCESS_URL_ERROR = -3
WRITE_DB_ERROR = -4
SYNTAX_ERROR = -5
IGNORE_RECORD = -6

QUERY_URL = """http://weixin.sogou.com/weixin?oq=&query={keyword}&_sug_type_=&tsn=2&sut=331&sourceid=inttime_week&ri=0&_sug_=n&type=2&interation=&ie=utf8&sst0=1478500034818&interV=kKIOkrELjboJmLkElbYTkKIKmbELjbkRmLkElbk%3D_1893302304"""
WX_CURL = """curl 'http://mp.weixin.qq.com/mp/getcomment?src={src}&ver={ver}&timestamp={timestamp}&signature={signature}&&uin=&key=&pass_ticket=&wxtoken=&devicetype=&clientversion=0&x5=0' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36' -H 'Accept: */*' -H 'Referer: {article_url}' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"""
WEBCRAWLER_DB_CONN = mdb.connect(host = "192.168.1.103", user="web", passwd="Crawler@test1", db="webcrawler", charset="utf8")

def gen_abuyun_proxy():
    # proxy server
    proxyHost = "proxy.abuyun.com"
    proxyPort = "9010"

    # authorization
    proxyUser = "H778U07K14M4250P"
    proxyPass = "FE04DDEF88A0CC9B"
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host" : proxyHost,
        "port" : proxyPort,
        "user" : proxyUser,
        "pass" : proxyPass,
    }
    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }
    return proxies

def parse_sougou_results(keyword):
    """
    Given keyword, form the Sougou search url and parse the search results page
    param keywords:list of keywords
    return : {err_no: , err_msg: , data: 
    	{ uri: , createdate:, search_url:, }}
    """
    print "Sougou searching for ", keyword
    wx_article_urls = []
    try:
        url = QUERY_URL.format(keyword=urllib.quote(keyword))
        access_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        r =requests.get(url)
        parser = bs(r.text, "html.parser")
        for a_tag in parser.find_all("a"):
            if "http://mp.weixin.qq.com" in a_tag.get("href", "") and a_tag.find("em"):
                # the keyword was embeded in text in red color
                wx_article_urls.append(a_tag.get("href", ""))
    except Exception as e:
    	print "Parsed Sougou Results Failed..."
        traceback.print_exc()
        return {"err_no": FAILED, "err_msg": e.message, "data": {}}
    return {"err_no": SUCCESSED, "err_msg": "", 
    	    "data": { "createdate": access_time, 
    	              "uri": url, 
                      "search_url": url, 
                      "search_keyword": keyword, 
                      "urls": wx_article_urls}}


def write_topic_into_db(topic_info):
    """
    Update two tables: wechatsearchtopic and wechatsearcharticlerelation
    param topic_info(dict): createdate, uri, search_url, search_keyword, urls
    """
    deprecate_topic = "UPDATE wechatsearchtopic " \
                       "SET is_up2date='N' " \
                       "WHERE search_keyword='{kw}';" \
                       .format(kw=topic_info.get('search_keyword', ''))
    insert_new_topic = "INSERT INTO wechatsearchtopic " \
                       "(uri, search_keyword, createdate, search_url) " \
                       "VALUES ('%s', '%s', '%s', '%s');" % \
                       (topic_info.get('uri', ''), topic_info.get('search_keyword', ''), \
                       topic_info.get('createdate', ''), topic_info.get('search_url', ''))
    try:
        # import ipdb; ipdb.set_trace()
        cursor = WEBCRAWLER_DB_CONN.cursor()
        cursor.execute(deprecate_topic)
        cursor.execute(insert_new_topic)
        WEBCRAWLER_DB_CONN.commit()
        print "Write topic succeeded..."
    except Exception as e:
    	print "Write topic failed"
        traceback.print_exc()
        WEBCRAWLER_DB_CONN.rollback()
    finally:
        cursor.close()

    
def write_article_into_db(article_info):
    """
    param article_info(dict): 
    """
    search_url = article_info.get('search_url', "")
    article_url = article_info.get('article_url', "")
    insert_new_relation = "INSERT INTO wechatsearcharticlerelation " \
                           "(search_url, search_date, article_url) " \
                           "VALUES ('{surl}', '{date}', '{aurl}');"
    insert_new_article = "INSERT INTO wechatarticle " \
                          "(uri, createdate, article_url, content, read_num, thumb_up_num) " \
                          "VALUES ('{uri}', '{date}', '{aurl}', '{text}', {rnum}, {tnum});"
    try:
        # Soft-remove old relation and insert new relation
        cursor = WEBCRAWLER_DB_CONN.cursor()
        cursor.execute("""
            UPDATE wechatsearcharticlerelation
            SET is_up2date='N'
            WHERE search_url=%s AND article_url=%s
        """, (search_url, article_url))  # prevent same article url from multiple searches
        cursor.execute(insert_new_relation.format(
            surl=search_url,
            date=article_info.get('createdate', ''), aurl=article_url))  # No need to set is_up2date, cuz temp wx url would be deprecated automatically. T^T

        is_existed = cursor.execute("""
            SELECT article_url FROM wechatarticle
            WHERE article_url=%s
        """, (article_url, ))
        if not is_existed:
            # Adjust whether insert new article
            cursor.execute(insert_new_article.format(
                uri=article_url,
                aurl=article_url,
                date=article_info.get('createdate', ''),
                text=article_info.get('content', ''),
                rnum=article_info.get('read_num', -1),
                tnum=article_info.get('like_num', -1)
            ))  # No need to adjust the temp wx url exited, also
        WEBCRAWLER_DB_CONN.commit()
        print "Write article succeeded..."
    except Exception as e:
    	print "Write article Failed..."
        traceback.print_exc()
        WEBCRAWLER_DB_CONN.rollback()
    finally:
        cursor.close()


def curl_str2post_data(curl_str):
    tokens = curl_str.split("'")
    url = ""
    post_data = {}
    try:
        for i in range(0, len(tokens)-1, 2):
            if tokens[i].startswith("curl"):
                url = tokens[i+1]
            elif "-H" in tokens[i]:
                attr, value = tokens[i+1].split(": ")  # be careful space
                post_data[attr] = value
    except Exception as e:
        print "Parsed cURL Failed"
        traceback.print_exc()
        return '', {}
    return url, post_data


def get_like_vote_nums(wx_url, proxy={}):
    """
    Given the url of wechat article and proxies of Abuyun,
    get the numbers of praise and read throught Web Api json
    param wx_url(str): the url of wechat
    param proxy(dict, optional): the Abutun proxies
    """
    print "Parsing Wetchat article: ", wx_url
    like_num = -1
    reaq_num = -1
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
    # trasfer curl string to requests send data
    api_url, post_data = curl_str2post_data(curl_str)
    if not(api_url and post_data):  # transfer curl string failed.
        return -1, -1

    try:
        r = requests.get(api_url, params=post_data, proxies=proxy)
        response_dict = json.loads(r.text)
        like_num = int(response_dict.get('like_num', -1))
        read_num = int(response_dict.get('read_num', -1))
    except Exception as e:
        print "Get the numbers of like and read FAILED"
        traceback.print_exc()
    return like_num, read_num


def get_article_content(wx_url, proxy={}):
    """
    Given a url of wechat article, parse the HTML source code and get text
    param wx_url(str): url string
    param proxy(dict, optional): the proxy of Abuyun
    return content(str): long Chinese text
    """
    content = ""
    try:
        r = requests.get(wx_url, proxies=proxy)
        parser = bs(r.text, "html.parser")
        content_div = parser.find("div", attrs={"class":"rich_media_content"})
        # read_span = parser.find("span", attrs={"id": "sg_readNum3"})  No read num
        # praise_span = parser.find("span", attrs={"id": "sg_likeNum3"})  No like num
        if content_div:
            for child in content_div.children:
                if isinstance(child, bs4.element.Tag):
                    content += child.text
    except Exception as e:
    	print "Parsed Content Failed..."
        traceback.print_exc()
    return content


def get_article_info(sougou_url, wx_url, proxy={}):
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
    err_no = -0
    err_msg = ""
    article_info = {}  # form the info dict of article
    article_info['createdate'] = dt.now().strftime('%Y-%m-%d %H:%M:%S')  # 2016-11-07 12:23:45
    article_info['search_url'] = sougou_url
    article_info['uri'] = wx_url
    article_info['article_url'] = wx_url
    like_num, read_num = get_like_vote_nums(wx_url, proxy)
    content = get_article_content(wx_url, proxy)
    article_info["read_num"] = read_num
    article_info["like_num"] = like_num
    article_info["content"] = content
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


if __name__=="__main__":
    abuyun_proxy = gen_abuyun_proxy()
    print abuyun_proxy
    for kw in ["美国大选"]:
        sougou_result = parse_sougou_results(kw)
        write_topic_into_db(sougou_result['data'])
        for url in sougou_result['data'].get("urls", []):
            wetchat_result = get_article_info(sougou_result['data'].get('search_url', ), url, proxy=abuyun_proxy)
            if wetchat_result['err_no'] == IGNORE_RECORD:
                # ignore the article
                continue
            wetchat_data = wetchat_result['data']
            write_article_into_db(wetchat_data)
            print "This article has %d like, %d read and %d characters\n" % (wetchat_data.get('like_num', -1), wetchat_data.get('read_num', -1), len(wetchat_data.get('content', '')))
    WEBCRAWLER_DB_CONN.close()
