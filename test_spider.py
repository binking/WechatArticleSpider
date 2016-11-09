#coding=utf-8
import sys, time
import MySQLdb as mdb
from wetchat_spider import get_article_info
from sougou_spider import parse_sougou_results
from abuyun_proxy import gen_abuyun_proxy, test_abuyun, get_current_ip
from database_operator import (
    write_article_into_db, 
    write_topic_into_db,
    read_topics_from_db
)
reload(sys)
sys.setdefaultencoding('utf-8')

IGNORE_RECORD = -6


def main():
    WEBCRAWLER_DB_CONN = mdb.connect(
        host = "192.168.1.103", 
        user="web", 
        passwd="Crawler@test1", 
        db="webcrawler",
        charset="utf8"
    )
    abuyun_proxy = gen_abuyun_proxy()
    print abuyun_proxy
    for kw in read_topics_from_db(WEBCRAWLER_DB_CONN, limit=100)[18:]:
        sougou_result = parse_sougou_results(kw)
        write_topic_into_db(WEBCRAWLER_DB_CONN, sougou_result['data'])
        for i, url in enumerate(sougou_result['data']['urls']):
            print "%d-th " % i,
            wetchat_result = get_article_info(sougou_result['data'].get('search_url', ), url, proxy=abuyun_proxy)
            proxy_info = get_current_ip()
            if wetchat_result['err_no'] == IGNORE_RECORD:
                # ignore the article
                continue
            wetchat_data = wetchat_result['data']
            write_article_into_db(WEBCRAWLER_DB_CONN, wetchat_data)
            print "The article has %d like, %d read and %d characters\n" % (
                wetchat_data.get('like_num', -1), 
                wetchat_data.get('read_num', -1), 
                len(wetchat_data.get('content', '')))
    WEBCRAWLER_DB_CONN.close()

if __name__=="__main__":
    start_time = time.time()
    main()
    print "Time Consuming: %d" % (time.time() - start_time)