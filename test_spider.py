#coding=utf-8
import sys, time, traceback
import MySQLdb as mdb
from datetime import datetime as dt
from wetchat_spider import get_article_info
from sougou_spider import parse_sougou_results
from abuyun_proxy import gen_abuyun_proxy, test_abuyun, get_current_ip
from database_operator import (
    connect_database,
    write_article_into_db, 
    write_topic_into_db,
    read_topics_from_db
)
reload(sys)
sys.setdefaultencoding('utf-8')

IGNORE_RECORD = -6
MYSQL_GONE_ERROR = -100

def main():
    db_conn = connect_database()
    abuyun_proxy = gen_abuyun_proxy()
    for kw in read_topics_from_db(db_conn)[:100]:
        sougou_result = parse_sougou_results(kw)
        result_no1 = write_topic_into_db(db_conn, sougou_result['data'])
        if result_no1 == MYSQL_GONE_ERROR:  # reconnect database
            time.sleep(5)
            db_conn.close()
            db_conn = connect_database()
        for i, url in enumerate(sougou_result['data']['urls']):
            try:
                print "%d-th " % (i+1),
                wetchat_result = get_article_info(sougou_result['data'].get('search_url', ), url, proxy=abuyun_proxy)
                # proxy_info = get_current_ip()
                if wetchat_result['err_no'] == IGNORE_RECORD:
                    continue
                wetchat_data = wetchat_result['data']
                result_no2 = write_article_into_db(db_conn, wetchat_data)
                if result_no2 == MYSQL_GONE_ERROR:
                    time.sleep(5)
                    db_conn.close()
                    db_conn = connect_database()
            except Exception as e:
                print "Error in main function"
                traceback.print_exc()
    db_conn.close()

if __name__=="__main__":
    print "\n" + "Began Scraping time is : %s" % dt.now() + "\n"
    start = time.time()
    main()
    print "*"*10, "Total Scrping Time Consumed : %d seconds" % (time.time() - start), "*"*10
