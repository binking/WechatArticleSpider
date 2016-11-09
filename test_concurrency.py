#coding=utf-8
import sys, time
from datetime import datetime as dt
# import MySQLdb as mdb
from queue import Queue
from multiprocessing.dummy import Pool as MPool
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
URL_QUEUE = Queue()
ARTICLE_QUEUE = Queue()
TOPIC_QUEUE = Queue()

def test_wxurl_generator(keyword):
	"""
	Producer for urls and topics
	"""
	sougou_result = parse_sougou_results(keyword)
	sougou_url = sougou_result['data'].get('search_url', )
	TOPIC_QUEUE.put(sougou_result['data'])
	for url in sougou_result['data']['urls']:
		URL_QUEUE.put("%s|%s" % (sougou_url, url))  # merge two url

def test_wxarticle_generator(abuyunproxy={}):
	"""
	Consummer for urls and Producer for articles
	"""
	while not URL_QUEUE.empty():
		merged_url = URL_QUEUE.get_nowait()
		s_url, w_url = merged_url.split("|")
		wetchat_result = get_article_info(s_url, w_url, proxy=abuyun_proxy)
		ARTICLE_QUEUE.put(wetchat_result['data'])  # generate article data and put into queue

def test_article_db_writer():
	"""
	Consummer for articles
	"""
	while not ARTICLE_QUEUE.empty():
		article_record = ARTICLE_QUEUE.get_nowait()
		print "The article has %d like, %d read and %d characters\n" % (
                    article_record.get('like_num', -1), 
                    article_record.get('read_num', -1), 
                    len(article_record.get('content', '')))

def test_topic_db_writer():
	"""
	Consummer for topics
	"""
	while not TOPIC_QUEUE.empty():
		topic_record = TOPIC_QUEUE.get_nowait()
		print "Topic %s has %d wx_urls" % (
			topic_record['search_keyword'], len(topic_record['urls']))

def run_all_worker(concurrency=4):
	abuyun_proxy = gen_abuyun_proxy()
    try:
    	# Producer is on !!!
        list_of_kw = ["特朗普", "45任总统", "暴走大事件", "王尼玛", "阴阳师"]
		pool = MPool(concurrency)  # Processes pool
        pool.map(test_wxurl_generator, list_of_kw)  # Keep up generate keywords
        pool.close()
        pool.join()  # why join
    except KeyboardInterrupt:
        print "Interrupted by you and quit in force, but save the results"
    # Consummer followes
    test_wxurl_generator(abuyun_proxy)
    test_article_db_writer()
    test_topic_db_writer()


if __name__=="__main__":
	print "\n" + "Began Scraping time is : %s" % dt.now() + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Total Scrping Time Consumed : %d seconds" % (time.time() - start), "*"*10
