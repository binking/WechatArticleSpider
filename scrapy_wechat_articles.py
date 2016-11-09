#coding=utf-8
import sys, time
from datetime import datetime as dt
import MySQLdb as mdb
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

def wxurl_generator(keyword):
	"""
	Producer for urls and topics
	"""
	sougou_result = parse_sougou_results(keyword)
	sougou_url = sougou_result['data'].get('search_url', )
	TOPIC_QUEUE.put(sougou_result['data'])
	for url in sougou_result['data']['urls']:
		URL_QUEUE.put("%s|%s" % (sougou_url, url))  # merge two url

def wxarticle_generator(abuyunproxy={}):
	"""
	Consummer for urls and Producer for articles
	"""
	while not URL_QUEUE.empty():
		merged_url = URL_QUEUE.get_nowait()
		s_url, w_url = merged_url.split("|")
		wetchat_result = get_article_info(s_url, w_url, proxy=abuyun_proxy)
		ARTICLE_QUEUE.put(wetchat_result['data'])  # generate article data and put into queue

def article_db_writer():
	"""
	Consummer for articles
	"""
	with mdb.connect(host = "192.168.1.103", user="web", 
        passwd="Crawler@test1", db="webcrawler", charset="utf8" ) as conn:
		while not ARTICLE_QUEUE.empty():
			article_record = ARTICLE_QUEUE.get_nowait()
			write_article_into_db(conn, article_record)

def topic_db_writer():
	"""
	Consummer for topics
	"""
	with mdb.connect(host = "192.168.1.103", user="web", 
        passwd="Crawler@test1", db="webcrawler", charset="utf8" ) as conn:
		while not TOPIC_QUEUE.empty():
			topic_record = TOPIC_QUEUE.get_nowait()
			write_topic_into_db(conn, topic_record)

def  run_all_worker(conn, concurrency=4):
	abuyun_proxy = gen_abuyun_proxy()
    try:
    	# Producer is on !!!
        list_of_kw = read_topics_from_db(conn)
		pool = MPool(concurrency)  # Processes pool
        pool.map(wxurl_generator, list_of_kw)  # Keep up generate keywords
        pool.close()
        pool.join()  # why join
    except KeyboardInterrupt:
        print "Interrupted by you and quit in force, but save the results"
    # Consummer followes
    wxurl_generator(abuyun_proxy)
    article_db_writer(conn)
    topic_db_writer(conn)


if __name__=="__main__":
	print "\n" + "Began Scraping time is : %s" % dt.now() + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Total Scrping Time Consumed : %d seconds" % (time.time() - start), "*"*10
