#coding=utf-8
import sys, time
from datetime import datetime as dt
import MySQLdb as mdb
# from Queue import Queue
# from multiprocessing.dummy import Pool as MPool
import multiprocessing as mp
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


def wxurl_generator(keywords, url_queue, topic_queue):
    """
    Producer for urls and topics
    """
    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Urls Process %s's pid is %d" % (cp, cp.pid) 
    for kw in keywords:
        sougou_result = parse_sougou_results(kw)
        sougou_url = sougou_result['data'].get('search_url', )
        topic_queue.put(sougou_result['data'])
        print "There are %d urls of %s" % (len(sougou_result['data']['urls']), kw)
        for url in sougou_result['data']['urls']:
            url_queue.put("%s|%s|%s" % (kw, sougou_url, url))  # merge two url


def wxarticle_generator(url_queue, article_queue):
    """
    Consummer for urls and Producer for articles
    """
    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Article Process %s's pid is %d" % (cp, cp.pid)
    abuyun_proxy = gen_abuyun_proxy()
    while True:
        merged_url = url_queue.get()
        word, s_url, w_url = merged_url.split("|")
        wetchat_result = get_article_info(s_url, w_url, search_word=word, proxy=abuyun_proxy)
        article_queue.put(wetchat_result['data'])  # generate article data and put into queue
        url_queue.task_done()


def article_db_writer(article_queue):
    """
    Consummer for articles
    """
    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Article Process %s's pid is %d" % (cp, cp.pid)
    while True:
        with connect_database() as cursor:
            # using try-with-recources, auto-commit
            article_record = article_queue.get()
            write_status = write_article_into_db(cursor, article_record)
            if write_status:
                article_queue.task_done()


def topic_db_writer(topic_queue):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Topics Process %s's pid is %d" % (cp, cp.pid)
    while True:
        with connect_database() as cursor:
            topic_record = topic_queue.get()
            write_status = write_topic_into_db(cursor, topic_record)
            if write_status:
                topic_queue.task_done()


def run_all_worker():
    try:
        # Producer is on !!!
        url_queue = mp.JoinableQueue()
        topic_queue = mp.JoinableQueue()
        article_queue = mp.JoinableQueue()
        for _ in range(6):  # different process, different speed
            parse_article_proc = mp.Process(target=wxarticle_generator, 
                args=(url_queue, article_queue))
            parse_article_proc.daemon = True
            parse_article_proc.start()

        write_topic_proc = mp.Process(target=topic_db_writer, args=(topic_queue,))
        write_topic_proc.daemon = True
        write_topic_proc.start()

        for _ in range(6):  #  4 processes to write article info into db
            write_article_proc = mp.Process(target=article_db_writer, args=(article_queue, ))
            write_article_proc.daemon = True
            write_article_proc.start()

        try:
            cp = mp.current_process()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Word Process %s's pid is %d" % (cp, cp.pid)
            conn = connect_database()
            if not conn:
                return False
            list_of_kw = read_topics_from_db(conn.cursor())
            wxurl_generator(list_of_kw, url_queue, topic_queue)
            url_queue.join()
            topic_queue.join()
            article_queue.join()
        except mdb.OperationalError as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            traceback.print_exc()
        finally:
            conn.close()
            return True
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"


if __name__=="__main__":
    # cp = mp.current_process()
    # print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Main Process %s's pid is %d" % (cp, cp.pid)
    print "\n" + "Began Scraping time is : %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Total Scrping Time Consumed : %d seconds" % (time.time() - start), "*"*10
