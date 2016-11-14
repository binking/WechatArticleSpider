#coding=utf-8
import sys, time, os, signal, traceback
from datetime import datetime as dt, timedelta
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


def wxurl_generator(topic_jobs, url_jobs, topic_results):
    """
    Producer for urls and topics, Consummer for topics
    """
    cp = mp.current_process()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Urls Process pid is %d" % (cp.pid)
        kw = topic_jobs.get()
        sougou_result = parse_sougou_results(kw)
        sougou_url = sougou_result['data'].get('search_url', )
        topic_results.put(sougou_result['data'])
        print "There are %d urls of %s" % (len(sougou_result['data']['urls']), kw)
        for url in sougou_result['data']['urls']:
            url_jobs.put("%s|%s|%s" % (kw, sougou_url, url))  # merge two url
        topic_jobs.task_done()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Urls Process %d finished" % (cp.pid)


def wxarticle_generator(url_jobs, article_results):
    """
    Consummer for urls and Producer for articles
    """
    cp = mp.current_process()
    abuyun_proxy = gen_abuyun_proxy()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Article Process pid is %d" % (cp.pid)
        merged_url = url_jobs.get()
        word, s_url, w_url = merged_url.split("|")
        wetchat_result = get_article_info(s_url, w_url, search_word=word, proxy=abuyun_proxy)
        article_results.put(wetchat_result['data'])  # generate article data and put into queue
        url_jobs.task_done()
        time.sleep(0.02)
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Article Process %d finished" % (cp.pid)


def article_db_writer(article_results):
    """
    Consummer for articles
    """
    cp = mp.current_process()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Article Process pid is %d" % (cp.pid)
        with connect_database() as cursor:
            # using try-with-recources, auto-commit
            article_record = article_results.get()
            write_status = write_article_into_db(cursor, article_record)
            article_results.task_done()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Article Process %d finished" % (cp.pid)

def topic_db_writer(topic_results):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Topics Process pid is %d" % (cp.pid)
        with connect_database() as cursor:
            topic_record = topic_results.get()
            write_status = write_topic_into_db(cursor, topic_record)
            topic_results.task_done()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Topics Process %d finished" % (cp.pid)


def create_processes(func, args, concurrency):
    for _ in range(concurrency):
        sub_proc = mp.Process(target=func, args=args)
        sub_proc.daemon = True
        sub_proc.start()


def add_topic_jobs(target, start_date):
    todo = 0
    try:
        conn = connect_database()
        if not conn:
            return False
        list_of_kw = read_topics_from_db(conn.cursor(), start_date)
        for kw in list_of_kw:
            todo += 1
            target.put(kw)
    except mdb.OperationalError as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        traceback.print_exc()
    finally:
        conn.close()
        return todo

def run_all_worker():
    try:
        # Producer is on !!!
        topic_jobs = mp.JoinableQueue()
        url_jobs = mp.JoinableQueue()
        topic_results = mp.JoinableQueue()
        article_results = mp.JoinableQueue()
        create_processes(wxurl_generator, (topic_jobs, url_jobs, topic_results), 1)
        create_processes(wxarticle_generator, (url_jobs, article_results), 6)
        create_processes(topic_db_writer, (topic_results,), 1)
        create_processes(article_db_writer, (article_results, ), 6)

        seven_days_ago = (dt.today() - timedelta(6)).strftime("%Y-%m-%d")
        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        num_of_topics = add_topic_jobs(target=topic_jobs, start_date=seven_days_ago)
        print "<"*10, "There are %d topics to process" % num_of_topics, ">"*10
        topic_jobs.join()
        
        if topic_jobs.empty():
            print "+"*10, "topic_jobs is empty"
        if url_jobs.empty():
            print "+"*10, "url_jobs is empty"
        if topic_results.empty():
            print "+"*10, "topic_results is empty"
        if article_results.empty():
            print "+"*10, "article_results is empty"
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"


if __name__=="__main__":
    print "\n" + "Began Scraping time is : %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Total Scrping Time Consumed : %d seconds" % (time.time() - start), "*"*10
