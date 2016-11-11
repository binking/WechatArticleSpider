#coding=utf-8
import sys, time
import MySQLdb as mdb
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')

MYSQL_GONE_ERROR = -100

def connect_database():
    try:
        print "Connecting database ..."
        WEBCRAWLER_DB_CONN = mdb.connect(
            host = "192.168.1.103", 
            user="web", 
            passwd="Crawler@test1", 
            db="webcrawler",
            charset="utf8"
        )
    except Exception as e:
        traceback.print_exc()
        print "Connecting database error."
        return False
    return WEBCRAWLER_DB_CONN

def write_topic_into_db(conn, topic_info):
    """
    Update two tables: wechatsearchtopic and wechatsearcharticlerelation
    param topic_info(dict): createdate, uri, search_url, search_keyword, urls
    """
    is_succeed = 1
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
        cursor = conn.cursor()
        cursor.execute(deprecate_topic)
        cursor.execute(insert_new_topic)
        conn.commit()
        print "Write topic succeeded..."
    except (mdb.ProgrammingError, mdb.OperationalError) as e:
        if 'MySQL server has gone away' in e.message:
            return MYSQL_GONE_ERROR
        else:
            traceback.print_exc()
            print "Other Program or Operation Errors"
    except Exception as e:
        traceback.print_exc()
        is_succeed = 0
        print "Write topic failed"
        conn.rollback()
    finally:
        cursor.close()
    return is_succeed


def write_article_into_db(conn, article_info):
    """
    param article_info(dict): 
    """
    is_succeed = 1
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
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE wechatsearcharticlerelation
            SET is_up2date='N'
            WHERE search_url=%s AND article_url=%s
        """, (search_url, article_url))  # prevent same article url from multiple searches
        cursor.execute(insert_new_relation.format(
            surl=search_url,
            date=article_info.get('createdate', ''), 
            aurl=article_url
        ))  
        # No need to set is_up2date, cuz temp wx url would be deprecated automatically. T^T

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
        conn.commit()
        print "Write article succeeded..."
    except (mdb.ProgrammingError, mdb.OperationalError) as e:
        if 'MySQL server has gone away' in e.message:
            return MYSQL_GONE_ERROR
        else:
            traceback.print_exc()
            print "Other Program or Operation Errors"
    except Exception as e:
        traceback.print_exc()
        is_succeed = 0
        print "Write article Failed..."
        conn.rollback()
    finally:
        cursor.close()
    return is_succeed

def read_topics_from_db(conn):
    """
    Read topics from database, return list of topics
    """
    topic_list = []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT title FROM topicinfo
        """)
        topics = cursor.fetchall()
        for t in topics:
            topic_list.append(t[0])
    except Exception as e:
        traceback.print_exc()
        print "Unable read topic from database.."
    return topic_list


"""
print "Inserted article error, tried to skip content"
        try:
            cursor.execute(insert_new_article.format(
                    uri=article_url,
                    aurl=article_url,
                    date=article_info.get('createdate', ''),
                    rnum=article_info.get('read_num', -1),
                    tnum=article_info.get('like_num', -1)
                ))
        except Exception as e:
            is_succeed = 0
            print "Write article Failed, eventhought skip content..."
"""