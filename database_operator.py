#coding=utf-8
import sys, time
import MySQLdb as mdb
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')

MYSQL_GONE_ERROR = -100
MYSQL_SERVER_HOST = "123.206.64.22"
# MYSQL_SERVER_HOST = "192.168.1.103"
MYSQL_SERVER_PASS = "Crawler20161231"
# MYSQL_SERVER_PASS = "Crawler@test1"
MYSQL_SERVER_USER = 'web'
MYSQL_SERVER_BASE = 'webcrawler'
MYSQL_SERVER_CSET = 'utf8'

def connect_database():
    """
    We can't fail in connect database, which will make the subprocess zoombie
    """
    attempt = 0
    while True:
        seconds = pow(2, attempt)
        try:
            print "Connecting database ..."
            WEBCRAWLER_DB_CONN = mdb.connect(
                host=MYSQL_SERVER_HOST, 
                user=MYSQL_SERVER_USER, 
                passwd=MYSQL_SERVER_PASS, 
                db=MYSQL_SERVER_BASE,
                charset=MYSQL_SERVER_CSET
            )
            return WEBCRAWLER_DB_CONN
        except mdb.OperationalError as e:
            traceback.print_exc()
            print "Hey, Sleep %s seconds cuz we can't connect MySQL..." % seconds
        except Exception as e:
            traceback.print_exc()
            print "Oh, Sleep %s cuz unknown connecting database error." % seconds
        time.sleep(seconds)
        attempt += 1
    

def write_topic_into_db(cursor, topic_info):
    """
    Update two tables: wechatsearchtopic and wechatsearcharticlerelation
    param topic_info(dict): createdate, uri, search_url, search_keyword, urls
    """
    is_succeed = True
    topic_kw = topic_info.get('search_keyword', '')
    topic_uri = topic_info.get('uri', '')
    topic_date = topic_info.get('createdate', '')
    topic_s_url = topic_info.get('search_url', '')

    deprecate_topic = """
        UPDATE wechatsearchtopic
        SET is_up2date='N' 
        WHERE search_keyword=%s
    """
    may_existed_topic = """
        UPDATE wechatsearchtopic
        SET is_up2date='Y' 
        WHERE createdate=%s and search_url=%s and search_keyword=%s
    """
    insert_new_topic = """
        INSERT INTO wechatsearchtopic 
        (uri, search_keyword, createdate, search_url)
        VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(deprecate_topic, (topic_kw, ))  # set N
        is_existed = cursor.execute(may_existed_topic, (topic_date, topic_s_url, topic_kw))
        if not is_existed:
            cursor.execute(insert_new_topic, (topic_uri, topic_kw, topic_date, topic_s_url))
        print "Write topic succeeded..."
    except (mdb.ProgrammingError, mdb.OperationalError) as e:
        traceback.print_exc()
        is_succeed = False
        if 'MySQL server has gone away' in e.message:
            return MYSQL_GONE_ERROR
        elif 'Deadlock found when trying to get lock' in e.message:
            print "Boy, You did not solve dead lock"
        elif e.args[0] in [1064, 1366]:
            print "Hey Hey Hey, Wrong string"
        else:
            print "Other Program or Operation Errors"
    except Exception as e:
        traceback.print_exc()
        is_succeed = False
        print "Write topic failed"
    return is_succeed


def write_article_into_db(cursor, article_info):
    """
    param article_info(dict): 
    """
    is_succeed = True
    createdate = article_info.get('createdate', '')
    search_url = article_info.get('search_url', "")
    article_url = article_info.get('article_url', "")
    article_text = article_info.get('content', '')
    num_of_praise = article_info.get('like_num', -1)
    num_of_read = article_info.get('read_num', -1)

    insert_new_relation = """
        INSERT INTO wechatsearcharticlerelation
        (search_url, search_date, article_url)
        VALUES (%s, %s, %s)
    """
    select_article = """
        SELECT article_url FROM wechatarticle
        WHERE article_url=%s
    """
    insert_new_article = """
        INSERT INTO wechatarticle
        (uri, createdate, article_url, content, read_num, thumb_up_num)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        # Soft-remove old relation and insert new relation
        # prevent same article url from multiple searches
        # cursor.execute(select_relation, (search_url, article_url))
        # relation_to_update = cursor.fetchone()
        # if relation_to_update:  # low prob of executing
        #     cursor.execute(update_relation, (relation_to_update[0], ))
        cursor.execute(insert_new_relation, (search_url, createdate, article_url))
        # conn.commit() # save relation no matter the article is correct or not
        # No need to set is_up2date, cuz temp wx url would be deprecated automatically. T^T

        is_existed = cursor.execute(select_article, (article_url, ))
        if not is_existed:
            # Adjust whether insert new article
            cursor.execute(insert_new_article, (article_url, createdate, article_url,
                article_text, num_of_read, num_of_praise))
        print "Write article succeeded..."

    except (mdb.ProgrammingError, mdb.OperationalError) as e:
        traceback.print_exc()
        is_succeed = False
        if 'MySQL server has gone away' in e.message:
            return MYSQL_GONE_ERROR
        elif 'Deadlock found when trying to get lock' in e.message:
            print "Boy, You did not solve dead lock"
        elif e.args[0] in [1064, 1366]:
            print "Hey Hey Hey, Wrong string"
        else:
            print "Other Program or Operation Errors"
    except Exception as e:
        traceback.print_exc()
        is_succeed = False
        print "Write article Failed..."
    return is_succeed

def read_topics_from_db(cursor):
    """
    Read unchecked topics from database, return list of topics
    """
    todo_topic_list = []
    try:
        # read search keywords from table topicinfo
        cursor.execute("""
            SELECT id, title FROM topicinfo T
            WHERE theme LIKE '新浪微博_热门话题%'
            AND createdate LIKE '%2016-11-%'
        """)
        topicinfo_res = cursor.fetchall()
        for res in topicinfo_res:
            todo_topic_list.append(res[1])
        print "There are %d topics to process" % len(todo_topic_list)
    except Exception as e:
        traceback.print_exc()
        print "Unable read topic from database.."
    return todo_topic_list


"""
WEBCRAWLER_DB_CONN = mdb.connect(
            host = "192.168.1.103", 
            user="web", 
            passwd="Crawler@test1", 
            db="webcrawler",
            charset="utf8"
        )
SELECT id, title FROM topicinfo T
    WHERE theme like '新浪微博_热门话题%' AND NOT EXISTS (
    SELECT id FROM wechatsearchtopic
    WHERE search_keyword=T.title);
WEBCRAWLER_DB_CONN = mdb.connect(
            host = "123.206.64.22", 
            user="web", 
            passwd="Crawler20161231", 
            db="webcrawler",
            charset="utf8"
        )
select_relation = 
        SELECT id FROM wechatsearcharticlerelation
        WHERE search_url=%s AND article_url=%s
    # prevent dead lock, divide SELECT and Update
    update_relation = 
        UPDATE wechatsearcharticlerelation
        SET is_up2date='N'
        WHERE id=%d
    
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