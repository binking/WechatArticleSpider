import traceback
import MySQLdb as mdb

a = dict(a=1, b=1)

try:
    c = a['c']
    raise mdb.ProgrammingError
except KeyError as e:
    print dir(e)
    print e
    print 'Hey'
    print e.message
    traceback.print_exc()
    a = traceback.format_exc()
    print a
except mdb.ProgrammingError as e:
    print e
    print 'Hey'
    print e.message
    print 'Tracing back'
    traceback.print_exc()
except Exception as e:
    print e
    print 'Hey'
    print e.no
    print e.message
    print 'Tracing back'
    traceback.set_trace()

