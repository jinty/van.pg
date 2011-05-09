import unittest
import doctest
import os
from testresources import ResourcedTestCase, OptimisingTestSuite

from van.pg import DatabaseManager
import psycopg2

def init_db(db):
    conn = psycopg2.connect(host=db.host, database=db.database)
    cur = conn.cursor()
    cur.execute("CREATE TABLE foo (bar INTEGER);")
    conn.commit()
    conn.close()

TEMPLATE_DB = DatabaseManager(initialize_sql=init_db)

class TestTemplateDB(ResourcedTestCase):

    resources = (('db', DatabaseManager(template=TEMPLATE_DB)),
                 )

    def setUp(self):
        ResourcedTestCase.setUp(self)
        conn = psycopg2.connect(host=self.db.host, database=self.db.database)
        self.conn = self.db.dirty_on_commit_wrapper(conn)

    def tearDown(self):
        # NOTE: must close connections or dropping databases fails
        self.conn.close()
        ResourcedTestCase.tearDown(self)

    def test_wrapper_isolation(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO foo VALUES (1);")
        self.conn.commit()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM foo")
        self.assertEquals(cur.fetchall(), [(1, )])
        
    test_wrapper_isolation_repeat = test_wrapper_isolation # repeat test to check isolation
        

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    # test test_resources without optimization
    suite.addTest(tests)
    # and again with optimization
    suite.addTest(OptimisingTestSuite(tests))
    # Also test our documentation
    suite.addTest(doctest.DocFileSuite(os.path.join('..', 'README.txt')))
    return suite
