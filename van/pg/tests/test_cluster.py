from __future__ import print_function
import unittest
import doctest
import os
from testresources import ResourcedTestCase, OptimisingTestSuite

from van.pg import DatabaseManager
import psycopg2

try:
    import transaction
except ImportError:
    transaction = None

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
        self.assertFalse(self.resources[0][1].isDirty())
        self.conn.commit()
        self.assertTrue(self.resources[0][1].isDirty())
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM foo")
        self.assertEqual(cur.fetchall(), [(1, )])
        
    test_wrapper_isolation_repeat = test_wrapper_isolation # repeat test to check isolation

class TestDirtyOnCommit(ResourcedTestCase):
    
    resources = (('db', DatabaseManager(template=TEMPLATE_DB, dirty_on_commit=True)),
                 ('db2', DatabaseManager(template=TEMPLATE_DB)),
                 )

    def test_transaction_commit_dirties(self):
        if transaction is None:
            print("Skipping test requiring transaction")
        dbm_to_dirty = self.resources[0][1]
        dbm_keep_clean = self.resources[1][1]
        self.assertFalse(dbm_to_dirty.isDirty())
        self.assertFalse(TEMPLATE_DB.isDirty())
        self.assertFalse(dbm_keep_clean.isDirty())
        transaction.commit()
        self.assertTrue(dbm_to_dirty.isDirty())
        self.assertFalse(TEMPLATE_DB.isDirty())
        self.assertFalse(dbm_keep_clean.isDirty())
        

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    # test test_resources without optimization
    suite.addTest(tests)
    # and again with optimization
    suite.addTest(OptimisingTestSuite(tests))
    # Also test our documentation
    suite.addTest(doctest.DocFileSuite(os.path.join('..', 'README.txt')))
    return suite
