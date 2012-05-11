Test PostgreSQL Databases
=========================

Easy creation of PostgreSQL databases (and clusters) for unit testing.

Dirty Databases
---------------

Test databases take a long time to create. In general you need to be a little
careful when you decide to delete/recreate a test database fixture.

Also, there seems to be no robust way in PostgreSQL of figuring out if a
database was committed to or not.

So van.pg has no choice but to place the responsibility on the you to notify
it when a database is dirty.  If this isn't done properly, test isolation will
be compromised.  It's not ideal, but the best we can do.

One exception is if you consistently use the ``transaction`` package
(http://pypi.python.org/pypi/transaction) to manage database commits.  In this
case you can ask for the resource to be dirtied whenever a transaction is
committed.

Integration with ``testresources``
----------------------------------

The typical way to use these fixtures is via ``testresources``
(http://pypi.python.org/pypi/testresources/):

    >>> from testresources import ResourcedTestCase
    >>> from van.pg import DatabaseManager
    >>> import psycopg2

    >>> def init_db(db):
    ...     conn = psycopg2.connect(host=db.host, database=db.database)
    ...     cur = conn.cursor()
    ...     cur.execute("CREATE TABLE foo (bar INTEGER);")
    ...     conn.commit()
    ...     conn.close()

    >>> class MyTest(ResourcedTestCase):
    ... 
    ...     resources = [('db', DatabaseManager(initialize_sql=init_db))]
    ... 
    ...     def runTest(self):
    ...         conn = psycopg2.connect(host=self.db.host, database=self.db.database)
    ...         cur = conn.cursor()
    ...         cur.execute("INSERT INTO foo VALUES (1);")
    ...         conn.commit()
    ...         cur = conn.cursor()
    ...         cur.execute("SELECT * FROM foo")
    ...         self.assertEqual(cur.fetchall(), [(1, )])
    ...         # NOTE: must close connections or dropping databases fails
    ...         conn.close()
    ...         self.db.dirtied() # we changed the DB, so it needs re-loading
        
Actually run the test:

    >>> from unittest import TextTestRunner
    >>> import sys
    >>> runner = TextTestRunner(stream=sys.stdout)
    >>> runner.run(MyTest()) # doctest: +ELLIPSIS
    .
    ...
    OK
    ...

Using template databases
------------------------

If you need to recreate the same database many times, it can be faster to let
PostgreSQL copy the database from a template database. You can do this by having one DatabaseManager serve as the template for another:

    >>> template_db = DatabaseManager(initialize_sql=init_db)
    >>> class MyTest2(MyTest):
    ...     resources = [('db', DatabaseManager(template=template_db))]

    >>> runner.run(MyTest2()) # doctest: +ELLIPSIS
    .
    ...
    OK
    ...

``transaction`` integration
---------------------------

If the keyword argumen ``dirty_on_commit`` is True, a DatabaseManager will mark
the database as dirtied after every successfull commit made through the
``transaction`` module. This means each test which dirties the database does not
have to manually notify it.

    >>> man = DatabaseManager(dirty_on_commit=True)

If you use this feature, you need to depend on the transaction
(http://pypi.python.org/pypi/transaction) package yourself.

Using an existing database
--------------------------

By default, van.pg creates a new PostgreSQL cluster in a temporary directory
and launches a PostgreSQL daemon. This works most of the time, but is not very
fast.

If you have an already running PostgreSQL cluster, you can tell van.pg to use
it by setting the environment variable VAN_PG_HOST. For example, to run
van.pg's tests against a local PostgreSQL server with it's sockets in
/tmp/pgcluster do:

    $ VAN_PG_HOST=/tmp/pgcluster python setup.py test

WARNING: any databases starting with test_db in the target database are likely
to be dropped.

Closing Connections
-------------------

Be careful to properly close all connections to the database once your test is
done with it. PostgreSQL doesn't allow dropping databases while there are open
connections. This will cause van.pg to error when trying to drop the test
database.

Programatically creating a cluster
----------------------------------

At a lower level, you can also programmatically manipulate your own PostgreSQL cluster.

Initialize the Cluster:
 
    >>> from van.pg import Cluster
    >>> cluster = Cluster()
    >>> cluster.initdb()

Which creates a database in a temporary directory:

    >>> import os
    >>> dbdir = cluster.dbdir
    >>> 'PG_VERSION' in os.listdir(dbdir)
    True
    
Start it:

    >>> cluster.start()

Create/Test a database:

    >>> dbname = cluster.createdb()
    

We can connect to the database:

    >>> import psycopg2
    >>> conn = psycopg2.connect(database=dbname, host=cluster.dbdir)
    >>> cur = conn.cursor()

Twiddle the database to make sure we can do the basics:
    
    >>> cur.execute("CREATE TABLE x (y int)")
    >>> cur.execute("INSERT INTO x VALUES (1)")
    >>> conn.commit()
    >>> cur.execute("SELECT * from x")
    >>> cur.fetchall()[0][0]
    1

Stop the cluster daemon:

    >>> conn.close()
    >>> cluster.stop()
    
Start it again:

    >>> cluster.start()
    >>> conn = psycopg2.connect(database=dbname, host=cluster.dbdir)
    >>> cur = conn.cursor()
    >>> cur.execute("SELECT * from x")
    >>> cur.fetchall()[0][0]
    1

And cleanup:

    >>> conn.close()
    >>> cluster.cleanup()
    >>> cluster.dbdir is None
    True
    >>> os.path.exists(dbdir)
    False

Development
-----------

Development takes place on GitHub:

    http://github.com/jinty/van.pg
