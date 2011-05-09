Test Postgres Databases
=======================

Programatically creating a cluster
----------------------------------

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
