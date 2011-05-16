"""Postgres Testing Support for other packages.
"""

import os
import threading
import gc
import errno
import shutil
import tempfile
from subprocess import Popen, STDOUT, PIPE

from testresources import TestResourceManager

def _pg_run(args, env=None, stdout=PIPE):
    try:
        p = Popen(args, env=env, stdout=stdout, stderr=STDOUT)
        stdout, _ = p.communicate()
        if p.returncode != 0:
            raise Exception("command failed: %s\nOUTPUT:\n%s" % (' '.join(args), stdout))
    except OSError, exc:
        if exc.errno != errno.ENOENT:
            raise
        # carefully convert a common exception message into something more informative
        raise Exception("""Could not find the binary %s. This is probably because
postgresql executables are not on the path.Try setting the PATH variable
to include the postgresql executables. On Debian Linux with PostgreSQL this
you could run PATH=/usr/lib/postgresql/8.3/bin:$PATH""" % args[0])
    return stdout

class Cluster(object):
    """This is an object that manages a postgres cluster.

    The cluster is created in a temporary directory.
    """

    dbdir = None
    started = False
    _db_counter = 0

    @property
    def host(self):
        return self.dbdir

    def initdb(self):
        assert self.dbdir is None
        # setup template
        dbdir = tempfile.mkdtemp()
        new_env = os.environ.copy()
        new_env['PGOPTIONS'] = '-F'
        _pg_run(['initdb', '-E', 'utf-8', '-D', dbdir, '-A', 'trust'], env=new_env)
        self.dbdir = dbdir

    def cleanup(self):
        if self.started:
            self.stop()
        if self.dbdir is not None:
            shutil.rmtree(self.dbdir)
            self.dbdir = None

    def __del__(self):
        self.cleanup()

    def start(self):
        assert not self.started
        assert self.dbdir is not None
        new_env = os.environ.copy()
        new_env['PGHOST'] = self.dbdir
        args = ['pg_ctl', 'start', '-w', '-t', '10', '-s', '-D', self.dbdir,
                '-o', "-k %s -F -h '' --log_min_messages=FATAL" % self.dbdir]
        _pg_run(args, env=new_env, stdout=None) # fails with stdout=PIPE, never returns
        self.started = True

    def stop(self):
        assert self.started
        os.environ.copy()
        new_env = os.environ.copy()
        new_env['PGHOST'] = self.dbdir
        args = ['pg_ctl', 'stop', '-w', '-m', 'fast', '-s', '-D', self.dbdir]
        _pg_run(args, env=new_env)
        self.started = False

    def createdb(self, template=None):
        assert self.started
        self._db_counter += 1
        dbname = 'test_db%s' % self._db_counter
        args = ['createdb', '-h', self.dbdir, dbname]
        if template is not None:
            args.extend(['--template', template])
        _pg_run(args)
        return dbname

    def dropdb(self, dbname):
        assert self.started
        args = ['dropdb', '-h', self.dbdir, dbname]
        _pg_run(args)

class RunningCluster:
    """Representing an extranal cluster that cannot be stopped/started/inited.
    
    Basically, you can point this at an already running database to save time initing the database.
    """

    _db_counter = 0
    _max_prepared = 1

    def __init__(self, host):
        self.host = host
        self._db_preload = {}
        self._is_bg_thread = None
        args = ['psql', '-h', self.host, '-c', 'SELECT datname FROM pg_catalog.pg_database;', '-t', '-A', 'postgres']
        self._existing_dbs = _pg_run(args).splitlines()

    def createdb(self, template=None):
        assert template is None or template.startswith('test_db'), template
        assert template is None or int(template[7:]) <= self._db_counter, (template, self._db_counter)
        dbs = self._db_preload.get(template, [])
        try:
            dbname = dbs.pop()
        except IndexError:
            dbname = None
        if dbname is None:
            dbname = self._next_dbname()
            self._createdb(dbname, template)
        if len(dbs) <= self._max_prepared and template is not None and \
                (self._is_bg_thread is None or not self._is_bg_thread.isAlive()):
            # NOTE: we only prepare templated databases as we can 
            if self._is_bg_thread is not None:
                self._is_bg_thread.join() # make very sure the current bg thread is finished
            preload_dbname = self._next_dbname()
            if template not in self._db_preload:
                self._db_preload[template] = []
            self._is_bg_thread = threading.Thread(target=self._preload, args=(preload_dbname, template, ))
            self._is_bg_thread.start()
        return dbname

    def _preload(self, dbname, template):
        # run in a thread to create the next DB while we run this test
        self._createdb(dbname, template)
        self._db_preload[template].append(dbname)
        self._is_bg_running = False

    def _next_dbname(self):
        # remove database from previously failed run, if necessary
        self._db_counter += 1
        dbname = 'test_db%s' % self._db_counter
        if dbname in self._existing_dbs:
            self.dropdb(dbname)
        return dbname

    def _createdb(self, dbname, template):
        # CAREFUL!!! can be run inside a thread
        args = ['createdb', '-h', self.host, dbname]
        if template is not None:
            args.extend(['--template', template])
        _pg_run(args)

    def dropdb(self, dbname):
        if dbname in self._db_preload:
            # we are dropping a template
            if self._is_bg_thread is not None:
                self._is_bg_thread.join() # make very sure the current bg thread is finished
            # drop any prepared dbs we have for that template
            while self._db_preload[dbname]:
                self.dropdb(self._db_preload[dbname].pop())
            del self._db_preload[dbname]
        args = ['dropdb', '-h', self.host, dbname]
        _pg_run(args)

    def cleanup(self):
        # wait till our background thread finishes
        if self._is_bg_thread is not None:
            self._is_bg_thread.join() # make very sure the current bg thread is finished
        # drop all the preloaded dbs we have around
        for dbname in  self._db_preload:
            while self._db_preload[dbname]:
                self.dropdb(self._db_preload[dbname].pop())

class _Synch(object):
    """Dirties it's layer every time a transaction is succesfully committed."""

    def __init__(self, func):
        self.func = func

    def afterCommitHook(self, status):
        self.func()

    def beforeCompletion(self, txn):
        pass

    def newTransaction(self, txn):
        pass

    def afterCompletion(self, txn):
        txn.addAfterCommitHook(self.afterCommitHook)

class ClusterResource(TestResourceManager):

    setUpCost = 10
    tearDownCost = 5

    def make(self, dependency_resources):
        cluster = Cluster()
        cluster.initdb()
        cluster.start()
        return cluster

    def clean(self, resource):
        resource.cleanup()


class RunningClusterResource(TestResourceManager):

    def make(self, dependency_resources):
        return RUNNING_CLUSTER


_test_db = os.environ.get('VAN_PG_HOST', None)
if _test_db is not None:
    # If a VAN_POSTGRES_TESTDB environmet variable exists, that is used as the database to connect to.
    RUNNING_CLUSTER = RunningCluster(_test_db)
    CLUSTER = RunningClusterResource()
else:
    RUNNING_CLUSTER = None
    CLUSTER = ClusterResource()
del _test_db

class ConnWrapper(object):

    def __init__(self, db, conn):
        object.__setattr__(self, '_conn', conn)
        object.__setattr__(self, '_db', db)

    def commit(self):
        self._db.dirtied()
        return self._conn.commit()

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __setattr__(self, name, val):
        return setattr(self._conn, name, val)


class Database(object):

    def __init__(self, manager, database, cluster):
        self.manager = manager
        self.database = database
        self.cluster = cluster

    def dirtied(self):
        self.manager.dirtied(self)

    def drop(self):
        gc.collect() # Try make sure the gc collects open psycopg2 connections so the next step passes
        self.cluster.dropdb(self.database)

    def dirty_on_commit_wrapper(self, conn):
        """Utility wrapper to dirty the database when committing."""
        return ConnWrapper(self, conn)

    @property
    def host(self):
        return self.cluster.host

class DatabaseManager(TestResourceManager):

    def __init__(self, template=None, initialize_sql=None, dirty_on_commit=False):
        TestResourceManager.__init__(self)
        self.dirty_on_commit = dirty_on_commit
        self.resources = [('cluster', CLUSTER)]
        if template is not None:
            self.resources.append(('template_db', template))
        if initialize_sql is not None:
            self.initialize_sql = initialize_sql

    def initialize_sql(self, database):
        """You can override this function here by subclassing or by passing it to __init__"""
        return None

    def make(self, dependency_resources):
        cluster = dependency_resources['cluster']
        template = dependency_resources.get('template_db', None)
        if template is not None:
            assert cluster is template.cluster
            template = template.database
        resource = Database(self, cluster.createdb(template=template), cluster)
        self.initialize_sql(resource)
        if self.dirty_on_commit:
            self._synch = _Synch(resource.dirtied)
            import transaction
            transaction.manager.registerSynch(self._synch)
        return resource

    def clean(self, resource):
        if self.dirty_on_commit:
            import transaction
            transaction.manager.unregisterSynch(self._synch)
            self._synch = False
            transaction.abort()
        resource.drop()
