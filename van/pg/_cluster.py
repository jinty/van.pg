"""Postgres Testing Support for other packages.
"""

import os
import signal
import time
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
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise
        # carefully convert a common exception message into something more informative
        raise Exception("""Could not find the binary %s. This is probably because
postgresql executables are not on the path.Try setting the PATH variable
to include the postgresql executables. On Debian Linux with PostgreSQL
you could run PATH=/usr/lib/postgresql/X.Y/bin:$PATH""" % args[0])
    return stdout


class RunningCluster(object):
    """Representing an extranal cluster that cannot be stopped/started/inited.

    Basically, you can point this at an already running database to save time initing the database.
    """

    _db_counter = 0
    _max_prepared = 1
    _is_bg_thread = None

    def __init__(self, host):
        self.host = host
        self._db_preload = {}
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
            # NOTE: we only prepare templated databases if we can
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

    def __del__(self):
        self.cleanup()


class Cluster(RunningCluster):
    """This is an object that manages a postgres cluster.

    The cluster is created in a temporary directory.
    """

    dbdir = None
    _postmaster = None
    _db_counter = 0
    _existing_dbs = ()

    def __init__(self):
        self._db_preload = {}

    @property
    def host(self):
        return self.dbdir

    @property
    def started(self):
        return self._postmaster is not None

    def initdb(self):
        assert self.dbdir is None
        # setup template
        dbdir = tempfile.mkdtemp()
        new_env = os.environ.copy()
        new_env['PGOPTIONS'] = '-F'
        _pg_run(['initdb', '-E', 'utf-8', '-D', dbdir, '-A', 'trust'], env=new_env)
        self.dbdir = dbdir

    def cleanup(self):
        super(Cluster, self).cleanup()
        if self._postmaster is not None:
            self.stop()
        if self.dbdir is not None:
            shutil.rmtree(self.dbdir)
            self.dbdir = None

    def start(self):
        assert self._postmaster is None
        assert self.dbdir is not None
        args = ['postgres', '-D', self.dbdir, '-k', self.dbdir, '-F', '-h', '', '--log_min_messages=PANIC']
        self._postmaster = Popen(args)
        timeout = 10 # seconds
        for i in range(timeout * 20):
            try:
                time.sleep(0.05)
                self._postmaster.poll()
                if self._postmaster.returncode is not None:
                    raise Exception("Postmaster died unexpectedly")
                args = ['psql', '-h', self.host, '-c', "SELECT 'YAY';", '-t', '-A', 'postgres']
                p = Popen(args, stdout=PIPE, stderr=PIPE)
                result, psql_err = p.communicate()
                if p.returncode == 0 and b'YAY' in result:
                    break # success
            except:
                self.stop()
                raise
        else:
            self.stop()
            raise Exception('Timed out connecting to postgres: %s' % psql_err)

    def stop(self):
        assert self._postmaster is not None
        self._postmaster.poll()
        if self._postmaster.returncode is None:
            # http://www.postgresql.org/docs/9.1/static/server-shutdown.html
            self._postmaster.send_signal(signal.SIGQUIT)
            self._postmaster.wait()
        self._postmaster = None


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
