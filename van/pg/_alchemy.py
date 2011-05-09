from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import orm

from van.postgres import optimizing_db_layer

def _null(*args, **kw):
    return None
        
def dispose_engine(engine):
    engine.dispose()

def engine_layer(cls):

    def prepare_template_db():
        url = URL('postgres', host=cls.cluster.dbdir, database=cls.template_dbname)
        engine_factory = getattr(cls, 'create_engine', create_engine)
        engine = engine_factory(url)
        conn = engine.raw_connection()
        cur = conn.cursor()
        getattr(cls, 'setup_sql', _null)(cur)
        conn.commit()
        conn.close()
        engine.dispose()
        del engine

    def prepare_db():
        url = URL('postgres', host=cls.cluster.dbdir, database=cls.dbname)
        engine_factory = getattr(cls, 'create_engine', create_engine)
        cls.engine = engine_factory(url)
        currently_registered_mappers = set(orm._mapper_registry.keys())
        cls.mappers = getattr(cls, 'setup_mappers', _null)()
        if cls.mappers is None:
            cls.mappers = set(orm._mapper_registry.keys()) - currently_registered_mappers

    def unprepare_db():
        for m in list(cls.mappers):
            m.dispose()
        cls.mappers = None
        f = getattr(cls, 'dispose_engine', dispose_engine)
        f(cls.engine)
        cls.engine = None

    optimizing_db_layer(cls, _prepare_db=prepare_db, _unprepare_db=unprepare_db, _prepare_template_db=prepare_template_db)
