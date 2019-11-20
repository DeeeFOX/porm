import os
import sys
from copy import deepcopy

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import os

from porm.databases import MyDBApi

logger = logging.getLogger('porm')


def make_db_params(key):
    params = {}
    env_vars = [(part, 'PORM_%s_%s' % (key, part.upper()))
                for part in ('host', 'port', 'user', 'password')]
    for param, env_var in env_vars:
        value = os.environ.get(env_var)
        if value:
            params[param] = int(value) if param == 'port' else value
    return params


MYSQL_PARAMS = make_db_params('MYSQL')


def db_loader(engine, name='porm_test', db_class=None, **params):
    db_params = params
    if db_class is None:
        engine_aliases = {
            MyDBApi: ['mysql'],
        }
        engine_map = dict((alias, _db) for _db, aliases in engine_aliases.items()
                          for alias in aliases)
        if engine.lower() not in engine_map:
            raise Exception('Unsupported engine: %s.' % engine)
        db_class = engine_map[engine.lower()]
    elif issubclass(db_class, MyDBApi):
        db_params = deepcopy(MYSQL_PARAMS)
        db_params.update(params)
    return db_class(name, **db_params)


class QueryLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.queries = []
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.queries.append(record)
