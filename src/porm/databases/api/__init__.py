import logging
import threading
import uuid
import warnings
from functools import wraps

from porm.errors import InterfaceError, OperationalError, __exception_wrapper__

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logger = logging.getLogger('porm')
logger.addHandler(NullHandler())

# TODO
# explain usage
SENTINEL = object()


class _callable_context_manager(object):
    def __call__(self, fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)

        return inner


def __deprecated__(arg):
    warnings.warn(arg, DeprecationWarning)


# CONNECTION CONTROL.


class _ConnectionState(object):
    def __init__(self, **kwargs):
        super(_ConnectionState, self).__init__(**kwargs)
        # reset
        self._closed = True
        self.conn = None
        self.ctx = []
        self.transactions = []

        self.reset()

    def reset(self):
        self._closed = True
        self.conn = None
        self.ctx = []
        self.transactions = []

    def set_connection(self, conn):
        self.conn = conn
        self._closed = False
        self.ctx = []
        self.transactions = []

    @property
    def closed(self):
        if self._closed:
            return self._closed
        else:
            self.conn.ping(reconnect=True)
            return self._closed


class _ConnectionLocal(_ConnectionState, threading.local):
    pass


class _NoopLock(object):
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


# TRANSACTION CONTROL.


class _transaction(_callable_context_manager):
    def __init__(self, db, lock_type=None):
        self.db = db
        self._lock_type = lock_type

    def _begin(self):
        if self._lock_type:
            self.db.begin(self._lock_type)
        else:
            self.db.begin()

    def commit(self, begin=True):
        self.db.commit()
        if begin:
            self._begin()

    def rollback(self, begin=True):
        self.db.rollback()
        if begin:
            self._begin()

    def __enter__(self):
        if self.db.transaction_depth() == 0:
            self._begin()
        self.db.push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback(False)
            elif self.db.transaction_depth() == 1:
                try:
                    self.commit(False)
                except Exception:
                    self.rollback(False)
                    raise
        finally:
            self.db.pop_transaction()


class _atomic(_callable_context_manager):
    def __init__(self, db, lock_type=None):
        self.db = db
        self._lock_type = lock_type
        self._transaction_args = (lock_type,) if lock_type is not None else ()

    def __enter__(self):
        if self.db.transaction_depth() == 0:
            self._helper = self.db.transaction(*self._transaction_args)
        else:
            self._helper = self.db.savepoint()
        return self._helper.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._helper.__exit__(exc_type, exc_val, exc_tb)


class _savepoint(_callable_context_manager):
    def __init__(self, db, sid=None):
        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = self.sid.join(self.db.quote)

    def _begin(self):
        self.db.execute_sql('SAVEPOINT %s;' % self.quoted_sid)

    def commit(self, begin=True):
        self.db.execute_sql('RELEASE SAVEPOINT %s;' % self.quoted_sid)
        if begin:
            self._begin()

    def rollback(self):
        self.db.execute_sql('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    def __enter__(self):
        self._begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            try:
                self.commit(begin=False)
            except Exception:
                self.rollback()
                raise


class DBApi(_callable_context_manager):

    def __enter__(self):
        if self.is_closed():
            self.connect()
        ctx = self.atomic()
        self._state.ctx.append(ctx)
        ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ctx = self._state.ctx.pop()
        try:
            ctx.__exit__(exc_type, exc_val, exc_tb)
        finally:
            if not self._state.ctx:
                self.close()

    def __init__(
            self, database_name=None, db=None, thread_safe=True, autorollback=False, autocommit=None, autoconnect=True,
            t=None, **config):

        self.autoconnect = autoconnect
        self.autorollback = autorollback
        self.thread_safe = thread_safe
        self.database_name = db or database_name
        if thread_safe:
            self._state = _ConnectionLocal()
            self._lock = threading.Lock()
        else:
            self._state = _ConnectionState()
            self._lock = _NoopLock()

        if autocommit is not None:
            __deprecated__('Porm is learned from Peewee that no longer uses the "autocommit" option, as '
                           'the semantics now require it to always be True. '
                           'Because some database-drivers also use the '
                           '"autocommit" parameter, you are receiving a '
                           'warning so you may update your code and remove '
                           'the parameter, as in the future, specifying '
                           'autocommit could impact the behavior of the '
                           'database driver you are using.')
            self.autocommit = False
        else:
            self.autocommit = autocommit

        self.connect_params = {}
        self.deferred = False
        self.init(conn=t, autocommit=self.autocommit, **config)

    def _connect(self):
        """
        Implement in database api
        :return:
        """
        raise NotImplementedError

    def _initialize_connection(self, conn):
        """

        :param conn:
        :return:
        """
        raise NotImplementedError

    def connect(self, reuse_if_open=False, conn=None):
        with self._lock:
            if self.deferred:
                raise InterfaceError('Error, database must be initialized '
                                     'before opening a connection.')
            if not self._state.closed:
                if reuse_if_open:
                    return False
                raise OperationalError('Connection already opened.')
            self._state.reset()
            with __exception_wrapper__:
                new_conn = conn or self._connect()
                self._state.set_connection(new_conn)
                self._initialize_connection(self._state.conn)
        return True

    @property
    def conn(self):
        if self.is_closed():
            self.connect()
        return self._state.conn

    def init(self, conn=None, **config):
        if not self.is_closed():
            self.close()
        self.connect_params.update(config)
        self.connect(conn=conn)
        self.deferred = not bool(self.conn)

    def session_start(self):
        with self._lock:
            return self.transaction().__enter__()

    def session_commit(self):
        with self._lock:
            try:
                txn = self.pop_transaction()
            except IndexError:
                return False
            txn.commit(begin=self.in_transaction())
            return True

    def session_rollback(self):
        with self._lock:
            try:
                txn = self.pop_transaction()
            except IndexError:
                return False
            txn.rollback(begin=self.in_transaction())
            return True

    def in_transaction(self):
        return bool(self._state.transactions)

    def push_transaction(self, transaction):
        self._state.transactions.append(transaction)

    def pop_transaction(self):
        return self._state.transactions.pop()

    def transaction_depth(self):
        return len(self._state.transactions)

    def top_transaction(self):
        if self._state.transactions:
            return self._state.transactions[-1]

    def atomic(self):
        return _atomic(self)

    def transaction(self):
        return _transaction(self)

    def savepoint(self):
        return _savepoint(self)

    def begin(self):
        if self.is_closed():
            self.connect()

    def commit(self):
        return self._state.conn.commit()

    def rollback(self):
        return self._state.conn.rollback()

    def close(self):
        with self._lock:
            if self.deferred:
                raise InterfaceError('Error, database must be initialized '
                                     'before opening a connection.')
            if self.in_transaction():
                raise OperationalError('Attempting to close database while '
                                       'transaction is open.')
            is_open = not self._state.closed
            try:
                if is_open:
                    with __exception_wrapper__:
                        self._close(self._state.conn)
            finally:
                self._state.reset()
            return is_open

    def _close(self, conn):
        conn.close()

    def is_closed(self):
        return self._state.closed

    def connection(self):
        if self.is_closed():
            self.connect()
        return self._state.conn

    def cursor(self, commit=None):
        if self.is_closed():
            if self.autoconnect:
                self.connect()
            else:
                raise InterfaceError('Error, database connection not opened.')
        return self._state.conn.cursor()

    def execute_sql(self, sql, params=None, commit=SENTINEL):
        logger.debug((sql, params))
        if commit is SENTINEL:
            if self.in_transaction():
                commit = False
            else:
                commit = not sql[:6].lower().startswith('select')

        with __exception_wrapper__:
            cursor = self.cursor(commit)
            try:
                cursor.execute(sql, params or ())
            except Exception:
                if self.autorollback and not self.in_transaction():
                    self.rollback()
                raise
            else:
                if commit and not self.in_transaction():
                    self.commit()
        return cursor
