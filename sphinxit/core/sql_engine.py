# coding=utf-8
from typing import Any, Dict, Type
from typing import NoReturn
from .exceptions import SphinxQLDriverException


_driver_registry = {}  # type: Dict[str, Type[AbstractSqlEngine]]


def register_engine(name):
    def decorator(cls):
        _driver_registry[name] = cls
        return cls
    return decorator


def get_engine_class(name):
    return _driver_registry.get(name)


def list_engine_names():
    return sorted(_driver_registry.keys())


class AbstractSqlEngine(object):
    module = None
    cursors_module = None

    def __init__(self, options):
        # type: (Dict[str, Any]) -> None
        options.setdefault('host', 'localhost')
        options.setdefault('user', None)
        options.setdefault('password', None)
        options.setdefault('db', None)
        options.setdefault('port', 3306)
        options.setdefault('unix_socket', None)
        options.setdefault('connect_timeout', 10)
        options.setdefault('read_default_file', None)
        options.setdefault('use_unicode', True)
        options.setdefault('charset', 'utf8')
        options.setdefault('sql_mode', None)
        options.setdefault('cursorclass', 'Cursor')
        self.options = options

    def get_cursor_class(self, name):
        # type: (str) -> Any
        return getattr(self.cursors_module, name)

    def get_connection(self):
        # type: () -> Any
        raise NotImplementedError

    def execute(self, cursor, query):
        # type: (Any, str) -> Any
        return cursor.execute(query)

    def raise_for_exception(self, e):
        # type: (Exception) -> NoReturn
        raise SphinxQLDriverException(e)


@register_engine('oursql')
class OurSqlEngine(AbstractSqlEngine):
    def __init__(self, options):
        super(OurSqlEngine, self).__init__(options)
        import oursql
        self.module = oursql
        self.cursors_module = oursql

    def get_connection(self):
        # type: () -> Any
        return self.module.connect(
            host=self.options['host'],
            port=self.options['port'],
            db=self.options['db'],
            user=self.options['user'],
            passwd=self.options['password'],
            unix_socket=self.options['unix_socket'],
            connect_timeout=self.options['connect_timeout'],
            read_default_file=self.options['read_default_file'],
            use_unicode=self.options['use_unicode'],
            charset=self.options['charset'],
            # sql_mode=self.options['sql_mode'],
            # cursorclass=self.get_cursor_class(self.options['cursorclass']),
        )

    def execute(self, cursor, query):
        # type: (Any, str) -> Any
        return cursor.execute(query, plain_query=True)

    def raise_for_exception(self, e):
        # type: (Exception) -> NoReturn
        if type(e).__name__ == 'ProgrammingError':
            errno, msg, extra = e
            if errno is not None:
                raise SphinxQLDriverException(msg)


@register_engine('pymysql')
@register_engine('mysqldb')
class PyMySqlEngine(AbstractSqlEngine):
    def __init__(self, options):
        super(PyMySqlEngine, self).__init__(options)
        import pymysql
        import pymysql.cursors
        self.module = pymysql
        self.cursors_module = pymysql.cursors

    def get_connection(self):
        # type: () -> Any
        return self.module.connect(
            host=self.options['host'],
            port=self.options['port'],
            database=self.options['db'],
            user=self.options['user'],
            password=self.options['password'],
            unix_socket=self.options['unix_socket'],
            connect_timeout=self.options['connect_timeout'],
            read_default_file=self.options['read_default_file'],
            use_unicode=self.options['use_unicode'],
            charset=self.options['charset'],
            sql_mode=self.options['sql_mode'],
            cursorclass=self.get_cursor_class(self.options['cursorclass']),
        )


@register_engine('mysqldb1')
class MySqlDbEngine(AbstractSqlEngine):
    def __init__(self, options):
        super(MySqlDbEngine, self).__init__(options)
        import MySQLdb
        import MySQLdb.cursors
        self.module = MySQLdb
        self.cursors_module = MySQLdb.cursors

    def get_connection(self):
        # type: () -> Any
        return self.module.connect(
            host=self.options['host'],
            port=self.options['port'],
            db=self.options['db'],
            user=self.options['user'],
            passwd=self.options['password'],
            unix_socket=self.options['unix_socket'],
            connect_timeout=self.options['connect_timeout'],
            read_default_file=self.options['read_default_file'],
            use_unicode=self.options['use_unicode'],
            charset=self.options['charset'],
            sql_mode=self.options['sql_mode'],
            cursorclass=self.get_cursor_class(self.options['cursorclass']),
        )


@register_engine('cymysql')
class CyMySqlEngine(AbstractSqlEngine):
    def __init__(self, options):
        super(CyMySqlEngine, self).__init__(options)
        import cymysql
        import cymysql.cursors
        self.module = cymysql
        self.cursors_module = cymysql.cursors

    def get_connection(self):
        # type: () -> Any
        return self.module.connect(
            host=self.options['host'],
            port=self.options['port'],
            db=self.options['db'],
            user=self.options['user'],
            passwd=self.options['password'],
            unix_socket=self.options['unix_socket'],
            connect_timeout=self.options['connect_timeout'],
            read_default_file=self.options['read_default_file'],
            use_unicode=self.options['use_unicode'],
            charset=self.options['charset'],
            sql_mode=self.options['sql_mode'],
            cursorclass=self.get_cursor_class(self.options['cursorclass']),
        )


@register_engine('umysqldb')
class UMySqlEngine(AbstractSqlEngine):
    def __init__(self, options):
        super(UMySqlEngine, self).__init__(options)
        import umysqldb
        import umysqldb.cursors
        self.module = umysqldb
        self.cursors_module = umysqldb.cursors

    def get_connection(self):
        # type: () -> Any
        return self.module.connect(
            host=self.options['host'],
            port=self.options['port'],
            database=self.options['db'],
            user=self.options['user'],
            password=self.options['password'],
            unix_socket=self.options['unix_socket'],
            connect_timeout=self.options['connect_timeout'],
            read_default_file=self.options['read_default_file'],
            use_unicode=self.options['use_unicode'],
            charset=self.options['charset'],
            sql_mode=self.options['sql_mode'],
            cursorclass=self.get_cursor_class(self.options['cursorclass']),
        )

