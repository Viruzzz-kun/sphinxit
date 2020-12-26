"""
    sphinxit.core.connector
    ~~~~~~~~~~~~~~~~~~~~~~~

    Implements Sphinxit <-> searchd interaction.

    :copyright: (c) 2013 by Roman Semirook.
    :license: BSD, see LICENSE for more details.
"""

from __future__ import unicode_literals

import logging
import threading
from collections import deque
from contextlib import contextmanager

from typing import Optional

from sphinxit.core.sql_engine import AbstractSqlEngine
from .exceptions import ImproperlyConfigured
from .mixins import ConfigMixin
from .sql_engine import get_engine_class, list_engine_names

logger = logging.getLogger(__name__)


class SphinxConnector(ConfigMixin):

    def __init__(self, config):
        connection_options = {
            'host': '127.0.0.1',
            'port': 9306,
        }
        connection_options.update(config.SEARCHD_CONNECTION)

        self.config = config
        self.connection_options = connection_options

        sql_engine = config.SQL_ENGINE
        engine_class = get_engine_class(sql_engine)
        self.engine = None  # type: Optional[AbstractSqlEngine]
        if not engine_class:
            logger.warning(
                '"{}" is not a registered sql engine. Use one of "{}" for SQL_ENGINE variable'.format(
                    sql_engine,
                    '", "'.join(list_engine_names()),
                )
            )
        else:
            try:
                self.engine = engine_class(connection_options)
            except ImportError as e:
                logger.error('Cannot instantiate engine: "{}"'.format(e))

        self.__connections_pool = deque([])
        self.__local = threading.local()
        self.__conn_lock = threading.Lock()

    def __del__(self):
        self.close_connections()

    def close_connections(self):
        with self.__conn_lock:
            if hasattr(self.__local, 'conn'):
                del self.__local.conn
            for connection in self.__connections_pool:
                try:
                    connection.close()
                except:
                    pass
            self.__connections_pool.clear()

    def _init_connection_pool(self):
        if not self.engine:
            raise ImproperlyConfigured(
                'One of Sql libraries has to be installed to work with searchd'
            )
        if not self.__connections_pool:
            for i in range(getattr(self.config, 'POOL_SIZE', 10)):
                self.__connections_pool.append(self.engine.get_connection())

    @contextmanager
    def get_connection(self):
        with self.__conn_lock:
            self._init_connection_pool()
            local_connection = self.__connections_pool.pop()
            self.__local.conn = local_connection

        yield self.__local.conn

        with self.__conn_lock:
            self.__local.conn = None
            self.__connections_pool.appendleft(local_connection)

    def get_cursor(self, connection):
        cursor_class = self.engine.get_cursor_class('DictCursor')
        return connection.cursor(cursor_class)

    def _normalize_meta(self, raw_result):
        return dict([(x['Variable_name'], x['Value']) for x in raw_result])

    def _normalize_status(self, raw_result):
        return dict([(x['Counter'], x['Value']) for x in raw_result])

    def _execute_batch(self, cursor, sxql_batch):
        total_results = {}

        for sub_ql_pair in sxql_batch:
            subresult = {}
            sub_ql, sub_alias = sub_ql_pair
            self.engine.execute(cursor, sub_ql)
            subresult['items'] = [r for r in cursor]

            if getattr(self.config, 'WITH_META', False):
                meta_ql, meta_alias = 'SHOW META', 'meta'
                self.engine.execute(cursor, meta_ql)
                subresult[meta_alias] = self._normalize_meta(cursor)

            if getattr(self.config, 'WITH_STATUS', False):
                status_ql, status_alias = 'SHOW STATUS', 'status'
                self.engine.execute(cursor, status_ql)
                subresult[status_alias] = self._normalize_status(cursor)

            total_results[sub_alias] = subresult

        return total_results

    def _execute_query(self, cursor, sxql_query):
        self.engine.execute(cursor, sxql_query)
        return cursor.fetchall()

    def execute(self, sxql_query):
        with self.get_connection() as connection:
            cursor = self.get_cursor(connection)
            total_results = {}
            try:
                if isinstance(sxql_query, (tuple, list)):
                    total_results = self._execute_batch(cursor, sxql_query)
                else:
                    total_results = self._execute_query(cursor, sxql_query)
            except Exception as e:
                self.engine.raise_for_exception(e)
            finally:
                cursor.close()

        return total_results
