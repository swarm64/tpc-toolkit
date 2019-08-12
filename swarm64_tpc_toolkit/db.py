
import logging
import time

from collections import namedtuple
from enum import Enum
from urllib.parse import urlparse

from .dbconn import DBConn

import psycopg2

LOG = logging.getLogger()
Timing = namedtuple('Timing', ['start', 'stop', 'status'])


class Status(Enum):
    OK = 0
    TIMEOUT = 1
    ERROR = 2


class DB:
    def __init__(self, dsn):
        self.dsn = dsn
        dsn_url = urlparse(dsn)
        self.dsn_pg_db = f'{dsn_url.scheme}://{dsn_url.netloc}/postgres'

    def apply_config(self, config):
        with DBConn(self.dsn_pg_db) as conn:
            for key, value in config.items():
                conn.cursor.execute(f'ALTER SYSTEM SET {key} = $${value}$$')

            conn.cursor.execute('SELECT pg_reload_conf()')

    def reset_config(self):
        with DBConn(self.dsn_pg_db) as conn:
            conn.cursor.execute('ALTER SYSTEM RESET ALL')
            conn.cursor.execute('SELECT pg_reload_conf()')

    def run_query(self, sql, timeout):
        status = Status.ERROR
        with DBConn(self.dsn, statement_timeout=timeout) as conn:
            try:
                start = time.time()
                conn.cursor.execute(sql)
                if conn.cursor.description is not None:
                    query_result = conn.cursor.fetchall()
                else:
                    query_result = None
                status = Status.OK

            except psycopg2.extensions.QueryCanceledError:
                status = Status.TIMEOUT

            except (psycopg2.InternalError, psycopg2.Error):
                LOG.exception('Ignoring psycopg2 Error')

            finally:
                stop = time.time()

            return Timing(start=start, stop=stop, status=status), query_result
