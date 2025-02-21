import traceback
import pymysql

from db_client.db_base import ClientBase
from utils.util_log import log


def mysql_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                log.error("[MySql Exception] %s" % str(traceback.format_exc()))
                return False

        return inner_wrapper

    return wrapper


class ClientMySql(ClientBase):
    """ ClientMySql is client for MySql v5.7"""

    def __init__(self, host='http://localhost', port=3306, user="", password='primary', database=None,
                 charset="utf8", cursorclass=pymysql.cursors.DictCursor, timeout=600):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = pymysql.connect(host=host, port=port, user=user, password=password, database=database,
                                      charset=charset, cursorclass=cursorclass, connect_timeout=timeout,
                                      read_timeout=timeout, write_timeout=timeout)

    def query(self, sql, args=None):
        log.debug("[ClientMySql] Query sql:{0}, args:{1}".format(sql, args))
        res = self.execute(query=sql, args=args)
        log.debug("[ClientMySql] Query result: {0}".format(res))
        return res

    def insert(self, sql, args=None):
        log.debug("[ClientMySql] Insert sql:{0}, args:{1}".format(sql, args))
        return self.execute(query=sql, args=args)

    def update(self, sql, args=None):
        log.debug("[ClientMySql] Update sql:{0}, args:{1}".format(sql, args))
        return self.execute(query=sql, args=args)

    def delete(self, sql, args=None):
        log.debug("[ClientMySql] Delete sql:{0}, args:{1}".format(sql, args))
        return self.execute(query=sql, args=args)

    def execute(self, query, args=None):
        try:
            cursor = self.client.cursor()
            cursor.execute(query=query, args=args)
            result = cursor.fetchall()
            # client is not autocommit by default. So you must commit to save your changes.
            self.client.commit()
        except Exception as e:
            self.client.rollback()
            log.error("[ClientMySql] Cannot execute sql:{0}, raise error:{1}".format(query, e))
        else:
            return result

    def close_connect(self):
        self.client.close()
        log.debug(f"[ClientMySql] Server closed the connection, host: {self.host}, port: {self.port}")
