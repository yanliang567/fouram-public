import traceback
from influxdb import InfluxDBClient

from db_client.db_base import ClientBase
from utils.util_log import log
from commons.common_func import dict_recursive_key


def influxdb_v1_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                log.error("[InfluxDBV1 Exception] %s" % str(traceback.format_exc()))
                return False

        return inner_wrapper

    return wrapper


class ClientInfluxDBV1(ClientBase):
    """ClientInfluxDBV1 is client for InfluxDB v1.8"""

    @influxdb_v1_try_catch()
    def __init__(self, host='localhost', port=8086, username='qa', password='password', database="fouram",
                 measurement="fouramf", timeout=10000):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.measurement = measurement

        self.client = InfluxDBClient(host=self.host, port=self.port, username=self.username, password=self.password,
                                     timeout=timeout)

        # check db
        self.create_db()

    @influxdb_v1_try_catch()
    def query(self, query, bind_params=None, database=None, out_put=False, format_output=True):
        """
        query = 'select Float_value from cpu_load_short;'
        or:
        query = 'select Int_value from cpu_load_short where host=$host;'
        bind_params = {'host': 'server01'}
        """

        database = database or self.database
        res = self.client.query(query, bind_params=bind_params, database=database)

        if out_put:
            log.debug("[InfluxDBV1 API] The result of query is: {0}".format(res))

        if format_output and len(res):
            self.query_content_format(res)

        return res

    @influxdb_v1_try_catch()
    def insert(self, measurement=None, tags=None, fields=None, time=None, database=None):
        database = database or self.database
        measurement = measurement or self.measurement
        insert_json = self.insert_json_body(measurement=measurement, tags=tags, fields=fields, time=time)

        log.debug("[InfluxDBV1 API] Insert data: {0}, DB name:{1}, measurement:{2}".format(
            insert_json, self.database, self.measurement))
        res = self.client.write_points(insert_json, database=database)
        return res

    @influxdb_v1_try_catch()
    def insert_define_data(self, insert_json, database=None):
        database = database or self.database
        log.debug("[InfluxDBV1 API] Insert data: {0} ".format(insert_json))
        res = self.client.write_points(insert_json, database=database)
        return res

    @staticmethod
    def query_content_format(query_content):
        q_c = query_content.raw["series"][0]

        # log.debug content for query
        log.debug("DATABASE: {0}".format(q_c['name']))

        str_ = " %20s "
        column_len = " %30s "
        columns = ()

        for i in range(len(q_c['columns']) - 1):
            column_len += str_

        for i in q_c['columns']:
            columns = columns + (i,)

        log.debug(column_len % columns)
        values = q_c['values']
        for value in values:
            v_c = ()
            for v in value:
                v_c = v_c + (v,)
            log.debug(column_len % v_c)

    @staticmethod
    def insert_json_body(measurement='', tags=None, fields=None, time=None):
        """
        json_body = [
            {
                "measurement": "cpu_load_short",
                "tags": {
                    "host": "server01",
                    "region": "us-west"
                },
                "time": "2009-11-10T23:00:00Z",
                "fields": {
                    "Float_value": 0.64,
                    "Int_value": 3,
                    "String_value": "Text",
                    "Bool_value": True
                }
            }
        ]
        """
        if measurement == '' or fields is None:
            log.error("[InfluxDBV1 API] measurement {0} and fields {1} cannot be empty.".format(measurement, fields))
            return []

        if not isinstance(fields, dict):
            log.error("[InfluxDBV1 API] feilds:{0} should be a dictionary type.".format(type(fields)))
            return []

        json_ = {
            "measurement": measurement,
            "tags": tags,
            "time": time,
            "fields": fields
        }

        return [dict_recursive_key(json_)]

    @influxdb_v1_try_catch()
    def delete(self, database=None, measurement=None, tags=None):
        return self.client.delete_series(database=database, measurement=measurement, tags=tags)

    @influxdb_v1_try_catch()
    def check_connect_db(self):
        return self.client.ping()

    @influxdb_v1_try_catch()
    def list_db(self, out_put=False):
        db_ = []

        res = self.client.get_list_database()
        if out_put:
            log.debug("[InfluxDBV1 API] List databases in influxDB: {0}".format(res))

        for i in res:
            db_.append(i["name"])
        if out_put:
            log.debug("[InfluxDBV1 API] All databases in influxDB: {0}".format(db_))
        return db_

    @influxdb_v1_try_catch()
    def create_db(self):
        self.client.ping()
        if self.database not in self.list_db():
            self.client.create_database(self.database)
            if self.database not in self.list_db():
                log.error("[InfluxDBV1 API] Create database {0} failed, please check manually.".format(self.database))

    @influxdb_v1_try_catch()
    def close_connect(self):
        self.client.close()
        log.debug(f"[InfluxDBV1 API] Server closed the connection, host: {self.host}, port: {self.port}")
