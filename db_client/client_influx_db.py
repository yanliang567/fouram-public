import traceback
from datetime import datetime, timedelta

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from db_client.db_base import ClientBase
from utils.util_log import log


def influxdb_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                log.error("[InfluxDB Exception] %s" % str(traceback.format_exc()))
                return False

        return inner_wrapper

    return wrapper


class ClientInfluxDB(ClientBase):
    """ClientInfluxDB is client for InfluxDB v2."""

    @influxdb_try_catch()
    def __init__(self, url='http://localhost:8086', token="", org='primary', bucket='fouram', measurement='fouramf',
                 timeout=10000):
        super().__init__()
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.measurement = measurement

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=timeout)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()
        self.buckets_api = self.client.buckets_api()

        # check bucket
        self.create_bucket()

    @influxdb_try_catch()
    def query(self, bucket=None, org=None, measurement=None, tag=None, field=None, time="30d", query_content=None,
              out_put=False):
        bucket, org, measurement = self.params_value(bucket, org, measurement)

        if query_content is None:
            query_content = 'from(bucket: "%s") |> range(start: -%s)' % (bucket, time)
            query_content += ' |> filter(fn: (r) => r["_measurement"] == "%s")' % measurement
            if tag:
                query_content += ' |> filter(fn: (r) => r["tag"] == "%s")' % tag
            if field:
                query_content += ' |> filter(fn: (r) => r["_field"] == "%s")' % field

        log.debug("[InfluxDB API] Query content： %s" % query_content)
        result = self.query_api.query(org=org, query=query_content)
        return self.parse_query_results(result, out_put)

    @influxdb_try_catch()
    def insert(self, bucket=None, org=None, measurement=None, tags=None, fields=None, time=None):
        bucket, org, measurement = self.params_value(bucket, org, measurement)

        p = Point(measurement)

        if tags is not None and isinstance(tags, dict):
            for key, value in tags.items():
                p.tag(key, value)

        if fields is not None and isinstance(fields, dict):
            for key, value in fields.items():
                p.field(key, value)

        if time is not None and isinstance(time, str):
            p.time(time)

        log.debug("[InfluxDB API] Insert data tags:%s, fields:%s, into measurement:%s bucket:%s, time: %s"
                  % (p._tags, p._fields, p._name, self.bucket, p._time))
        self.write_api.write(bucket=bucket, org=org, record=p)

    @influxdb_try_catch()
    def delete(self, bucket=None, org=None, predicate='', time=None):
        """
        e.g.:
        time = {"seconds": 1} / {"minutes": 1} / {"days": 1}
        """
        bucket, org, measurement = self.params_value(bucket, org)

        _time = datetime.now() - timedelta(**time) if time is not None else datetime.now()
        log.debug("[InfluxDB API] Start deleting data in the database: bucket:%s, org:%s, time:%s, predicate:%s "
                  % (bucket, org, str(time), predicate))
        self.delete_api.delete(start=_time, stop=datetime.now(), predicate=predicate, bucket=bucket, org=org)

    def params_value(self, bucket=None, org=None, measurement=None):
        bucket = self.bucket if bucket is None else bucket
        org = self.org if org is None else org
        measurement = self.measurement if measurement is None else measurement
        return bucket, org, measurement

    @staticmethod
    def parse_query_results(result, out_put=False):
        results = []
        for table in result:
            for r in table.records:
                results.append((r.get_start(), r.get_stop(), r.get_time(), r.get_value(), r.get_field(),
                                r.get_measurement()))
        if out_put:
            log.debug("[InfluxDB API] Query result: (_start, _stop, _time, _value, _filed, _measurement)")
            for res in results:
                log.debug(res)
            log.debug("[InfluxDB API] Query finished.")
        return results

    @influxdb_try_catch()
    def create_bucket(self):
        if self.bucket not in [i.name for i in self.buckets_api.find_buckets().buckets]:
            self.buckets_api.create_bucket(bucket_name=self.bucket, org=self.org)
            if self.bucket not in [i.name for i in self.buckets_api.find_buckets().buckets]:
                log.error("[InfluxDB API] Create bucket {0} failed, please check manually.".format(self.bucket))

    @influxdb_try_catch()
    def close_connect(self):
        self.client.close()
        log.debug(f"[InfluxDB API] Server closed the connection, url: {self.url}, org: {self.org}")
