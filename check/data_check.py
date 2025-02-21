import re

from commons.streaming_read import StreamRead
from db_client.client_db import Database_Client
from utils.util_log import log


class DataCheck:
    def __init__(self, file_path="", interval=600, tags: dict = {}, old_version_format=True):
        """
        sync_report = False : report data after finished test
        old_version_format: True for go bench
        """
        self.old_version_format = old_version_format
        self._format = self.data_parser_format(old_version_format=old_version_format)
        self.default_tags = tags
        self.sync_report = True
        self.db_client = Database_Client
        self.read_client = StreamRead(file_path=file_path, interval=interval)

    @staticmethod
    def report_data_format(method, api, reqs, fails, _avg, _min, _max, _median, req, failures, tp_99: float = None):
        """
        :param method: locust or go
        :param api: report API
        :param reqs: the total number of api requests
        :param fails: the total number of api failed requests
        :param _avg: average response time of interface within statistical interval
        :param _min: minimum response time of interface within statistical interval
        :param _max: maximum response time of interface within statistical interval
        :param _median: median response time of interface within statistical interval
        :param req: the total number of api requests within statistical interval
        :param failures: the total number of api failed requests within statistical interval
        :param tp_99: TP99 response time of interface within statistical interval

        :return: tag, fields
        """
        _f = {'reqs': reqs, 'fails': fails, 'rt_avg_ms': _avg, 'rt_min_ms': _min, 'rt_max_ms': _max,
              'rt_median_ms': _median, 'req_s': req, 'failures_s': failures}
        if tp_99 is not None:
            _f.update({'TP99': tp_99})

        return {'method': method, 'api_name': api}, _f

    @staticmethod
    def data_parser_format(old_version_format=True):
        _data_time = r'\[\s{0,10}\d+-\d+-\d+\s+\d+:\d+:\d+.\d+\s{0,10}\]'
        _log_level = r'\[\s+INFO\]\s+-\s+'
        _name = r'[a-z]+\s+[a-zA-Z0-9._-]+\s+'
        _int = r'\d+'
        _int_percentage = r'\d+\(\d+\.\d+\%\)'
        _float = r'\d+\.\d+'
        _space = r'\s+'
        _vertical_bar = r'\|'

        # old version
        old_format = _data_time + _log_level + _name + _int + _space + _int_percentage + _space + _vertical_bar + _space
        old_format += _float + _space + _float + _space + _float + _space + _float + _space + _float + _space
        old_format += _vertical_bar + _space + _float + _space + _float + _space

        # new version
        _data_time = r'\[\d+-\d+-\d+\s+\d+:\d+:\d+.\d+\s+-\s+'
        _log_level = r'INFO\s+-\s+[a-zA-Z0-9]+\]:\s+'
        new_format = _data_time + _log_level + _name + _int + _space + _int_percentage + _space + _vertical_bar + _space
        new_format += _int + _space + _int + _space + _int + _space + _int + _space + _vertical_bar + _space
        new_format += _float + _space + _float + _space
        return old_format if old_version_format else new_format

    def data_read(self, content: str) -> list:
        return re.findall(re.compile(self._format, re.I), content)

    def parser_content(self, str_content: str):
        if self.old_version_format:
            dt = str_content.split(']')[0].split('[')[-1]
            k = str_content.split('-')[-1].split()
            if len(k) == 13:
                _params = [k[0], k[1], int(k[2]), int(str(k[3]).split('(')[0]), float(k[5]), float(k[6]), float(k[7]),
                           float(k[8]), float(k[11]), float(k[12]), float(k[9])]
                return dt, k, _params
        else:
            _dt = str_content.split('[')[-1].split()
            dt = _dt[0] + " " + _dt[1]
            k = str_content.split(':')[-1].split()
            if len(k) == 12:
                _params = [k[0], k[1], int(k[2]), int(str(k[3]).split('(')[0]), float(k[5]), float(k[6]), float(k[7]),
                           float(k[8]), float(k[10]), float(k[11])]
                return dt, k, _params
        return None, None, None

    def data_parser(self, content: str):
        _contents = self.data_read(content)
        for _c in _contents:
            dt, k, _p = self.parser_content(_c)
            if dt is None:
                continue
            try:
                _time_ = dt.split(' ')
                _time = _time_[0] + 'T' + _time_[-1].split('.')[0].split(',')[0] + 'Z'

                tags, fields = self.report_data_format(*_p)
                # update tag
                tags.update(self.default_tags)
                self.db_client.influx_insert(tags=tags, fields=fields, time=_time)
            except Exception as e:
                raise Exception("[DataCheck] Parser report data raise error: {0}, content:{1}".format(e, _c))

    def start_stream_read(self, sync_report=True):
        self.sync_report = sync_report
        if self.sync_report:
            log.info("[DataCheck] Starting sync read data and report.")
            self.read_client.tick_read_incremental_file(callable_object=self.data_parser)

    def finish_stream_read(self, block_size=65536):
        if self.sync_report:
            self.read_client.final_read_incremental_file(callable_object=self.data_parser)
            log.info("[DataCheck] Finished sync read data and report.")
        else:
            log.info("[DataCheck] Starting async read data and report.")
            loop_contents = self.read_client.streaming_read_file(block_size=block_size)
            while True:
                _content = next(loop_contents)
                if _content == "":
                    break
                for _c in _content:
                    self.data_parser(_c)
            log.info("[DataCheck] Finished async read data and report.")
