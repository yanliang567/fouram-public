from datetime import datetime
import time
import random


from parameters.input_params import param_info


class ClientMetric:

    def __init__(self, test_case_type: str = "", test_case_name: str = "", test_case_params: dict = {},
                 client_version: str = "", client_type: str = None):
        self.test_case_type = test_case_type
        self.test_case_name = test_case_name
        self.test_case_params = test_case_params

        self.run_id = param_info.run_id or self.gen_id()

        self.datetime = str(datetime.now())
        self.client_version = client_version
        self.client_type = client_type

    def update(self, test_case_type: str = "", test_case_name: str = "", test_case_params: dict = {}):
        self.test_case_type = test_case_type or self.test_case_type
        self.test_case_name = test_case_name or self.test_case_name
        self.test_case_params = test_case_params or self.test_case_params

        self.client_version = self.client_version or param_info.client_version
        self.client_type = self.client_type or param_info.client_type

    def clear_property(self):
        self.test_case_type = ""
        self.test_case_name = ""
        self.test_case_params = {}

        self.run_id = param_info.run_id or self.gen_id()
        self.datetime = str(datetime.now())

    @staticmethod
    def gen_id():
        _date = "{:%Y%m%d}".format(datetime.now())
        _timestamp = str(int(time.time()))[-4:]
        _random = str(int(random.randint(1000, 9999)))
        return int(_date + _timestamp + _random)
