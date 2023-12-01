from pymilvus.exceptions import MilvusException

from client.common.common_type import DefaultValue as dv
from client.common.common_type import Error, CheckTasks
import client.check.param_check as pc

from utils.util_log import log


class ResponseChecker:
    def __init__(self, response: dict, func_name, check_task, check_items, is_succ=True, **kwargs):
        self.response = response[0]  # response of api request
        self.func_name = func_name  # api function name
        self.check_task = check_task  # task to check response of the api request
        self.check_items = check_items  # check items and expectations that to be checked in check task
        self.succ = is_succ  # api responses successful or not
        self.kwargs = kwargs

    def run(self):
        """
        Method: start response checking for milvus API call
        """
        result = True
        if self.check_task is None:
            # Interface normal return check
            result = self.assert_succ(self.succ, True)

        elif self.check_task is CheckTasks.assert_result:
            result = self.assert_result(self.succ, True)

        elif self.check_task == CheckTasks.err_res:
            # Interface return error code and error message check
            result = self.assert_exception(self.response, self.succ, self.check_items)

        elif self.check_task == CheckTasks.ccr:
            # Connection interface response check
            result = self.check_value_equal(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.ignore_check:
            result = self.succ

        # Add check_items here if something new need verify

        return result

    def assert_result(self, actual, expect):
        if actual is not expect:
            log.error("[CheckFunc] {0} request check failed, response:{1}".format(self.func_name, self.response))
        return actual is expect

    # @staticmethod
    def assert_succ(self, actual, expect):
        if actual is not expect:
            log.error("[CheckFunc] {0} request check failed, response:{1}".format(self.func_name, self.response))
        assert actual is expect
        return actual is expect

    @staticmethod
    def assert_exception(res, actual=True, error_dict=None):
        assert actual is False
        assert len(error_dict) > 0

        if isinstance(res, MilvusException):
            assert res.code == error_dict[dv.err_code] or error_dict[dv.err_msg] in res.message
        else:
            log.error("[CheckFunc] Response of API is not an error: %s" % str(res))
            assert False

        return True

    @staticmethod
    def check_value_equal(res, func_name, params):
        """ check response of connection interface that result is normal """

        if func_name == "list_connections":
            if not isinstance(res, list):
                log.error("[CheckFunc] Response of list_connections is not a list: %s" % str(res))
                assert False

            list_content = params.get(dv.list_content, None)
            if not isinstance(list_content, list):
                log.error("[CheckFunc] Check param of list_content is not a list: %s" % str(list_content))
                assert False

            new_res = pc.get_connect_object_name(res)
            assert pc.list_equal_check(new_res, list_content)

        if func_name == "get_connection_addr":
            dict_content = params.get(dv.dict_content, None)
            assert pc.dict_equal_check(res, dict_content)

        if func_name == "connect":
            pass
            # class_obj = Connect_Object_Name
            #  res_obj = type(res).__name__
            #  assert res_obj == class_obj

        if func_name == "has_connection":
            value_content = params.get(dv.value_content, False)
            res_obj = res if res is not None else False
            assert res_obj == value_content

        return True
