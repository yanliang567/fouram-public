from typing import Union, List

from pymilvus.exceptions import MilvusException

from client.common.common_type import Error, CheckTasks, DefaultValue as dv
import client.check.param_check as pc
from client.check.exception_message import ServerExceptionsMessage

from utils.util_log import log


class InterfaceCheckTasks:
    search = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    hybrid_search = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    query = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    flush = CheckTasks.base_check(CheckTasks.checkIgnoreRateLimit, CheckTasks.checkIgnoreExpectedErrors)
    load = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    release = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    insert = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    upsert = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)
    delete = CheckTasks.base_check(CheckTasks.checkIgnoreExpectedErrors)


def parser_check_items(check_items) -> List[Error]:
    if not isinstance(check_items, (dict, list)):
        raise ValueError(
            f"[parser_check_items] Type of `check_items` is not a dict ot list:{type(check_items)}, {check_items}")

    if isinstance(check_items, dict):
        return [Error(check_items)]

    res = []
    for check_item in check_items:
        if not isinstance(check_item, dict):
            raise ValueError(
                f"[parser_check_items] Item type in `check_items` list is not a dict:{type(check_item)}, {check_item}")
        res.append(Error(check_item))

    return res


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
        if self.check_task is None:
            # Interface normal return check
            result = self.assert_success(self.succ, True)

        elif self.check_task == CheckTasks.checkResponse:
            # Verify that the request is successful
            result = self.check_response(self.succ, True)

        elif self.check_task == CheckTasks.checkErrorResponse:
            # Interface return error code and error message check
            result = self.check_error_response(self.response, self.succ, self.check_items)

        elif self.check_task == CheckTasks.checkConnectionResponse:
            # Connection interface response check
            result = self.check_value_equal(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.checkIgnore:
            # Don't verify the result, return the `success` or `failure` of the request
            result = self.succ

        elif self.check_task == CheckTasks.checkIgnoreRateLimit:
            result = self.ignore_rate_limit(self.response, self.succ)

        elif self.check_task == CheckTasks.checkIgnoreExpectedErrors:
            result = self.ignore_expected_errors(self.response, self.succ, self.check_items)

        else:
            log.warning(
                f"[CheckFunc] Check task does not exist:{self.check_task}, roll back to check response.")
            result = self.assert_success(self.succ, True)

        # Add check_items here if something new need verify

        return result

    def check_response(self, actual, expect):
        if actual is not expect:
            log.error("[CheckFunc] {0} request check failed, response:{1}".format(self.func_name, self.response))
        return actual is expect

    def assert_success(self, actual, expect):
        if actual is not expect:
            log.error("[CheckFunc] {0} request check failed, response:{1}".format(self.func_name, self.response))
        assert actual is expect
        return actual is expect

    def check_error_response(self, res, actual=True, check_items: Error = Error({})):
        if not (actual is False):
            raise ValueError(f"[CheckFunc] `{self.func_name}` requests successful, check response error failed !!!")

        if isinstance(res, MilvusException):
            if isinstance(check_items, dict):
                check_items = Error(check_items)
                if (check_items.code is not None or check_items.message is not None) and (
                        not (check_items.code == res.code or check_items.message in res.message)):
                    raise ValueError("[CheckFunc] Check `{0}` response error failed: ({1} == {2} or {3} in {4})".format(
                        self.func_name, check_items.code, res.code, check_items.message, res.message))
            else:
                log.warning(f"[CheckFunc] `check_items` is not a dict, please check !!!")
        else:
            log.error(f"[CheckFunc] Response of API is not an error of `MilvusException`: {type(res)}")
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

    def ignore_rate_limit(self, actual_res, actual_res_check):
        if actual_res_check is True:
            return True

        if isinstance(actual_res, MilvusException):
            assert ServerExceptionsMessage.RateLimitError in actual_res.message
            log.warning(f"[CheckFunc] Catch `{self.func_name}` rate limit error: {actual_res}")
            return True

        raise ValueError("[CheckFunc] Response of API is not an error of `MilvusException`: %s" % type(actual_res))

    def ignore_expected_errors(self, actual_res, actual_res_check, check_items: Union[dict, List[dict]] = {}):
        """
        :param actual_res: MilvusException
        :param actual_res_check: bool
        :param check_items: Union[dict, List[dict]]
                        dict<{"code": <int>, "message": <str>}>
                            - check `code` and `message`
                        list[dict<{"code": <int>, "message": <str>}>, ... ]
                            - check (`code` and `message`) || `code` || `message`
        """
        if actual_res_check is True:
            return True

        if isinstance(actual_res, MilvusException):
            res = []
            for check_item in parser_check_items(check_items):

                _res = True
                if check_item.code is not None:
                    _res = _res and check_item.code == actual_res.code
                if check_item.message is not None:
                    _res = _res and check_item.message in actual_res.message
                res.append(_res)

            if sum(res) == 0:
                log.error("[CheckFunc] Request: `{0}` error doesn't meet expectations: {1}, error: {2}".format(
                    self.func_name, check_items, actual_res))
                return False

            log.warning(f"[CheckFunc] Ignore request `{self.func_name}` error: {actual_res}")
            return True
        raise ValueError("[CheckFunc] Response of API is not an error of `MilvusException`: %s" % type(actual_res))
