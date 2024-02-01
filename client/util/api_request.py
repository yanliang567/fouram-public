import traceback
import time
from typing import Tuple
import uuid

from client.common.common_param import InterfaceResponse

from commons.common_type import PRECISION
from commons.common_func import truncated_output
from utils.util_log import log


class InfoLogout:
    _log_output = ["Collection.insert", "Collection.delete", "Index", "Collection.load", "Collection.search",
                   "Collection.query", "Collection.hybrid_search"]
    log_output = _log_output
    log_row_length = 3000

    def reset_output(self, output: list = []):
        self.log_output = output

    def recover_output(self, output: list = None):
        self.log_output = output or self._log_output

    def reset_log_row_length(self, log_row_length: int = 3000):
        self.log_row_length = log_row_length


info_logout = InfoLogout()


def time_catch():
    def wrapper(func):
        # @functools.wraps(func)
        def inner_wrapper(*args, **kwargs) -> Tuple[tuple, bool]:
            request_id = str(uuid.uuid1())
            func_name = args[0][0].__qualname__
            start = time.perf_counter()
            try:
                # start = time.perf_counter()
                res = func(*args, request_id=request_id, **kwargs)
                rt = time.perf_counter() - start

                log.debug("(api_response) : [%s] %s, [requestId: %s]" % (
                    func_name, truncated_output(res, info_logout.log_row_length), request_id))

                msg = "[Time] {0} run in {1}s".format(func_name, round(rt, PRECISION.COMMON_PRECISION))
                if callable(args[0][0]) and func_name in info_logout.log_output:
                    log.info(msg)
                else:
                    log.debug(msg)

                return (res, rt), True
            except Exception as e:
                rt = time.perf_counter() - start
                log.debug(traceback.format_exc())
                log.error("(api_response) : [%s] %s, [requestId: %s]" % (
                    func_name, truncated_output(e, info_logout.log_row_length), request_id))
                return (e, rt), False

        return inner_wrapper

    return wrapper


@time_catch()
def api_request(_list, request_id: str = None, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)

            log.debug("(api_request)  : [%s] args: %s, kwargs: %s, [requestId: %s]" % (
                func.__qualname__, truncated_output(arg, info_logout.log_row_length), str(kwargs), request_id))

            return func(*arg, **kwargs)
    return (False, 0), False


def time_wrapper(func):
    """
    This decorator prints the execution time for the decorated function.
    """

    def wrapper(*args, **kwargs):
        _rt = kwargs.get("_rt", False)
        kwargs.pop("_rt", None)

        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()

        response_time = end - start
        msg = "[time_wrapper] {} run in {}s".format(func.__name__, round(response_time, PRECISION.COMMON_PRECISION))

        if _rt:
            log.info(msg)
            return result, response_time
        else:
            log.debug(msg)
            return result

    return wrapper


def func_time_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs) -> InterfaceResponse:
            start = time.perf_counter()
            try:
                res = func(*args, **kwargs)
                rt = time.perf_counter() - start

                msg = "[Time] {0} run in {1}s, response: {2}".format(func.__name__,
                                                                     round(rt, PRECISION.COMMON_PRECISION), res)
                log.debug(msg)
                # return (res, rt), True
                return InterfaceResponse(res, rt, True, True)
            except Exception as e:
                rt = time.perf_counter() - start
                log.debug(traceback.format_exc())
                log.error("[func_time_catch] : %s" % truncated_output(e, info_logout.log_row_length))
                # return (e, rt), False
                return InterfaceResponse(e, rt, False, False)

        return inner_wrapper

    return wrapper


def docstring_decorator(func):
    def wrapper(*args, **kwargs):
        log.info("[docstring_decorator] Detailed test content is as follows: {}".format(func.__doc__))
        return func(*args, **kwargs)

    return wrapper
