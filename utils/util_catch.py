import traceback

from utils.util_log import log


def func_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            result_check = kwargs.get("result_check", False)
            kwargs.pop("result_check", None)
            try:
                res = func(*args, **kwargs)
                log.debug("(func_catch) : {} ".format(res))
                return (res, True) if result_check else res
            except Exception as e:
                log.error(traceback.format_exc())
                log.error("(func_catch) : {}".format(e))
                return (e, False) if result_check else e
        return inner_wrapper
    return wrapper


def func_execute_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise Exception("[func_execute_catch] Execute func failed:{0}, args:{1}, kwargs:{2}, error:{3}".format(
                    func.__name__, args, kwargs, e))
        return inner_wrapper
    return wrapper


@func_execute_catch()
def func_request(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)
            return func(*arg, **kwargs)
    else:
        raise Exception("[func_request] Can not parser func, args:{0}, kwargs:{1}".format(_list, kwargs))
