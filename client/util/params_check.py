import functools
import dacite

from client.common.common_func import check_key_exist, get_must_params, get_required_params, check_params_type
from client.parameters.functional_params import GetParamObj
from client.parameters import params_name as pn
from utils.util_log import log


def check_params(param_template: dict):
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            params = kwargs.get("params", None)
            log.info("[check_params] {0} input params: {1}".format(func.__name__, params))
            msg = "[check_params] Check Params Failed!"

            # Check whether the required parameters are included in the input parameters
            must_params = get_must_params(param_template)
            res = check_key_exist(must_params, params) if isinstance(params, dict) else False
            if res is False:
                log.error(msg)
                raise Exception(msg)

            required_params = get_required_params(params, param_template)
            # Check if the required parameter type conforms to the definition
            check_res = check_params_type(required_params, param_template)
            if check_res is False:
                log.error(msg)
                raise Exception(msg)

            kwargs.update(params=required_params)
            log.info("[check_params] {0} required params: {1}".format(func.__name__, required_params))
            return func(*args, **kwargs)

        return inner_wrapper
    return wrapper


def functional_check_params(param_template: dict):
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            params = kwargs.get("params", {})
            log.info("[functional_check_params] {0} input params: {1}".format(func.__name__, params))
            msg = "[functional_check_params] Check Params Failed!"

            # parser functional params
            sub_callable_obj = kwargs.get("sub_callable_obj", None)
            if sub_callable_obj:
                try:
                    # get functional func obj
                    sub_obj_name = sub_callable_obj.__func__.__closure__[0].cell_contents.__name__
                    sub_param_obj = GetParamObj().get_obj(sub_obj_name)

                    # parser input params
                    functional_params = params.get(pn.functional_params, {})
                    sub_params = dacite.from_dict(data_class=sub_param_obj, data=functional_params).to_dict

                    # get required params
                    functional_required_params = get_required_params(functional_params, sub_params)

                    # reset params
                    params.update(functional_params=functional_required_params)
                    log.info(f"[functional_check_params] {func.__name__} update functional params: {params}")
                except Exception as e:
                    log.error(f"[functional_check_params] {func.__name__} can't parse functional params: {e}")

            # Check whether the required parameters are included in the input parameters
            must_params = get_must_params(param_template)
            res = check_key_exist(must_params, params) if isinstance(params, dict) else False
            if res is False:
                log.error(msg)
                raise Exception(msg)

            required_params = get_required_params(params, param_template)
            # Check if the required parameter type conforms to the definition
            check_res = check_params_type(required_params, param_template)
            if check_res is False:
                log.error(msg)
                raise Exception(msg)

            kwargs.update(params=required_params)
            log.info("[functional_check_params] {0} required params: {1}".format(func.__name__, required_params))
            return func(*args, **kwargs)

        return inner_wrapper
    return wrapper
