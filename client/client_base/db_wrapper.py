import sys

try:
    from pymilvus import db
except ImportError as e:
    from client.client_base.base_wrapper import BaseInitWrapper

    db = BaseInitWrapper(object_name="db", message=e)

from client.check.func_check import ResponseChecker
from client.util.api_request import api_request
from client.common.common_param import InterfaceResponse


class ApiDBWrapper:
    def __init__(self):
        self.db = db

    def using_database(self, db_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.db.using_database, db_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_database(self, db_name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.db.create_database, db_name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, using=using,
                                       timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_database(self, db_name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.db.drop_database, db_name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, using=using,
                                       timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_database(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.db.list_database, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)
