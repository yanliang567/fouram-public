import sys

from pymilvus import Connections, DefaultConfig

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse


class ApiConnectionsWrapper:

    def __init__(self):
        self.connection = Connections()

    def add_connection(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.add_connection], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def disconnect(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.disconnect, alias])
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def remove_connection(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.remove_connection, alias])
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def connect(self, alias=DefaultConfig.DEFAULT_USING, check_task=None, check_items=None, **kwargs):
        """
        user="", password="", db_name=""
        To be compatible with the old version, the newly added parameters have been moved to kwargs
        """
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.connect, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def has_connection(self, alias=DefaultConfig.DEFAULT_USING, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.has_connection, alias])
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    #  def get_connection(self, alias=DefaultConfig.DEFAULT_USING, check_task=None, check_items=None):
    #      func_name = sys._getframe().f_code.co_name
    #      res, res_result = api_request([self.connection.get_connection, alias])
    #      check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, alias=alias).run()
    #      return res, check_result

    def list_connections(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.list_connections])
        check_result = ResponseChecker(res, func_name, check_task, check_items).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_connection_addr(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.connection.get_connection_addr, alias])
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias).run()
        return InterfaceResponse(*res.get_res_list, check_result)
