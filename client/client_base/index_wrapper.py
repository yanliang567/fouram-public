import sys

from pymilvus import Index

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse


class ApiIndexWrapper:
    _index = None

    @property
    def index(self):
        if not isinstance(self._index, Index):
            raise Exception(f"[ApiIndexWrapper] Index object: {self._index} may not be initialized yet, please check!")
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    def init_index(self, collection, field_name, index_params, check_task=None, check_items=None, **kwargs):
        """ In order to distinguish the same name of index """
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([Index, collection, field_name, index_params], **kwargs)
        self.index = res[0] if res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, collection=collection,
                                       field_name=field_name, index_params=index_params, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.index.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        # return res, check_result
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def params(self):
        return self.index.params

    @property
    def collection_name(self):
        return self.index.collection_name

    @property
    def field_name(self):
        return self.index.field_name
