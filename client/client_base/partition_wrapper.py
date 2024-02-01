import sys
import time
from numpy import NaN

from pymilvus import Partition

from client.check.func_check import ResponseChecker
from client.util.api_request import api_request
from client.common.common_param import InterfaceResponse

from parameters.input_params import param_info
from utils.util_log import log

TIMEOUT = None


class ApiPartitionWrapper:
    _partition = None

    @property
    def partition(self):
        if not isinstance(self._partition, Partition):
            raise Exception(
                f"[ApiPartitionWrapper] Partition object:{self._partition} may not be initialized yet, please check!")
        return self._partition

    @partition.setter
    def partition(self, value):
        self._partition = value

    def init_partition(self, collection, name, description="", check_task=None, check_items=None, **kwargs):
        """ In order to distinguish the same name of partition """
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([Partition, collection, name, description], **kwargs)
        self.partition = res[0] if res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def description(self):
        return self.partition.description

    @property
    def name(self):
        return self.partition.name

    @property
    def is_empty(self):
        return self.partition.is_empty

    @property
    def num_entities(self):
        return self.partition.num_entities

    def _flush(self):
        log.warning("[ApiCollectionWrapper] Collection has not attribute 'flush', call 'num_entities' instead.")
        start = time.perf_counter()
        _res = self.partition.num_entities
        rt = time.perf_counter() - start
        return InterfaceResponse(_res, rt, True, True)

    def flush(self, check_task=None, check_items=None, **kwargs):
        if not hasattr(self.partition, "flush"):
            return self._flush()

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.flush], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def load(self, replica_number=NaN, timeout=None, check_task=None, check_items=None, **kwargs):
        replica_number = param_info.param_replica_num if replica_number is NaN else replica_number

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.load, replica_number, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def release(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def insert(self, data, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.insert, data], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, data=data,
                                       **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def search(self, data, anns_field, params, limit, expr=None, output_fields=None, check_task=None, check_items=None,
               **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.search, data, anns_field, params, limit, expr, output_fields],
                                      **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, data=data,
                                       anns_field=anns_field, params=params, limit=limit, expr=expr,
                                       output_fields=output_fields, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def hybrid_search(self, reqs, rerank, limit, output_fields=None, timeout=None, round_decimal=-1, check_task=None,
                      check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.hybrid_search, reqs, rerank, limit, output_fields, timeout,
                                       round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, reqs=reqs,
                                       rerank=rerank, limit=limit, output_fields=output_fields, timeout=timeout,
                                       round_decimal=round_decimal, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def query(self, expr, output_fields=None, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.query, expr, output_fields, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, expression=expr,
                                       output_fields=output_fields, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def delete(self, expr, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.delete, expr], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ=res_result, expr=expr,
                                       **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def get_replicas(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.partition.get_replicas, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)
