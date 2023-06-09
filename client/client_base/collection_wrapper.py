import sys
import time
from numpy import NaN

from pymilvus import Collection, DefaultConfig
from pymilvus.orm.types import CONSISTENCY_STRONG

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse

from parameters.input_params import param_info
from utils.util_log import log


class ApiCollectionWrapper:
    _collection = None

    @property
    def collection(self):
        if not isinstance(self._collection, Collection):
            msg = "[ApiCollectionWrapper] Collection object:%s may not be initialized yet, please check!"
            raise Exception(msg % self._collection)
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    def __init__(self):
        pass

    def init_collection(self, name, schema=None, using="default", check_task=None, check_items=None, **kwargs):
        # consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        # kwargs.update({"consistency_level": consistency_level})

        """ In order to distinguish the same name of collection """
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([Collection, name, schema, using], **kwargs)
        self.collection = res[0] if res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, name=name, schema=schema,
                                       using=using, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def schema(self):
        return self.collection.schema

    @property
    def description(self):
        return self.collection.description

    @property
    def name(self):
        return self.collection.name

    @property
    def is_empty(self):
        return self.collection.is_empty

    @property
    def num_entities(self):
        return self.collection.num_entities

    @property
    def primary_field(self):
        return self.collection.primary_field

    @property
    def _shards_num(self):
        return self.collection._shards_num

    def construct_from_dataframe(self, name, dataframe, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([Collection.construct_from_dataframe, name, dataframe], **kwargs)
        self.collection = res[0] if res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, name=name,
                                       dataframe=dataframe, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def load(self, partition_names=None, replica_number=NaN, timeout=None, check_task=None, check_items=None, **kwargs):
        replica_number = param_info.param_replica_num if replica_number is NaN else replica_number

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.load, partition_names, replica_number, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       partition_names=partition_names, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def release(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def insert(self, data, partition_name=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.insert, data, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, dat=data,
                                       partition_name=partition_name, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def upsert(self, data, partition_name=None, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.upsert, data, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, dat=data,
                                       partition_name=partition_name, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def _flush(self):
        log.warning("[ApiCollectionWrapper] Collection has not attribute 'flush', call 'num_entities' instead.")
        start = time.perf_counter()
        _res = self.collection.num_entities
        rt = time.perf_counter() - start
        return InterfaceResponse(_res, rt, True, True)

    def flush(self, check_task=None, check_items=None, **kwargs):
        if not hasattr(self.collection, "flush"):
            return self._flush()

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.flush], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def search(self, data, anns_field, param, limit, expr=None, partition_names=None, output_fields=None, timeout=None,
               round_decimal=-1, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.search, data, anns_field, param, limit, expr, partition_names,
                                       output_fields, timeout, round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, data=data,
                                       anns_field=anns_field, param=param, limit=limit, expr=expr,
                                       partition_names=partition_names, output_fields=output_fields, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def query(self, expr, output_fields=None, partition_names=None, timeout=None, check_task=None, check_items=None,
              **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.query, expr, output_fields, partition_names, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, expression=expr,
                                       partition_names=partition_names, output_fields=output_fields, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def partitions(self):
        return self.collection.partitions

    def partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       partition_name=partition_name).run()
        return InterfaceResponse(*res, res_result, check_result)

    def has_partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.has_partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       partition_name=partition_name).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_partition(self, partition_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.drop_partition, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       partition_name=partition_name, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def create_partition(self, partition_name, check_task=None, check_items=None, description=""):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.create_partition, partition_name, description])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       partition_name=partition_name).run()
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def indexes(self):
        return self.collection.indexes

    def index(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.index])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def create_index(self, field_name, index_params, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.create_index, field_name, index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, field_name=field_name,
                                       index_params=index_params, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def has_index(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.has_index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_index(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.drop_index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def create_alias(self, alias_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.create_alias, alias_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_alias(self, alias_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.drop_alias, alias_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def alter_alias(self, alias_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.alter_alias, alias_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def delete(self, expr, partition_name=None, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.delete, expr, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def compact(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.compact, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def get_compaction_state(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.get_compaction_state, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def get_compaction_plans(self, timeout=None, check_task=None, check_items={}, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.get_compaction_plans, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    def wait_for_compaction_completed(self, timeout=None, **kwargs):
        res = self.collection.wait_for_compaction_completed(timeout, **kwargs)
        return res

    def get_replicas(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.collection.get_replicas, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)
