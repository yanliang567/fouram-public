import sys

try:
    from pymilvus import MilvusClient
except ImportError as e:
    from client.client_base.base_wrapper import BaseWrapper

    MilvusClient = BaseWrapper

try:
    from pymilvus.orm.constants import UNLIMITED
except ImportError as e:
    UNLIMITED = -1

from client.check.func_check import ResponseChecker
from client.util.api_request import api_request
from client.common.common_param import InterfaceResponse


class MilvusClientWrapper:
    _client = None

    def __init__(self):
        pass

    @property
    def client(self):
        if not isinstance(self._client, MilvusClient):
            msg = "[MilvusClientWrapper] MilvusClient object:%s may not be initialized yet, please check!"
            raise Exception(msg % self._client)
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    def init_milvus_client(self, uri="", user="", password="", db_name="", token="", timeout=None, check_task=None,
                           check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([MilvusClient, uri, user, password, db_name, token, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, uri=uri, user=user, password=password,
                                       db_name=db_name, token=token, timeout=timeout, **kwargs).run()
        self.client = res.response if res.res_result else None
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_collection(self, collection_name, dimension=None, primary_field_name="id", id_type="int",
                          vector_field_name="vector", metric_type="COSINE", auto_id=False, timeout=None, schema=None,
                          index_params=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_collection, collection_name, dimension, primary_field_name, id_type,
                           vector_field_name, metric_type, auto_id, timeout, schema, index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       dimension=dimension, primary_field_name=primary_field_name, id_type=id_type,
                                       vector_field_name=vector_field_name, metric_type=metric_type, auto_id=auto_id,
                                       timeout=timeout, schema=schema, index_params=index_params, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_index(self, collection_name, index_params, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_index, collection_name, index_params, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_params=index_params, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def insert(self, collection_name, data, timeout=None, partition_name="", check_task=None, check_items=None,
               **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.insert, collection_name, data, timeout, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       data=data, timeout=timeout, partition_name=partition_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def upsert(self, collection_name, data, timeout=None, partition_name="", check_task=None, check_items=None,
               **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.upsert, collection_name, data, timeout, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       data=data, timeout=timeout, partition_name=partition_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def hybrid_search(self, collection_name, reqs, ranker, limit=10, output_fields=None, timeout=None,
                      partition_names=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.hybrid_search, collection_name, reqs, ranker, limit, output_fields, timeout,
                           partition_names], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       reqs=reqs, ranker=ranker, limit=limit, output_fields=output_fields,
                                       timeout=timeout, partition_names=partition_names, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def search(self, collection_name, data, filter="", limit=10, output_fields=None, search_params=None, timeout=None,
               partition_names=None, anns_field=None, ranker=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.search, collection_name, data, filter, limit, output_fields, search_params,
                           timeout, partition_names, anns_field, ranker], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       data=data, filter=filter, limit=limit, output_fields=output_fields,
                                       search_params=search_params, timeout=timeout, partition_names=partition_names,
                                       anns_field=anns_field, ranker=ranker, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def query(self, collection_name, filter="", output_fields=None, timeout=None, ids=None, partition_names=None,
              check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        # NOTE: Hack `output_fields` behavior to be consistent with ORM behavior
        output_fields = output_fields or ["id"]
        res = api_request([self.client.query, collection_name, filter, output_fields, timeout, ids, partition_names],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       filter=filter, output_fields=output_fields, timeout=timeout, ids=ids,
                                       partition_names=partition_names, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def query_iterator(self, collection_name, batch_size=1000, limit=UNLIMITED, filter="", output_fields=None,
                       partition_names=None, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.query_iterator, collection_name, batch_size, limit, filter, output_fields,
                           partition_names, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       batch_size=batch_size, limit=limit, filter=filter, output_fields=output_fields,
                                       partition_names=partition_names, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def search_iterator(self, collection_name, data, batch_size=1000, filter=None, limit=UNLIMITED, output_fields=None,
                        search_params=None, timeout=None, partition_names=None, anns_field=None, round_decimal=-1,
                        check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.search_iterator, collection_name, data, batch_size, filter, limit, output_fields,
                           search_params, timeout, partition_names, anns_field, round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       data=data, batch_size=batch_size, filter=filter, limit=limit,
                                       output_fields=output_fields, search_params=search_params, timeout=timeout,
                                       partition_names=partition_names, anns_field=anns_field,
                                       round_decimal=round_decimal, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get(self, collection_name, ids, output_fields=None, timeout=None, partition_names=None, check_task=None,
            check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get, collection_name, ids, output_fields, timeout, partition_names], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       ids=ids, output_fields=output_fields, timeout=timeout,
                                       partition_names=partition_names, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def delete(self, collection_name, ids=None, timeout=None, filter=None, partition_name=None, check_task=None,
               check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.delete, collection_name, ids, timeout, filter, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       ids=ids, timeout=timeout, filter=filter, partition_name=partition_name,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_collection_stats(self, collection_name, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_collection_stats, collection_name, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_collection(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_collection, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def has_collection(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.has_collection, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_collections(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_collections], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_collection(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_collection, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def rename_collection(self, old_name, new_name, target_db="", timeout=None, check_task=None, check_items=None,
                          **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.rename_collection, old_name, new_name, target_db, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, old_name=old_name, new_name=new_name,
                                       target_db=target_db, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_schema(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_schema], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_struct_field_schema(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_struct_field_schema], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_field_schema(self, name, data_type, desc="", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_field_schema, name, data_type, desc], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, name=name, data_type=data_type,
                                       desc=desc, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def prepare_index_params(self, field_name="", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.prepare_index_params, field_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, field_name=field_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def close(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.close])
        check_result = ResponseChecker(res, func_name, check_task, check_items).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def load_collection(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.load_collection, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def release_collection(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.release_collection, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_load_state(self, collection_name, partition_name="", timeout=None, check_task=None, check_items=None,
                       **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_load_state, collection_name, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def refresh_load(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.refresh_load, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_indexes(self, collection_name, field_name="", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_indexes, collection_name, field_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       field_name=field_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_index(self, collection_name, index_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_index, collection_name, index_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_index(self, collection_name, index_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_index, collection_name, index_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_index_properties(self, collection_name, index_name, properties, timeout=None, check_task=None,
                               check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.alter_index_properties, collection_name, index_name, properties, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, properties=properties, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_index_properties(self, collection_name, index_name, property_keys, timeout=None, check_task=None,
                              check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_index_properties, collection_name, index_name, property_keys, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, property_keys=property_keys, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_collection_properties(self, collection_name, properties, timeout=None, check_task=None,
                                    check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.alter_collection_properties, collection_name, properties, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       properties=properties, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_collection_properties(self, collection_name, property_keys, timeout=None, check_task=None,
                                   check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_collection_properties, collection_name, property_keys, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       property_keys=property_keys, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_collection_field(self, collection_name, field_name, field_params, timeout=None, check_task=None,
                               check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.alter_collection_field, collection_name, field_name, field_params, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       field_name=field_name, field_params=field_params, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def add_collection_field(self, collection_name, field_name, data_type, desc="", timeout=None, check_task=None,
                             check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.add_collection_field, collection_name, field_name, data_type, desc, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       field_name=field_name, data_type=data_type, desc=desc, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_partition(self, collection_name, partition_name, timeout=None, check_task=None, check_items=None,
                         **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_partition, collection_name, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_partition(self, collection_name, partition_name, timeout=None, check_task=None, check_items=None,
                       **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_partition, collection_name, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def has_partition(self, collection_name, partition_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.has_partition, collection_name, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_partitions(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_partitions, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def load_partitions(self, collection_name, partition_names, timeout=None, check_task=None, check_items=None,
                        **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.load_partitions, collection_name, partition_names, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_names=partition_names, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def release_partitions(self, collection_name, partition_names, timeout=None, check_task=None, check_items=None,
                           **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.release_partitions, collection_name, partition_names, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_names=partition_names, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_partition_stats(self, collection_name, partition_name, timeout=None, check_task=None, check_items=None,
                            **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_partition_stats, collection_name, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_user(self, user_name, password, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_user, user_name, password, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name, password=password,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_user(self, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_user, user_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def update_password(self, user_name, old_password, new_password, reset_connection=False, timeout=None,
                        check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request(
            [self.client.update_password, user_name, old_password, new_password, reset_connection, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name,
                                       old_password=old_password, new_password=new_password,
                                       reset_connection=reset_connection, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_users(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_users, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_user(self, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_user, user_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def grant_role(self, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.grant_role, user_name, role_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name,
                                       role_name=role_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def revoke_role(self, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.revoke_role, user_name, role_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, user_name=user_name,
                                       role_name=role_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_role(self, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_role, role_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_role(self, role_name, force_drop=False, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_role, role_name, force_drop, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name,
                                       force_drop=force_drop, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_role(self, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_role, role_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_roles(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_roles, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def grant_privilege(self, role_name, object_type, privilege, object_name, db_name="", timeout=None, check_task=None,
                        check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request(
            [self.client.grant_privilege, role_name, object_type, privilege, object_name, db_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name,
                                       object_type=object_type, privilege=privilege, object_name=object_name,
                                       db_name=db_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def revoke_privilege(self, role_name, object_type, privilege, object_name, db_name="", timeout=None,
                         check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request(
            [self.client.revoke_privilege, role_name, object_type, privilege, object_name, db_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name,
                                       object_type=object_type, privilege=privilege, object_name=object_name,
                                       db_name=db_name, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def grant_privilege_v2(self, role_name, privilege, collection_name, db_name=None, timeout=None, check_task=None,
                           check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.grant_privilege_v2, role_name, privilege, collection_name, db_name, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name,
                                       privilege=privilege, collection_name=collection_name, db_name=db_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def revoke_privilege_v2(self, role_name, privilege, collection_name, db_name=None, timeout=None, check_task=None,
                            check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.revoke_privilege_v2, role_name, privilege, collection_name, db_name, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, role_name=role_name,
                                       privilege=privilege, collection_name=collection_name, db_name=db_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_alias(self, collection_name, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_alias, collection_name, alias, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       alias=alias, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_alias(self, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_alias, alias, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_alias(self, collection_name, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.alter_alias, collection_name, alias, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       alias=alias, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_alias(self, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_alias, alias, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, alias=alias, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_aliases(self, collection_name="", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_aliases, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def using_database(self, db_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.using_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def use_database(self, db_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.use_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_database(self, db_name, properties=None, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_database, db_name, properties, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, properties=properties,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_database(self, db_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_databases(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_databases, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_database(self, db_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_database_properties(self, db_name, properties, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.alter_database_properties, db_name, properties], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name, properties=properties,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_database_properties(self, db_name, property_keys, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_database_properties, db_name, property_keys], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, db_name=db_name,
                                       properties=property_keys, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def flush(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.flush, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def compact(self, collection_name, is_clustering=False, is_l0=False, timeout=None, check_task=None,
                check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.compact, collection_name, is_clustering, is_l0, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       is_clustering=is_clustering, is_l0=is_l0, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_compaction_state(self, job_id, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_compaction_state, job_id, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, job_id=job_id, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_server_version(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_server_version, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_privilege_group(self, group_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_privilege_group, group_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, group_name=group_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_privilege_group(self, group_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_privilege_group, group_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, group_name=group_name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_privilege_groups(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_privilege_groups, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def add_privileges_to_group(self, group_name, privileges, timeout=None, check_task=None, check_items=None,
                                **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.add_privileges_to_group, group_name, privileges, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, group_name=group_name,
                                       privileges=privileges, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def remove_privileges_from_group(self, group_name, privileges, timeout=None, check_task=None, check_items=None,
                                     **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.remove_privileges_from_group, group_name, privileges, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, group_name=group_name,
                                       privileges=privileges, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_resource_group(self, name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.create_resource_group, name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, name=name, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def update_resource_groups(self, configs, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.update_resource_groups, configs, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, configs=configs, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_resource_group(self, name, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.drop_resource_group, name, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, name=name, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_resource_group(self, name, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_resource_group, name, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, name=name, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_resource_groups(self, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_resource_groups, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def transfer_replica(self, source_group, target_group, collection_name, num_replicas, timeout=None, check_task=None,
                         check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.transfer_replica, source_group, target_group, collection_name, num_replicas,
                           timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, source_group=source_group,
                                       target_group=target_group, collection_name=collection_name,
                                       num_replicas=num_replicas, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_replica(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.describe_replica, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def run_analyzer(self, texts, analyzer_params=None, with_hash=False, with_detail=False, collection_name=None,
                     field_name=None, analyzer_names=None, timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.run_analyzer, texts, analyzer_params, with_hash, with_detail, collection_name,
                           field_name, analyzer_names, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, texts=texts,
                                       analyzer_params=analyzer_params, with_hash=with_hash, with_detail=with_detail,
                                       collection_name=collection_name, field_name=field_name,
                                       analyzer_names=analyzer_names, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def update_replicate_configuration(self, clusters=None, cross_cluster_topology=None, timeout=None, check_task=None,
                                       check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.update_replicate_configuration, clusters, cross_cluster_topology, timeout],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, clusters=clusters,
                                       cross_cluster_topology=cross_cluster_topology, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def flush_all(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.flush_all, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_flush_all_state(self, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_flush_all_state, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_loaded_segments(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_loaded_segments, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_persistent_segments(self, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.list_persistent_segments, collection_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_server_type(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_server_type])
        check_result = ResponseChecker(res, func_name, check_task, check_items).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_compaction_plans(self, job_id, timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.client.get_compaction_plans, job_id, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, job_id=job_id, timeout=timeout,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)
