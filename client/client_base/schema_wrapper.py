import sys

from pymilvus import CollectionSchema, FieldSchema

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse


class ApiCollectionSchemaWrapper:
    _collection_schema = None

    @property
    def collection_schema(self):
        if not isinstance(self._collection_schema, CollectionSchema):
            msg = "[ApiCollectionSchemaWrapper] CollectionSchema object:%s may not be initialized yet, please check!"
            raise Exception(msg % self._collection_schema)
        return self._collection_schema

    @collection_schema.setter
    def collection_schema(self, value):
        self._collection_schema = value

    def init_collection_schema(self, fields, description="", check_task=None, check_items=None, **kwargs):
        """In order to distinguish the same name of CollectionSchema"""
        func_name = sys._getframe().f_code.co_name
        res = api_request([CollectionSchema, fields, description], **kwargs)
        self.collection_schema = res.response if res.res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, fields=fields, description=description,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    @property
    def primary_field(self):
        return self.collection_schema.primary_field

    @property
    def fields(self):
        return self.collection_schema.fields

    @property
    def description(self):
        return self.collection_schema.description

    @property
    def auto_id(self):
        return self.collection_schema.auto_id


class ApiFieldSchemaWrapper:
    _field_schema = None

    @property
    def field_schema(self):
        if not isinstance(self._field_schema, FieldSchema):
            msg = "[ApiFieldSchemaWrapper] FieldSchema object:%s may not be initialized yet, please check!"
            raise Exception(msg % self._field_schema)
        return self._field_schema

    @field_schema.setter
    def field_schema(self, value):
        self._field_schema = value

    def init_field_schema(self, name, dtype, description="", check_task=None, check_items=None, **kwargs):
        """In order to distinguish the same name of FieldSchema"""
        func_name = sys._getframe().f_code.co_name
        res = api_request([FieldSchema, name, dtype, description], **kwargs)
        self.field_schema = res.response if res.res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, name=name, dtype=dtype,
                                       description=description, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    @property
    def description(self):
        return self.field_schema.description

    @property
    def params(self):
        return self.field_schema.params

    @property
    def dtype(self):
        return self.field_schema.dtype
