import sys

try:
    from pymilvus import Role
except ImportError as e:
    from client.client_base.base_wrapper import BaseWrapper
    Role = BaseWrapper

from client.check.func_check import ResponseChecker
from client.util.api_request import api_request
from client.common.common_param import InterfaceResponse


class ApiRoleWrapper:
    _role = None

    @property
    def role(self):
        if not isinstance(self._role, Role):
            raise Exception(f"[ApiRoleWrapper] Role object: {self._role} may not be initialized yet, please check!")
        return self._role

    @role.setter
    def role(self, value):
        self._role = value

    def init_role(self, name, using="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([Role, name, using], **kwargs)
        self.role = res[0] if res_result else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, **kwargs).run()
        return InterfaceResponse(*res, res_result, check_result)

    @property
    def name(self):
        return self.role.name

    def create(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.create])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.drop])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def add_user(self, username, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.add_user, username])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, username=username).run()
        return InterfaceResponse(*res, res_result, check_result)

    def remove_user(self, username, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.remove_user, username])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, username=username).run()
        return InterfaceResponse(*res, res_result, check_result)

    def get_users(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.get_users])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def is_exist(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.is_exist])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)

    def grant(self, object, object_name, privilege, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.grant, object, object_name, privilege])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, object=object,
                                       object_name=object_name, privilege=privilege).run()
        return InterfaceResponse(*res, res_result, check_result)

    def revoke(self, object, object_name, privilege, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.revoke, object, object_name, privilege])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, object=object,
                                       object_name=object_name, privilege=privilege).run()
        return InterfaceResponse(*res, res_result, check_result)

    def list_grant(self, object, object_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.list_grant, object, object_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, object=object,
                                       object_name=object_name).run()
        return InterfaceResponse(*res, res_result, check_result)

    def list_grants(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.role.list_grant])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result).run()
        return InterfaceResponse(*res, res_result, check_result)
