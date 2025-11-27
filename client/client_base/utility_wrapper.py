import sys
from datetime import datetime

from pymilvus import utility

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse

TIMEOUT = 600


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility

    def mkts_from_hybridts(self, hybridts, milliseconds=0., delta=None):
        res = api_request([self.ut.mkts_from_hybridts, hybridts, milliseconds, delta])
        return InterfaceResponse(*res.get_res_list, res.res_result)

    def mkts_from_unixtime(self, epoch, milliseconds=0., delta=None):
        res = api_request([self.ut.mkts_from_unixtime, epoch, milliseconds, delta])
        return InterfaceResponse(*res.get_res_list, res.res_result)

    def mkts_from_datetime(self, d_time=None, milliseconds=0., delta=None):
        d_time = datetime.now() if d_time is None else d_time
        res = api_request([self.ut.mkts_from_datetime, d_time, milliseconds, delta])
        return InterfaceResponse(*res.get_res_list, res.res_result)

    def hybridts_to_datetime(self, hybridts, tz=None):
        res = api_request([self.ut.hybridts_to_datetime, hybridts, tz])
        return InterfaceResponse(*res.get_res_list, res.res_result)

    def hybridts_to_unixtime(self, hybridts):
        res = api_request([self.ut.hybridts_to_unixtime, hybridts])
        return InterfaceResponse(*res.get_res_list, res.res_result)

    def loading_progress(self, collection_name, partition_names=None, using="default", check_task=None,
                         check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.loading_progress, collection_name, partition_names, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_names=partition_names, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def load_state(self, collection_name, partition_names=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.load_state, collection_name, partition_names, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_names=partition_names, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def wait_for_loading_complete(self, collection_name, partition_names=None, timeout=None, using="default",
                                  check_task=None, check_items=None):
        timeout = timeout or TIMEOUT

        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.wait_for_loading_complete, collection_name, partition_names, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_names=partition_names, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def index_building_progress(self, collection_name, index_name="", using="default", check_task=None,
                                check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.index_building_progress, collection_name, index_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def wait_for_index_building_complete(self, collection_name, index_name="", timeout=None, using="default",
                                         check_task=None, check_items=None):
        timeout = timeout or TIMEOUT

        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.wait_for_index_building_complete, collection_name, index_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       index_name=index_name, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def has_collection(self, collection_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.has_collection, collection_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def has_partition(self, collection_name, partition_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.has_partition, collection_name, partition_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       partition_name=partition_name, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_collection(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.drop_collection, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def rename_collection(self, old_collection_name, new_collection_name, timeout=None, using="default",
                          check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.rename_collection, old_collection_name, new_collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, old_collection_name=old_collection_name,
                                       new_collection_name=new_collection_name, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_collections(self, timeout=None, using="default", check_task=None, check_items=None):
        timeout = timeout or TIMEOUT

        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_collections, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def load_balance(self, collection_name, src_node_id, dst_node_ids, sealed_segment_ids, timeout=None,
                     using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request(
            [self.ut.load_balance, collection_name, src_node_id, dst_node_ids, sealed_segment_ids, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_query_segment_info(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.get_query_segment_info, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.create_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_alias(self, alias, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.drop_alias, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def alter_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.alter_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_aliases(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_aliases, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def do_bulk_insert(self, collection_name, files, partition_name=None, timeout=None, using="default",
                       check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.do_bulk_insert, collection_name, files, partition_name, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       files=files, partition_name=partition_name, timeout=timeout, using=using,
                                       **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_bulk_insert_state(self, task_id, timeout=None, using="default", check_task=None, check_items=None,
                              **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.get_bulk_insert_state, task_id, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, task_id=task_id, timeout=timeout,
                                       using=using, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_bulk_insert_tasks(self, limit=0, collection_name=None, timeout=None, using="default", check_task=None,
                               check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_bulk_insert_tasks, collection_name, limit, collection_name, timeout, using],
                          **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, limit=limit,
                                       collection_name=collection_name, timeout=timeout, using=using, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def reset_password(self, user, old_password, new_password, using="default", timeout=None, check_task=None,
                       check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.reset_password, user, old_password, new_password, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, user=user, old_password=old_password,
                                       new_password=new_password, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_user(self, user, password, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.create_user, user, password, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, user=user, password=password,
                                       using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def update_password(self, user, old_password, new_password, using="default", timeout=None, check_task=None,
                        check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.update_password, user, old_password, new_password, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, user=user, old_password=old_password,
                                       new_password=new_password, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def delete_user(self, user, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.delete_user, user, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, user=user, using=using,
                                       timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_usernames(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_usernames, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_roles(self, include_user_info, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_roles, include_user_info, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, include_user_info=include_user_info,
                                       using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_user(self, username, include_role_info, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_user, username, include_role_info, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, username=username,
                                       include_role_info=include_role_info, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_users(self, include_role_info, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_users, include_role_info, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, include_role_info=include_role_info,
                                       using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_server_version(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.get_server_version, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, using=using, timeout=timeout).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def create_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.create_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def drop_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.drop_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def describe_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.describe_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_resource_groups(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_resource_groups, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def transfer_node(self, source, target, num_node, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.transfer_node, source, target, num_node, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def transfer_replica(self, source, target, collection_name, num_replica, using="default", timeout=None,
                         check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.transfer_replica, source, target, collection_name, num_replica, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def flush_all(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.flush_all, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, timeout=timeout, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def get_server_type(self, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.get_server_type, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, using=using).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    def list_indexes(self, collection_name, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res = api_request([self.ut.list_indexes, collection_name, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, collection_name=collection_name,
                                       using=using, timeout=timeout, **kwargs).run()
        return InterfaceResponse(*res.get_res_list, check_result)

    # def bulk_load(self, collection_name, partition_name="", channels="", row_based=True, files="", timeout=None,
    #               using="default", check_task=None, check_items=None, **kwargs):
    #     func_name = sys._getframe().f_code.co_name
    #     res, res_result = api_request([self.ut.bulk_load, collection_name, partition_name, channels, row_based, files,
    #                                    timeout, using], **kwargs)
    #     check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
    #                                    collection_name=collection_name, using=using).run()
    #     return InterfaceResponse(*res, res_result, check_result)
    #
    # def get_bulk_load_state(self, task_id, timeout=None, using="default", check_task=None, check_items=None,
    # **kwargs):
    #     func_name = sys._getframe().f_code.co_name
    #     res, res_result = api_request([self.ut.get_bulk_load_state, task_id, timeout, using], **kwargs)
    #     check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, task_id=task_id,
    #                                    using=using).run()
    #     return InterfaceResponse(*res, res_result, check_result)
    #
    # def wait_for_bulk_load_tasks_completed(self, task_ids, timeout=None, using="default", **kwargs):
    #     start = time.time()
    #     successes = {}
    #     fails = {}
    #     if timeout is not None:
    #         task_timeout = timeout / len(task_ids)
    #     else:
    #         task_timeout = TIMEOUT
    #     while True and (len(successes) + len(fails)) < len(task_ids):
    #         in_progress = {}
    #         time.sleep(0.5)
    #         for task_id in task_ids:
    #             if successes.get(task_id, None) is not None or fails.get(task_id, None) is not None:
    #                 continue
    #             else:
    #                 state, _ = self.get_bulk_load_state(task_id, task_timeout, using, **kwargs)
    #                 if state.state_name == "BulkLoadPersisted":  # "BulkLoadCompleted"
    #                     successes[task_id] = state
    #                 elif state.state_name == "BulkLoadFailed":
    #                     fails[task_id] = state
    #                 else:
    #                     in_progress[task_id] = state
    #         end = time.time()
    #         if timeout is not None:
    #             if end - start > timeout:
    #                 in_progress.update(fails)
    #                 in_progress.update(successes)
    #                 return False, in_progress
    #
    #     if len(fails) == 0:
    #         return True, successes
    #     else:
    #         fails.update(successes)
    #         return False, fails
    #
    # def calc_distance(self, vectors_left, vectors_right, params=None, timeout=None, using="default", check_task=None,
    #                   check_items=None):
    #     timeout = TIMEOUT if timeout is None else timeout
    #
    #     func_name = sys._getframe().f_code.co_name
    #     res, res_result = api_request([self.ut.calc_distance, vectors_left, vectors_right, params, timeout, using])
    #     check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
    #                                    using=using).run()
    #     return InterfaceResponse(*res, res_result, check_result)
