from locust import User, TaskSet, events

from client.common.common_type import concurrent_global_params


def event_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            exception = None if result.check_result else "False"
            rt = result.rt * 1000
            events.request.fire(request_type=concurrent_global_params.request_type, name=func.__name__,
                                response_time=rt, response_length=0, exception=exception, context=User.context)
        return inner_wrapper
    return wrapper


class ClientTask:
    def __init__(self, obj: callable, request_type: str = "grpc"):
        self.obj = obj
        concurrent_global_params.request_type = request_type

    def __getattr__(self, name):
        func = getattr(self.obj, "concurrent_{0}".format(name))

        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                exception = None if result.check_result else "False"
                rt = result.rt * 1000
                events.request.fire(request_type=concurrent_global_params.request_type, name=name,
                                    response_time=rt, response_length=0, exception=exception, context=User.context)
            except Exception as e:
                events.request.fire(request_type=concurrent_global_params.request_type, name=name,
                                    response_time=0, response_length=0, exception="False", context=User.context)
                raise Exception(f"[ClientTask] {e}")
        return wrapper


class MyTaskSet(TaskSet):

    def debug(self):
        self.client.debug(self.tasks_params.debug.params)

    def search(self):
        self.client.search(self.tasks_params.search.params)

    def hybrid_search(self):
        self.client.hybrid_search(self.tasks_params.hybrid_search.params)

    def query(self):
        self.client.query(self.tasks_params.query.params)

    def flush(self):
        self.client.flush(self.tasks_params.flush.params)

    def load(self):
        self.client.load(self.tasks_params.load.params)

    def release(self):
        self.client.release(self.tasks_params.release.params)

    def release_partitions(self):
        self.client.release_partitions(self.tasks_params.release_partitions.params)

    def load_release(self):
        self.client.load_release(self.tasks_params.load_release.params)

    def insert(self):
        self.client.insert(self.tasks_params.insert.params)

    def upsert(self):
        self.client.upsert(self.tasks_params.upsert.params)

    def delete(self):
        self.client.delete(self.tasks_params.delete.params)

    def scene_test(self):
        self.client.scene_test(self.tasks_params.scene_test.params)

    def scene_insert_delete_flush(self):
        self.client.scene_insert_delete_flush(self.tasks_params.scene_insert_delete_flush.params)

    def scene_insert_partition(self):
        self.client.scene_insert_partition(self.tasks_params.scene_insert_partition.params)

    def scene_test_partition(self):
        self.client.scene_test_partition(self.tasks_params.scene_test_partition.params)

    def scene_test_partition_hybrid_search(self):
        self.client.scene_test_partition_hybrid_search(self.tasks_params.scene_test_partition_hybrid_search.params)

    def iterate_search(self):
        self.client.iterate_search(self.tasks_params.iterate_search.params)

    def load_search_release(self):
        self.client.load_search_release(self.tasks_params.load_search_release.params)

    def load_hybrid_search_release(self):
        self.client.load_hybrid_search_release(self.tasks_params.load_hybrid_search_release.params)

    def scene_search_test(self):
        self.client.scene_search_test(self.tasks_params.scene_search_test.params)

    def scene_hybrid_search_test(self):
        self.client.scene_hybrid_search_test(self.tasks_params.scene_hybrid_search_test.params)
