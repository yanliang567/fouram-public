import time
import threading
import queue

from chaos_mesh.commons.common_func import (
    format_dict_output, parser_kubectl_watch_pod_res
)

from deploy.commons.common_params import DeployLabelsBase

from utils.util_cmd import CmdExe
from utils.util_log import log


class ApiTimerPrintPod:
    def __init__(self, pod_q: queue.Queue, interval: int = 1):
        self.pod_q = pod_q
        self.interval = interval

        self._title = ("NAME", "STATUS", "RESTARTS", "AGE", "IP", "NODE")
        self.ignore_title = False

        self.stop_flag = False
        self.tick_flag = True
        self.t = None

        self._len_dict = {}

    @property
    def _get_q_data(self):
        data = [self.pod_q.get() for _ in range(self.pod_q.qsize())]
        return sorted(data, key=lambda x: x["NAME"])

    def run(self):
        if not self.ignore_title:
            time.sleep(1)

        self.tick_flag = False
        _, self._len_dict = format_dict_output(self._title, self._get_q_data, ignore_title=self.ignore_title,
                                               default_key_len=self._len_dict)
        self.tick_flag = True

        if not self.ignore_title:
            self.ignore_title = True

        if not self.stop_flag:
            self.t = threading.Timer(self.interval, self.run)
            self.t.start()

    def final_run(self):
        self.stop_flag = True

        end_time = time.time() + self.interval + 10
        while time.time() < end_time:
            if self.tick_flag:
                if self.t:
                    self.t.cancel()

                _, self._len_dict = format_dict_output(self._title, self._get_q_data, ignore_title=True,
                                                       default_key_len=self._len_dict)
                return

        if self.t:
            self.t.cancel()
        log.warning('[ApiTimerPrintPod] The print pod method may be stuck, please check!!!')


class CliWatchPod:

    def __init__(self, obj: CmdExe, pod_q: queue.Queue, deploy_labels: DeployLabelsBase, interval=0.01):
        self.obj = obj
        self.pod_q = pod_q
        self.deploy_labels = deploy_labels
        self.interval = interval

        self._stop_flag = False
        self.t = None

    @property
    def stop_flag(self):
        return self._stop_flag

    @stop_flag.setter
    def stop_flag(self, value):
        self._stop_flag = value

    def _print_data(self):
        for d in [self.pod_q.get() for _ in range(self.pod_q.qsize())]:
            log.info(d)

    def run(self):
        flag, res = parser_kubectl_watch_pod_res(self.obj.output(), self.deploy_labels.pod_labels_obj.get_unique_labels)
        if flag:
            self.pod_q.put(res)

        if not self.stop_flag:
            self.t = threading.Timer(self.interval, self.run)
            self.t.start()

    def tick(self, timeout: int):
        # print results at regular intervals
        end_time = time.time() + timeout
        while time.time() <= end_time:
            self._print_data()
            time.sleep(0.5)
        # stop timer thread
        self.stop_flag = True

    def stop(self):
        if self.t:
            self.t.cancel()
        self._print_data()
