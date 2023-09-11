from datetime import datetime
import random
import os

from configs.base_config import BaseConfig
from commons.common_params import EnvVariable


class LogConfig:

    def __init__(self):
        self.log_name = "fouram_log"
        self.log_debug = ""
        self.log_info = ""
        self.log_err = ""
        self.log_folder = ""
        self.get_default_config()

    def get_default_config(self, subfolder=""):
        """ Make sure the path exists """
        log_dir = self.gen_log_path(subfolder=subfolder)
        self.log_debug = "{0}/{1}.debug".format(log_dir, self.log_name)
        self.log_info = "{0}/{1}.log".format(log_dir, self.log_name)
        self.log_err = "{0}/{1}.err".format(log_dir, self.log_name)

    def gen_log_path(self, subfolder="", retry_counts=1000):
        count = 0
        random_count = 1

        _sub_folder = subfolder
        while True:
            if self.log_folder == "":
                sub_folder_prefix = EnvVariable.FOURAM_LOG_SUB_FOLDER_PREFIX or self.log_name
                self.log_folder = "/{:%Y_%m_%d}/{}".format(datetime.now(),
                                                           sub_folder_prefix + "_" + str(random.randint(10000, 99999)))

            log_folder = self.log_folder if subfolder == "" else "{0}/{1}".format(self.log_folder, _sub_folder)
            log_dir = str(EnvVariable.FOURAM_LOG_PATH).rstrip('/') + log_folder

            if not os.path.isdir(str(log_dir)):
                BaseConfig.create_path(log_dir)
                return log_dir

            if subfolder != "":
                _sub_folder = str(subfolder) + "_" + str(random_count)
                random_count += 1

            count += 1
            if count > retry_counts:
                raise Exception("[gen_log_path] Generated log path exists, please check:{0}".format(log_dir))


log_config = LogConfig()
