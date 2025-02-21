import subprocess
import os
# from pipes import quote

from utils.util_log import log


class CmdExe:
    def __init__(self, cmd=''):
        self._cmd = cmd
        self._obj = None

    def run_cmd(self, timeout=None):
        log.info("[Cmd Exe] {}".format(self._cmd))
        try:
            res = subprocess.check_output(self._cmd, shell=True, stderr=subprocess.STDOUT, timeout=timeout,
                                          encoding='utf-8')
            return res.rstrip('\n')
        except subprocess.CalledProcessError as e:
            msg = "[Cmd Exe] Execute cmd:{0} raise error output:{1}, code:{2}".format(self._cmd, e.output, e.returncode)
            log.error(msg)
            raise Exception(msg)
            # return ''
        except subprocess.SubprocessError as e:
            msg = "[Cmd Exe] Execute cmd:{0} raise error:{1}".format(self._cmd, e)
            log.error(msg)
            raise Exception(msg)
            # return ''

    def run_cmd_bg(self, env=None):
        if env is None:
            log.info("[Cmd Exe] {}".format(self._cmd))
            self._obj = subprocess.Popen(self._cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, encoding='utf-8')
        else:
            log.info("[Cmd Exe] {0}, env:{1}".format(self._cmd, env))
            self._obj = subprocess.Popen("export", close_fds=True, shell=True, env=env)

    def output(self):
        if self._obj:
            return self._obj.stdout.readline()
        else:
            return ''
