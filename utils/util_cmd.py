import subprocess
import os
import signal
# from pipes import quote

from utils.util_log import log


class CmdExe:
    def __init__(self, cmd=''):
        self._cmd = cmd
        self._obj = None

    @property
    def obj(self):
        return self._obj

    @obj.setter
    def obj(self, value):
        self._obj = value

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

    def run_cmd_bg(self, env=None, start_new_session=False):
        if env is None:
            log.info("[Cmd Exe] {}".format(self._cmd))
            self.obj = subprocess.Popen(self._cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, encoding='utf-8', start_new_session=start_new_session)
        else:
            log.info("[Cmd Exe] {0}, env:{1}".format(self._cmd, env))
            self.obj = subprocess.Popen("export", close_fds=True, shell=True, env=env)

    def output(self):
        if self.obj:
            return self.obj.stdout.readline()
        else:
            return ''

    def terminate(self, timeout=10):
        if hasattr(self.obj, "terminate"):
            log.debug(f"[Cmd Exe] Terminate subprocess: {self.obj}")
            self.obj.terminate()

            log.debug(f'[Cmd Exe] Wait for background subprocess to exit: {self.obj}')
            self.obj.wait(timeout=timeout)

        if self.obj.poll() is not None and hasattr(self.obj, "kill"):
            log.debug(f"[Cmd Exe] Kill subprocess: {self.obj}")
            self.obj.kill()

    def kill(self):
        if os.getpgid(self.obj.pid):
            log.debug(f"[Cmd Exe] Start killing pid: {self.obj.pid}, {os.getpgid(self.obj.pid)}")
            try:
                os.killpg(os.getpgid(self.obj.pid), signal.SIGTERM)
            except Exception as e:
                log.warning(f'[Cmd Exe] Kill subprocess failed: {e}')
        else:
            self.terminate()
