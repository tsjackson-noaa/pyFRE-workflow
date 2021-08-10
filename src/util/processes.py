
import os
import errno
import signal
import subprocess

from dagster import check
from dagster.utils import safe_tempfile_path

def popen_wrapper(args_list, log, log_name=None, shell=False, cwd=None, env=None,
    timeout=0):
    def pre_exec_fn():
        # Restore default signal disposition and invoke setsid
        for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), signal.SIG_DFL)
        os.setsid() # creates new process group

    def exception_handler(proc):
        # kill subprocess and any subsubprocesses it may have spawned
        # https://stackoverflow.com/a/36955420
        os.killpg(proc.pid, signal.SIGINT)
        proc.wait()
        return proc.returncode

    if log_name is None:
        log_name = f"\"{' '.join(args_list)}\""
    if shell:
        # python default is /bin/sh -c; FRE legacy code based on csh
        args_list = ['/bin/csh', '-f'] + args_list
    env = check.opt_dict_param(env, "env")
    stdout = ''
    stderr = ''
    retcode = 1
    pid = None

    # pylint: disable=subprocess-popen-preexec-fn
    with subprocess.Popen(
        args_list,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=False, cwd=cwd, env=env,
        preexec_fn=pre_exec_fn
    ) as proc:
        try:
            pid = proc.pid
            log.info(f"Running command {log_name} ({pid}).")
            (stdout, stderr) = proc.communicate(timeout=timeout)
            retcode = proc.returncode
        except subprocess.TimeoutExpired:
            log.error(f"Command {log_name} ({pid}) timed out (> {timeout} sec).")
            exception_handler(proc)
            retcode = errno.ETIME
        except subprocess.CalledProcessError:
            log.error(f"Command {log_name} ({pid}) raised CalledProcessError.")
            exception_handler(proc)
            retcode = 1
        except KeyboardInterrupt:
            exception_handler(proc)

    return (retcode, stdout.decode('utf-8'), stderr.decode('utf-8'))



