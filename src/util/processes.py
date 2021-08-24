import os
import errno
import json
import signal
import subprocess
import textwrap

class CompletedProcess(subprocess.CompletedProcess):
    def __init__(self, *args, env_in=None, env_out=None, **kwargs):
        self.env_in = env_in
        self.env_out = env_out
        super(CompletedProcess, self).__init__(*args, **kwargs)

    @classmethod
    def from_subprocess_result(cls, completed_process, **kwargs):
        env_in = kwargs.get('env_in', None)
        env_out = kwargs.get('env_out', None)
        return cls(
            args=completed_process.args,
            returncode=completed_process.returncode,
            stdout=completed_process.stdout,
            stderr=completed_process.stderr,
            env_in=env_in, env_out=env_out
        )

SHELL = '/bin/csh'   # used by legacy FRE
SHELL_FLAGS = ['-f'] # no -c, because we start the shell and then communicate()

_popen_kwargs = {
    'encoding':'utf-8',
    'executable': SHELL,
    'restore_signals': True,
    'start_new_session': True
}

def subproc_handler(func, cmd_or_args, log, log_name=None, **kwargs):
    retcode = 1
    try:
        log.info(f"Running command '{log_name}'.")
        return func(cmd_or_args, **kwargs)
    except subprocess.TimeoutExpired:
        log.error(f"Command '{log_name}' timed out (> {kwargs['timeout']} sec).")
        retcode = errno.ETIME
    except subprocess.CalledProcessError:
        log.error(f"Command '{log_name}' raised CalledProcessError.")
    except FileNotFoundError:
        log.error(f"Command '{log_name}' couldn't find executable.")

    env_in = kwargs.get('env', None)
    return CompletedProcess(
        args=cmd_or_args,
        returncode=retcode, stdout="", stderr="", env_in=env_in, env_out=env_in
    )

def _run_command(args_list, **kwargs):
    kwargs.update({'capture_output': True})
    kwargs.update(_popen_kwargs)
    env_in = kwargs.get('env', None)
    result = subprocess.run(args_list, **kwargs)
    return CompletedProcess.from_subprocess_result(
        result, env_in=env_in, env_out=env_in
    )

def run_command(args_list, *args, **kwargs):
    if not kwargs.get('log_name', None):
        kwargs['log_name'] = args_list[0]
    if kwargs.get('update_env', False):
        raise ValueError('Need run_shell instead')
    return subproc_handler(_run_command, args_list, *args, **kwargs)

def _run_shell(commands, shell_flags=None, timeout=None, update_env=False, **kwargs):
    def _exception_handler(proc):
        # kill subprocess and any subsubprocesses it may have spawned
        # https://stackoverflow.com/a/36955420
        # hasattr in case we're passed None
        if hasattr(proc, 'pid'):
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait()

    DELIMITER = '\v' # not likely to be used by anything else

    kwargs.update({'shell': False})
    kwargs.update(_popen_kwargs)
    if shell_flags is None:
        shell_cmd = [SHELL] + SHELL_FLAGS
    else:
        shell_cmd = [SHELL] + SHELL_FLAGS + shell_flags
    commands = textwrap.dedent(commands)
    if update_env:
        # only robust mechanism to recover changed env var state in subprocess
        commands_in = commands + f'\necho "{DELIMITER}";' \
            + '/usr/bin/env python -c "import os, json; print(json.dumps(dict(os.environ)))"'
    else:
        commands_in = commands

    with subprocess.Popen(
        shell_cmd,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        **kwargs
    ) as proc:
        try:
            (stdout, stderr) = proc.communicate(
                input=commands_in, timeout=timeout # encode?
            )
            retcode = proc.returncode
            if update_env:
                stdout, env_out = stdout.split(DELIMITER)
                env_out = json.loads(env_out)
            else:
                env_out = kwargs.get('env', None)
        except (Exception, KeyboardInterrupt):
            _exception_handler(proc)
            raise

    return CompletedProcess(
        args=shell_cmd + [commands],
        returncode=retcode, stdout=stdout, stderr=stderr,
        env_in=kwargs.get('env', None), env_out=env_out
    )

def run_shell(commands, *args, **kwargs):
    if not kwargs.get('log_name', None):
        if '\n' not in commands:
            kwargs['log_name'] = commands
        else:
            kwargs['log_name'] = '<multiple shell commands>'
    if not kwargs.get('shell', True):
        raise ValueError('Need run_command instead')
    return subproc_handler(_run_shell, commands, *args, **kwargs)

