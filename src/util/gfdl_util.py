"""Utility functions to support the site-specific classes in gfdl.py.
"""
import os
import re
import shutil
import subprocess
import time
import tempfile
from pyFRE import util

import logging
_log = logging.getLogger(__name__)

class ModuleManager(util.Singleton):
    # conda used for all POD dependencies instead of environment module-provided
    # executables; following only used for GFDL-specific data handling.
    # Use most recent versions available on both RDHPCS and workstations
    _current_module_versions = {
        'git':      'git/2.4.6',
        'gcp':      'gcp/2.3',
    }

    def __init__(self):
        if 'MODULESHOME' not in os.environ:
            # could set from module --version
            raise OSError(("Unable to determine how modules are handled "
                "on this host."))
        _ = os.environ.setdefault('LOADEDMODULES', '')

        # capture the modules the user has already loaded once, when we start up,
        # so that we can restore back to this state in revert_state()
        self.user_modules = set(self._list())
        self.modules_i_loaded = set()

    def _module(self, *args):
        # based on $MODULESHOME/init/python.py
        if isinstance(args[0], list): # if we're passed explicit list, unpack it
            args = args[0]
        cmd = '{}/bin/modulecmd'.format(os.environ['MODULESHOME'])
        proc = subprocess.Popen([cmd, 'python'] + args, stdout=subprocess.PIPE)
        (output, error) = proc.communicate()
        if proc.returncode != 0:
            raise util.MDTFCalledProcessError(
                returncode=proc.returncode,
                cmd=' '.join([cmd, 'python'] + args), output=error)
        exec(output)

    def _parse_names(self, *module_names):
        return [m if ('/' in m) else self._current_module_versions[m] \
            for m in module_names]

    def load(self, *module_names):
        """Wrapper for module load.
        """
        mod_names = self._parse_names(*module_names)
        for mod_name in mod_names:
            if mod_name not in self.modules_i_loaded:
                self.modules_i_loaded.add(mod_name)
                self._module(['load', mod_name])

    def load_commands(self, *module_names):
        return ['module load {}'.format(m) \
            for m in self._parse_names(*module_names)]

    def unload(self, *module_names):
        """Wrapper for module unload.
        """
        mod_names = self._parse_names(*module_names)
        for mod_name in mod_names:
            if mod_name in self.modules_i_loaded:
                self.modules_i_loaded.discard(mod_name)
                self._module(['unload', mod_name])

    def unload_commands(self, *module_names):
        return ['module unload {}'.format(m) \
            for m in self._parse_names(*module_names)]

    def _list(self):
        """Wrapper for module list.
        """
        return os.environ['LOADEDMODULES'].split(':')

    def revert_state(self):
        mods_to_unload = self.modules_i_loaded.difference(self.user_modules)
        for mod in mods_to_unload:
            self._module(['unload', mod])
        # User's modules may have been unloaded if we loaded a different version
        for mod in self.user_modules:
            self._module(['load', mod])
        assert set(self._list()) == self.user_modules

# ========================================================================

def gcp_wrapper(source_path, dest_dir, timeout=None, dry_run=None, log=_log):
    """Wrapper for file and recursive directory copying using the GFDL
    site-specific General Copy Program (`https://gitlab.gfdl.noaa.gov/gcp/gcp`__.)
    Assumes GCP environment module has been loaded beforehand, and calls GCP in
    a subprocess.
    """
    modMgr = ModuleManager()
    modMgr.load('gcp')
    if timeout is None:
        timeout = 0
    if dry_run is None:
        dry_run = False

    source_path = os.path.normpath(source_path)
    dest_dir = os.path.normpath(dest_dir)
    # gcp requires trailing slash, ln ignores it
    if os.path.isdir(source_path):
        source = ['-r', 'gfdl:' + source_path + os.sep]
        # gcp /A/B/ /C/D/ will result in /C/D/B, so need to specify parent dir
        dest = ['gfdl:' + os.path.dirname(dest_dir) + os.sep]
    else:
        source = ['gfdl:' + source_path]
        dest = ['gfdl:' + dest_dir + os.sep]
    log.info('\tGCP {} -> {}'.format(source[-1], dest[-1]))
    util.run_command(
        ['gcp', '--sync', '-v', '-cd'] + source + dest,
        timeout=timeout, dry_run=dry_run, log=log
    )

def make_remote_dir(dest_dir, timeout=None, dry_run=None, log=_log):
    """Workaround to create a directory on a remote filesystem by GCP'ing it.
    """
    try:
        os.makedirs(dest_dir)
    except OSError as exc:
        # use GCP for this because output dir might be on a read-only filesystem.
        # apparently trying to test this with os.access is less robust than
        # just catching the error
        log.debug("os.makedirs at %s failed (%r); trying GCP.", dest_dir, exc)
        with tempfile.TemporaryDirectory() as temp_dir:
            work_dir = os.path.join(temp_dir, os.path.basename(dest_dir))
            os.makedirs(work_dir)
            gcp_wrapper(work_dir, dest_dir, timeout=timeout, dry_run=dry_run, log=log)

def running_on_PPAN():
    """Return true if current host is in the PPAN cluster.
    """
    host = os.uname()[1].split('.')[0]
    return (re.match(r"(pp|an)\d{3}", host) is not None)

def is_on_tape_filesystem(path):
    """Return true if path is on a DMF tape-backed filesystem. Does not attempt
    to determine status of path (active disk vs. tape).
    """
    # handle eg. /arch0 et al as well as /archive.
    return any(os.path.realpath(path).startswith(s) \
        for s in ['/arch', '/ptmp', '/work', '/uda'])

def rmtree_wrapper(path):
    """Attempt to workaround errors with :py:func:`shutil.rmtree` on NFS
    filesystems.
    """
    # Standard shutil.rmtree raises ``OSError: [Errno 39] Directory not empty``,
    # presumably due to a .nfsXXXX lock file still being present. Don't know of
    # a better workaround than to wait and retry.
    # https://stackoverflow.com/q/58943374
    # https://github.com/astropy/astropy/issues/9970
    shutil.rmtree(path, ignore_errors=True)
    time.sleep(1)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)

def frepp_freq(date_freq):
    # logic as written would give errors for 1yr chunks (?)
    if date_freq is None:
        return date_freq
    assert isinstance(date_freq, util.DateFrequency)
    if date_freq.unit == 'hr' or date_freq.quantity != 1:
        return date_freq.format()
    else:
        # weekly not used in frepp
        _frepp_dict = {
            'yr': 'annual',
            'season': 'seasonal',
            'mo': 'monthly',
            'day': 'daily',
            'hr': 'hourly'
        }
        return _frepp_dict[date_freq.unit]

