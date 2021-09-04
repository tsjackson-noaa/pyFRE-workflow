"""Transliteration of FRE/lib/FREDefaults.pm.
"""

import os
import sys

import logging
_log = logging.getLogger(__name__)

# Global return statuses -------------------------------------------------------

STATUS_OK            = 0
STATUS_XML_NOT_VALID = 1

STATUS_COMMAND_GENERIC_PROBLEM  = 10
STATUS_COMMAND_NO_EXPERIMENTS   = 11
STATUS_COMMAND_PLATFORM_PROBLEM = 12

STATUS_FS_GENERIC_PROBLEM    = 20
STATUS_FS_PERMISSION_PROBLEM = 21
STATUS_FS_PATH_NOT_EXISTS    = 22
STATUS_FS_PATH_EXISTS        = 23

STATUS_FRE_GENERIC_PROBLEM = 30
STATUS_FRE_PATH_UNEXPECTED = 31

STATUS_FRE_SOURCE_GENERIC_PROBLEM = 40
STATUS_FRE_SOURCE_NOT_EXISTS      = 41
STATUS_FRE_SOURCE_PROBLEM         = 42
STATUS_FRE_SOURCE_NO_MATCH        = 43

STATUS_FRE_COMPILE_GENERIC_PROBLEM = 50
STATUS_FRE_COMPILE_NOT_EXISTS      = 51
STATUS_FRE_COMPILE_PROBLEM         = 52
STATUS_FRE_COMPILE_NO_MATCH        = 53

STATUS_FRE_RUN_GENERIC_PROBLEM   = 60
STATUS_FRE_RUN_NO_TEMPLATE       = 61
STATUS_FRE_RUN_EXECUTION_PROBLEM = 62

STATUS_DATA_NOT_EXISTS = 70
STATUS_DATA_NO_MATCH   = 71


# Global constants -------------------------------------------------------------

SITE_CURRENT = os.environ.get("FRE_SYSTEM_SITE", None)
if not SITE_CURRENT:
    _log.fatal("FRE environment variables aren't set correctly")
    sys.exit(STATUS_FRE_GENERIC_PROBLEM)

SITES_ALL = SITE_CURRENT.split(':')

XMLFILE_DEFAULT = 'rts.xml';
TARGET_DEFAULT  = 'prod';

GLOBAL_NAMES = 'site,siteDir,suite,platform,target,name,root,stem';
EXPERIMENT_DIRS = "root,src,exec,scripts,stdout,stdoutTmp,state,work,ptmp,stmp," \
        + "archive,postProcess,analysis,include"
DEFERRED_NAMES = 'name'

# Functions --------------------------------------------------------------------

def Site():
    return os.environ["FRE_SYSTEM_SITE"]

def Sites():
    return os.environ["FRE_SYSTEM_SITE"].split(':')

def XMLFile():
    return XMLFILE_DEFAULT

def Target():
    return TARGET_DEFAULT

def ExperimentDirs():
    return EXPERIMENT_DIRS.split(',')

def ReservedPropertyNames():
    return GLOBAL_NAMES.split(',') + [s + 'Dir' for s in ExperimentDirs()]

def DeferredPropertyNames():
    return DEFERRED_NAMES.split(',')

