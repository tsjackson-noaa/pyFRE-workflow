"""Transliteration of FRE/lib/FRETargets.pm.
"""

from . import FREDefaults

import logging
_log = logging.getLogger(__name__)

# Global constants -------------------------------------------------------------

TARGET_DEFAULT = FREDefaults.Target()
TARGET_STARTERS = [TARGET_DEFAULT, 'repro', 'debug' ]
TARGET_FOLLOWERS = ['hdf5', 'openmp']

# Functions --------------------------------------------------------------------

def standardize(targetList):
    """Return standard representation of the target list (as a string) and
    optional error message."""
    raise NotImplementedError()

def contains(targetList, target):
    return (target in [item for sublist in targetList for item in sublist.split('-')])

def containsProd(targetList):
    return contains(targetList, 'prod')

def containsRepro(targetList):
    return contains(targetList, 'repro')

def containsDebug(targetList):
    return contains(targetList, 'debug')

def containsHDF5(targetList):
    return contains(targetList, 'hdf5')

def containsOpenMP(targetList):
    return contains(targetList, 'openmp')

def starters():
    return TARGET_STARTERS

def followers():
    return TARGET_FOLLOWERS;

def all():
    return TARGET_STARTERS + TARGET_FOLLOWERS

def allCombinations():
    raise NotImplementedError()

