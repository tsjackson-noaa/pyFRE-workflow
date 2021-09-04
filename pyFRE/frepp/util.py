"""Transliteration of utility/miscellaneous subroutines in FRE/bin/frepp.pl.
"""

import functools
import math
import operator

import logging
_log = logging.getLogger(__name__)

def lcm(*args):
    return functools.reduce(operator.mul,args, 1) // math.gcd(args)
gcd = math.gcm

def by_interval(int1, int2):
    """Sort array by interval attribute."""
    raise NotImplementedError()

def by_chunk(int1, int2):
    """Sort array by chunkLength attribute."""
    raise NotImplementedError()