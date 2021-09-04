"""Transliteration of time series/time average subroutines in FRE/bin/frepp.pl.
"""

import logging
_log = logging.getLogger(__name__)


def zInterpolate(zInterp, infile, outfile, caltype, variables, source):
    """Set up interpolation on z levels."""
    raise NotImplementedError()

def segStartMonths(segTime, segUnits):
    """"""
    raise NotImplementedError()

def convertSegments(segTime, segUnits, diag_source, type):
    """Make csh for splitting history files into monthly files."""
    raise NotImplementedError()

def get_subint(node):
    """Return appropriate subinterval."""
    raise NotImplementedError()

def gettimelevels(freq, cl):
    """Get appropriate number of time levels in a time series file."""
    raise NotImplementedError()

def segmentLengthInMonths():
    """"""
    raise NotImplementedError()

def getSegmentLength():
    """"""
    raise NotImplementedError()

# ------------------------------------------------------------------------------

def annualTS(tsNode, sim0, startofrun, diagtablecontent):
    """TIMESERIES - ANNUAL"""
    raise NotImplementedError()

def seasonalTS(tsNode, sim0):
    """TIMESERIES - SEASONAL"""
    raise NotImplementedError()

def monthlyAVfromhist(taNode, sim0):
    """TIMEAVERAGES - MONTHLY"""
    raise NotImplementedError()

def annualAV1yrfromhist(taNode, sim0, write2arch, yr2do):
    """TIMEAVERAGES - ANNUAL 1YR"""
    raise NotImplementedError()

def annualAVxyrfromann(taNode, sim0, ppcNode, annavnodes, annCalcInterval):
    """TIMEAVERAGES - ANNUAL XYR"""
    raise NotImplementedError()

def monthlyTSfromdailyTS(tsNode, sim0, startofrun):
    """TIMESERIES - monthly from daily ts"""
    raise NotImplementedError()

def directTS(tsNode, sim0, startofrun):
    """TIMESERIES - HOURLY, DAILY, MONTHLY, ANNUAL"""
    raise NotImplementedError()

def monthlyAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - MONTHLY"""
    raise NotImplementedError()

def annualAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - ANNUAL XYR"""
    raise NotImplementedError()

def staticvars(diag_source, ptmpDir, tmphistdir, refinedir):
    """Create static variables file."""
    raise NotImplementedError()

def TSfromts(tsNode, sim0, subchunk):
    """TIMESERIES - from smaller timeSeries"""
    raise NotImplementedError()

def seaTSfromts(tsNode, sim0, subchunk):
    """TIMESERIES - from smaller timeSeries"""
    raise NotImplementedError()

def seasonalAVfromhist(taNode, sim0):
    """TIMEAVERAGES - SEASONAL"""
    raise NotImplementedError()

def seasonalAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - SEASONAL (doesn't support cubedsphere)"""
    raise NotImplementedError()