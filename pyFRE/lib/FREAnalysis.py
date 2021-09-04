"""Transliteration of FRE/lib/FREAnalysis.pm.
"""

import dataclasses as dc
from typing import Any

from . import FRE, FREDefaults, FRETargets, FREUtil

import logging
_log = logging.getLogger(__name__)

class Analysis():
    def __init__(self, **kwargs):
        self.ts_av_Node = kwargs.get("node", None)
        self.expt = kwargs.get("experiment", None)
        self.gridspec = kwargs.get("gridSpec", None)
        self.staticfile = kwargs.get("staticFile", None)
        self.tsORav = kwargs.get("type", None)
        self.diagfile = kwargs.get("diagSrc", None)
        self.ppRootDir = kwargs.get("ppRootDir", None)
        self.component = kwargs.get("comp", None)
        self.dtvars_ref = kwargs.get("dtvarsRef", None)
        self.analysisdir = kwargs.get("analysisDir", None)
        self.aoutscriptdir = kwargs.get("scriptDir", None)
        self.workdir = kwargs.get("workDir", None)
        self.archivedir = kwargs.get("archDir", None)
        self.experID = kwargs.get("experID", None)
        self.realizID = kwargs.get("realizID", None)
        self.runID = kwargs.get("runID", None)
        self.opt_t = kwargs.get("opt_t", None)
        self.opt_O = kwargs.get("opt_O", None)
        self.opt_Y = kwargs.get("opt_Y", None)
        self.opt_Z = kwargs.get("opt_Z", None)
        self.opt_V = kwargs.get("opt_V", None)
        self.opt_u = kwargs.get("opt_u", None)
        self.sim0 = kwargs.get("sim0", None)
        self.opt_R = kwargs.get("opt_R", None)
        self.hist_dir = kwargs.get("histDir", None)
        self.nlat = kwargs.get("nLat", None)
        self.nlon = kwargs.get("nLon", None)
        self.frexml = kwargs.get("absXmlPath", None)
        self.stdoutdir = kwargs.get("stdoutDir", None)
        self.opt_P = kwargs.get("opt_P", None)
        self.opt_T = kwargs.get("stdTarget", None)
        self.opt_s = kwargs.get("opt_s", None)

        # ----------------------------------------------------------------------

    def graindate(self, date, freq):
        raise NotImplementedError()

    def padzeros(self, date):
        raise NotImplementedError()

    def writescript(self, out, mode, outscript, argu, opt_s):
        raise NotImplementedError()

    def availablechunks(self):
        raise NotImplementedError()

    def checkmissingchunks(self, databegyr, dataendyr, clnumber, pt,
        availablechunksfirst_ref):
        """Now check for the missing chunks."""
        raise NotImplementedError()

    def filltemplate(self, arrayofExptsH_ref, figureDir, aScript, aargu,
        aScriptout, iExpt, workdir, mode, asrcfile, opt_s, opt_u, opt_V,
        frexml, stdoutdir, platform, target, experID, realizID, runID):
        """Fill the template with the passing variables."""
        raise NotImplementedError()

    def adjYearlow(self, edgeYear, chunkedge):
        """Adjust data-begin-year to the beginning/ending of a data chunk."""
        raise NotImplementedError()

    def adjYearhigh(self, edgeYear, chunkedge):
        """Adjust data-begin-year to the beginning/ending of a data chunk."""
        raise NotImplementedError()

    def queueAnaAttr(self, anaNode):
        """"""
        raise NotImplementedError()

    def seasonAV(self, frequency):
        raise NotImplementedError()

    def start_end_date(self, astartYear, aendYear, opt_Y, opt_Z,
        availablechunksfirst_ref, availablechunkslast_ref, clnumber):
        raise NotImplementedError()

    def getxpathval(self, path):
        """Gets a value from xml, recurse using @inherit and optional second
        argument expt."""
        raise NotImplementedError()

    def checkExptExists(self, e):
        """Make sure experiment exists in xml."""
        raise NotImplementedError()

    def anodenum(self, tNode):
        """Return the number of analysis nodes under the given Node."""
        raise NotImplementedError()

    def acarch(self, cmd):
        """Manipulate /archive."""
        raise NotImplementedError()

    @staticmethod
    def cleanpath(str_):
        """Clean up a string that should be a filepath."""
        raise NotImplementedError()
