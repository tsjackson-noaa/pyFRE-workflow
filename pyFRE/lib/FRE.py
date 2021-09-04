"""Transliteration of FRE/lib/FRE.pm.
"""

import dataclasses as dc
import xml.etree.ElementTree as ET

from . import FREDefaults, FREUtil

import logging
_log = logging.getLogger(__name__)

VERSION_DEFAULT = 1
VERSION_CURRENT = 4

MAIL_MODE_VARIABLE = 'FRE_SYSTEM_MAIL_MODE'
MAIL_MODE_DEFAULT  = 'fail'

@dc.dataclass
class FRE():
    name: str = None
    experID: str = None
    realizID: str = None
    runID: str = None
    rootNode: str = None
    expNames: str = None
    expNodes: str = None
    version: str = None
    xmlfileAbsPath: str = None
    platformSite: str = None
    platform: str = None
    target: str = None
    siteDir: str = None
    project: str = None
    mkmfTemplate: str = None
    baseCsh: str = None
    getFmsData: str = None
    fmsRelease: str = None

    properties: dict = dc.field(default_factory=dict)

    @classmethod
    def new(cls, caller, options):
        """Read a FRE XML tree from a file, check for basic errors, return the
        FRE object."""

        fre = FRE()

        pass


    # Private methods ------------------------------------------------------------

    @classmethod
    def _xmlLoad(cls, xmlfile, verbose):
        """Return the loaded document."""
        raise NotImplementedError()

    @classmethod
    def _xmlValidateAndLoad(cls, xmlfile, verbose):
        """Return the loaded document."""
        raise NotImplementedError()

    @classmethod
    def _versionGet(cls, rootNode, verbose):
        """Return the (modified) version number."""
        raise NotImplementedError()

    @classmethod
    def _platformNodeGet(cls, rootNode):
        """Return the platform node."""
        nodes = rootNode.findall('setup/platform')
        if len(nodes) == 1:
            return nodes[0]
        else:
            return ""

    def _infoGet(self, xPath, verbose):
        """Return a piece of info, starting from the root node."""
        raise NotImplementedError()

    def _mkmfTemplateGet(self, caller, platformNode, verbose):
        """Return the mkmfTemplate file, defined on the platform level."""
        raise NotImplementedError()

    def _baseCshCompatibleWithTargets(self, verbose):
        raise NotImplementedError()

    def _projectGet(self, project):
        raise NotImplementedError()

    # Class methods ------------------------------------------------------------

    @classmethod
    def home(cls):
        return FREUtil.home()

    @classmethod
    def curator(cls, x, expName, v):
        raise NotImplementedError()

    @classmethod
    def validate(cls, *args):
        raise NotImplementedError()

    @classmethod
    def _print_validation_errors(cls, xml, schema):
        """Returns nothing, prints report."""
        raise NotImplementedError()

    # Object methods -----------------------------------------------------------

    def xmlAsString(self):
        """Return the XML file with entities expanded."""
        return ''.join('<?xml version="1.0"?>', "\n", ET.tostring(self.rootNode))

    def experimentNames(self):
        """Return list of experiment names."""
        return self.expNames.split(' ')

    def experimentNode(self, expName):
        return self.expNodes.expName

    def dataFiles(self, node, label):
        """Return a list of datafiles with targets."""
        raise NotImplementedError()

    def dataFilesMerged(self, node, label, attrName):
        """Return a list of datafiles, merged with list of files in the
        <$label/@$attrName> format, without targets."""
        raise NotImplementedError()

    def configFileAbsPathName(self):
        """Return the absolute pathname of the XML config file."""
        return self.xmlfileAbsPath

    def placeholdersExpand(self, string):
        """Expand all the global level placeholders in the given $string. The
        placeholder $name is expanded here after the setCurrentExperimentName call."""
        raise NotImplementedError()

    def property_(self, propertyName):
        """Return the external property value."""
        return self.placeholdersExpand(self.properties[propertyName])

    def propertyParameterized(self, propertyName, values):
        """Return the external property value, where all the '$' are replaced by
        @values."""
        raise NotImplementedError()

    def nodeValue(self, node, xPath):
        """Return $xPath value relative to $node."""
        raise NotImplementedError()

    def platformValue(self, xPath):
        """Return $xPath value relative to <setup/platform> node."""
        raise NotImplementedError()

    def runTime(self, npes):
        """Return maximum runtime for $npes."""
        raise NotImplementedError()

    def mailMode(self):
        """Return mail mode for the batch scheduler."""
        raise NotImplementedError()

    def out(self, level=None, strings=""):
        """Output @strings provided that the 0 <= $level <= $verbose + 1."""
        if level is None:
            level = self.verbose
        _log.log(level, strings)

    def default_platform_csh(self):
        """Reads the site and compiler-specific default environment file. Replaces
        compiler version and fre version. Returns string containing default platform
        environment c-shell."""
        raise NotImplementedError()

    def check_for_fre_version_mismatch(self):
        """Checks for consistency between the fre version in the platform xml
        section and the current shell environment. Exits with error if different,
        returns nothing."""
        raise NotImplementedError()

