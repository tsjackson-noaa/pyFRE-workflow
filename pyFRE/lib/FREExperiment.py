"""Transliteration of FRE/lib/FREExperiment.pm.
"""

import dataclasses as dc
from typing import Any

from . import FRE, FREDefaults, FRETargets, FREUtil

import logging
_log = logging.getLogger(__name__)

# Global constants -------------------------------------------------------------

DIRECTORIES = FREDefaults.ExperimentDirs()
REGRESSION_SUITE = ['basic', 'restarts', 'scaling']

# Module-scope variables -------------------------------------------------------

global FREExperimentMap # XXX need to carry this around in FREpp?
FREExperimentMap = dict()

# Utilities --------------------------------------------------------------------

def _experimentFind(expName):
    return FREExperimentMap.get(expName, '')

def _experimentCreate(className, fre, expName):
    raise NotImplementedError()

def _strMergeWS(string):
    """Merge all the workspaces to a single space."""
    raise NotImplementedError()

def _strRemoveWS(string):
    """Remove all the workspaces."""
    raise NotImplementedError()

def _rankSet(refToComponentHash, refToComponent, depth):
    """Recursively set and return the component rank. Return -1 if loop is found."""
    raise NotImplementedError()

def _MPISizeCompatible(fre, namelistsHandle):
    raise NotImplementedError()

def _long_component_names(fre):
    """Returns a hash whose keys are the 3-letter standard component names
    and value is the legacy/long name. The only use for the long names is
    do_atmos = 1 style coupler namelist entries."""
    short = fre.property_('FRE.mpi.component.names').split(';')
    long_ = fre.property_('FRE.mpi.component.long_names').split(';')
    return dict(zip(short, long_))

# ------------------------------------------------------------------------------


@dc.dataclass
class FREExperiment():
    name: str = ""
    rootDir: str = ""
    srcDir: str = ""
    execDir: str = ""
    scriptsDir: str = ""
    stdoutDir: str = ""
    stdoutTmpDir: str = ""
    stateDir: str = ""
    workDir: str = ""
    ptmpDir: str = ""
    archiveDir: str = ""
    postProcessDir: str = ""
    analysisDir: str = ""
    includeDir: str = ""

    # additional configuration that was stored globally in frepp.pl
    expt: str = ""
    basedate: str = ""
    cshscripttmpl: str = ""
    diagtablecontent: str = ""
    batch_job_name: str = ""
    version_info: str = ""
    this_frepp_cmd: str = ""
    mkdircommand: str = ""
    statefile: str = ""
    frepp_plus_calls: list = dc.field(default_factory=list)

    aggregateTS: bool = True

    # NB: paths below are not necessarily the same as their *Dir counterparts above
    workdir: str = ""
    archivedir: str = ""
    postprocessdir: str = ""
    analysisdir: str = ""
    scriptsdir: str = ""
    stdoutdir: str = ""
    tmphistdir: str = ""
    tempCache: str = ""
    statedir: str = ""
    outscriptdir: str = ""
    aoutscriptdir: str = ""
    histDir: str = ""
    refinedir: str = ""
    outscript: str = ""
    ppRootDir: str = ""


    def __post_init__(self):
        # using new() for creation anyway, so use this to set defaults for
        # fields we don't want in templating dict
        self.fre = None
        self.node = None
        self.parent = None

        self.ppNode = None

    def template_dict(self):
        """Dict of all configuration key:values for templating .csh fragments."""
        return dc.asdict(self)

    @classmethod
    def new(cls, className, fre, expName):
        return _experimentCreate(className, fre, expName)


    # Private methods -----------------------------------------------------------

    def _experimentDirsCreate(self):
        for t in DIRECTORIES:
            dirName = t + 'Dir'
            setattr(object, dirName, object.property_(dirName))

    def _experimentDirsVerify(self, expName):
        raise NotImplementedError()

    def _regressionLabels(self):
        raise NotImplementedError()

    def _regressionRunNode(self, label):
        raise NotImplementedError()

    def _productionRunNode(self):
        raise NotImplementedError()

    def _extractOverrideParams(self, mamelistsHandle, runNode):
        raise NotImplementedError()

    def _overrideRegressionNamelists(self, namelistsHandle, runNode):
        raise NotImplementedError()

    def _overrideProductionNamelists(self, namelistsHandle):
        raise NotImplementedError()

    def _MPISizeParametersCompatible(self, resources, namelistsHandle, ensembleSize):
        raise NotImplementedError()

    def _MPISizeComponentEnabled(self, namelistsHandle, componentName):
        raise NotImplementedError()

    def _MPISizeParametersGeneric(self, resources, namelistsHandle, ensembleSize):
        raise NotImplementedError()

    def _MPISizeParameters(self, resources, namelistsHandle):
        raise NotImplementedError()

    def _regressionPostfix(self, label, runNo, hoursFlag, segmentsNmb, monthsNmb,
        daysNmb, hoursNmb, mpiInfo):
        raise NotImplementedError()

    # Object methods -----------------------------------------------------------

    def placeholdersExpand(self, string):
        """Expand all the experiment level placeholders in the given $string."""

    def property_(self, propertyName):
        """Return the value of the property $propertyName, expanded on the
        experiment level."""
        return self.placeholdersExpand(self.fre.property_(propertyName))

    def nodeValue(self, node, xPath):
        """Return $xPath value relative to the given $node."""
        return self.placeholdersExpand(self.fre.nodeValue(node, xPath))

    def experimentValue(self, xPath):
        """Return $xPath value relative to the experiment node."""
        return self.nodeValue(self.node, xPath)

    def description(self):
        """Returns the experiment description."""
        return self.experimentValue('description')

    def executable(self):
        """Return standard executable name for the given experiment."""
        return f"{self.execDir}/fms_{self.name}.x"

    def executableCanBeBuilt(self):
        """Return 1 if the executable for the given experiment can be built."""
        return bool(self.experimentValue('*/source/codeBase') \
            or self.experimentValue('*/source/csh') \
            or self.experimentValue('*/compile/cppDefs') \
            or self.experimentValue('*/compile/srcList') \
            or self.experimentValue('*/compile/pathNames') \
            or self.experimentValue('*/compile/csh')
        )

    # Data Extraction With Inheritance -----------------------------------------

    def extractNodes(self, xPathRoot, xPathChildren):
        """Return a nodes list corresponding to the $xPathRoot/$xPathChildren,
        following inherits. If xPathRoot returns a list of nodes, only the first
        node will be taken into account."""
        exp = self
        results = []
        while (exp and (len(results) == 0)):
            rootNode = exp.node.findnodes(xPathRoot)->get_node(1) # XXX
            if rootNode:
                results.append(rootNode.findnodes(xPathChildren))
            exp = exp.parent
        return results

    def extractValue(self, xPath):
        """Return a value corresponding to the $xPath, following inherits."""
        exp = self
        value = ''
        while (exp and not value):
            value = exp.experimentValue(xPath)
            exp = exp.parent
        return value

    def extractComponentValue(self, xPath, componentName):
        """Return a value corresponding to the $xPath under the <component> node,
        following inherits."""
        return self.extractValue(f'component[@name="{componentName}"]/{xPath}')

    def extractSourceValue(self, xPath, componentName):
        """Return a value corresponding to the $xPath under the <component/source>
        node, following inherits."""
        return self.extractValue(f'component[@name="{componentName}"]/source/{xPath}')

    def extractCompileValue(self, xPath, componentName):
        """Return a value corresponding to the $xPath under the <component/compile>
        node, following inherits."""
        return self.extractValue(f'component[@name="{componentName}"]/compile/{xPath}')

    def extractDoF90Cpp(self, xPath, componentName):
        """Return a value corresponding to the $xPath under the <component/compile>
        node, following inherits."""
        raise NotImplementedError()

    def extractExecutable(self):
        """Return predefined executable name (if found) and experiment object,
        following inherits"""
        exp = self
        results = []
        while exp:
            makeSenseToCompile = exp.executableCanBeBuilt()
            results = self.fre.dataFilesMerged(exp.node, 'executable', 'file')
            if results or makeSenseToCompile:
                break
            exp = exp.parent

        if results:
            if len(results) > 1:
                _log.warning(("The executable name is predefined more than once "
                    "- all the extra definitions are ignored"))
            return (results[0], exp)
        elif makeSenseToCompile:
            return (None, exp)
        else:
            return (None, None)

    def extractMkmfTemplate(self, componentName):
        """Extracts a mkmf template, following inherits."""
        exp = self
        results = []
        while (exp and not results):
            nodes = exp.node.findall(f'component[@name="{componentName}"]/compile')
            for node in nodes:
                results.append(self.fre.dataFilesMerged(node, 'mkmfTemplate', 'file'))
            exp = exp.parent

        if len(results) > 1:
            _log.warning((f"The '{componentName}' component mkmf template is "
                "defined more than once - all the extra definitions are ignored"))
        return results[0]

    def extractDatasets(self):
        """Extracts file pathnames together with their target names, following
        inherits."""
        raise NotImplementedError()

    def extractNamelists(self):
        """Returns namelists handle, following inherits, but doesn't overwrite
        existing hash entries."""
        raise NotImplementedError()

    def extractTable(self, label):
        """Returns data, corresponding to the $label table, following inherits."""
        raise NotImplementedError()

    def extractShellCommands(self, xPath, adjustment):
        """Returns shell commands, corresponding to the $xPath, following inherits.
        Adjusts commands, depending on node types."""
        exp = self
        value = ''
        while (exp and not value):
            nodes = exp.node.findnodes(xPath)
            for node in nodes:
                type_ = exp.nodeValue(node, '@type')
                content = exp.nodeValue(node, 'text()')

                #if ( exists( $a{$type} ) ) { $content = $a{$type}[0] . $content . $a{$type}[1]; }
                value = value + content
            exp = exp.parent
        return value

    def extractVariableFile(self, label):
        """Returns filename for the $label variable, following inherits."""
        exp = self
        results = []
        while (exp and not results):
            inputNode = exp.node.findnodes('input')->get_node(1) # XXX
            if inputNode:
                results.append(self.fre.dataFilesMerged(inputNode, label, 'file' ))
            exp = exp.parent

        if len(results) > 1:
            _log.warning((f"The variable '{label}'  is defined more than once - "
                "all the extra definitions are ignored"))
        return results[0]

    def extractReferenceFiles(self):
        """Return list of reference files, following inherits."""
        exp = self
        results = []
        while (exp and not results):
            runTimeNode = exp.node.findnodes('runtime')->get_node(1) # XXX
            if runTimeNode:
                results.append(self.fre.dataFilesMerged(runTimeNode, 'reference', 'restart'))
            exp = exp.parent
        return results

    def extractReferenceExperiments(self):
        """Return list of reference experiment names, following inherits."""
        results = []
        nodes = self.extractNodes('runtime', 'reference/@experiment')
        for node in nodes:
            results.append(self.nodeValue(node, '.'))
        return results

    def extractPPRefineDiagScripts(self):
        """Return list of postprocessing refine diagnostics scriptnames, following
        inherits."""
        raise NotImplementedError()

    def extractCheckoutInfo(self):
        """Return a reference to checkout info, following inherits."""
        raise NotImplementedError()

    def extractCompileInfo(self):
        """Return a reference to compile info."""
        raise NotImplementedError()

    def extractRegressionLabels(self, regressionOption):
        raise NotImplementedError()

    def extractRegressionRunInfo(self, label):
        """Return a reference to the regression run info."""
        raise NotImplementedError()

    def extractProductionRunInfo(self):
        """Return a reference to the production run info."""
        raise NotImplementedError()

    def addResourceRequestsToMpiInfo(self, fre, resources, info):
        """Convenience function used in extractProductionRunInfo and
        extractRegressionRunInfo. MPISizeParameters() generates $mpiInfo from resource
        requests, but a few additional related parameters must be added as well.
        Given the complexity of MPISizeParameters(), those additional related
        parameters are added using this function.
        Returns: nothing, $mpiInfo is changed.
        """
        raise NotImplementedError()

    def getResourceRequests(self, namelists, regression_run_node):
        """Get resource info from <runtime>/.../<resources> tag and decides whether
        hyperthreading will be used.
        Returns: hashref containing resource specs or undef on failure.
        """
        raise NotImplementedError()
