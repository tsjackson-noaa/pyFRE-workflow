"""Transliteration of FRE/lib/FRE.pm.
"""
import os
from pyFRE.util.pyfre import is_readable
import sys
import collections
import dataclasses as dc
import re
from textwrap import dedent
import xml.etree.ElementTree as ET
from xml.etree import ElementInclude

from . import FREDefaults, FREPlatforms, FREProperties, FRETargets, FREUtil
import pyFRE.util as util

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
    def new(cls, caller, opt):
        """Read a FRE XML tree from a file, check for basic errors, return the
        FRE object."""
        # FRE.pm l.418
        fre = FRE()
        xmlfileAbsPath = os.path.abspath(opt['xmlfile'])
        if os.path.isfile(xmlfileAbsPath) and util.is_readable(xmlfileAbsPath):
            _log.info(f"The '{caller}' begun using the XML file '{xmlfileAbsPath}'...")

            # validate and load the configuration file
            # --novalidate option is not advertised
            if opt.get('novalidate', False):
                rootNode = cls.xmlLoad(xmlfileAbsPath)
            else:
                # XXX equivalent of xmlValidateAndLoad? uses schema?
                rootNode = cls.xmlValidateAndLoad(xmlfileAbsPath)
            if rootNode:
                # if platform isn't specified or contains default, print a descriptive message and exit.
                # let frelist go ahead, setting platform to first available, if no options are specified
                # so it can print experiments and if -d is used so it can list experiment descriptions
                if caller == 'frelist' and len(opt) <= 4:
                    opt['platform'] = rootNode.findnodes('setup/platform[@name]')->get_node(1)->getAttribute('name')
                else:
                    FREPlatforms.checkPlatform(opt['platform'])
            else:
                _log.error(f"The XML file '{xmlfileAbsPath}' can't be parsed")
                return None
        else:
            _log.error(f"The XML file '{xmlfileAbsPath}' doesn't exist or isn't readable")
            return None

        version = cls.versionGet(rootNode)

        # standardize the platform string and verify its correctness
        platformSite, platformTail = FREPlatforms.parse(opt['platform'])
        if platformSite:
            opt['platform'] = f"{platformSite}.{platformTail}"

            # verify availability of the platform site
            if platformSite in FREDefaults.Sites():
                # verify locality of the platform site
                if FREPlatforms.siteIsLocal(platformSite) or caller == 'frelist':
                    # standardize the target string
                    opt['target'], targetErrorMsg = FRETargets.standardize(opt['target'])
                    if opt['target']:
                        # initialize properties object (properties expansion happens here)
                        siteDir = FREPlatforms.siteDir(platformSite)
                        properties = FREProperties.new(rootNode, siteDir, opt)
                        if properties:
                            properties = properties.propertiesList(opt['verbose'])
                            # locate the platform node (no backward compatibility anymore)
                            platformNode = cls.platformNodeGet(rootNode)
                            if platformNode:
                                # create the object
                                # save caller name and global options in the object
                                fre.caller = caller
                                fre.platformSite = platformSite
                                fre.platform = opt['platform']
                                fre.target = opt['target']
                                fre.verbose = opt['verbose']
                                # save calculated earlier values in the object
                                fre.xmlfileAbsPath = xmlfileAbsPath
                                fre.rootNode = rootNode
                                fre.version = version
                                fre.siteDir = siteDir
                                fre.properties = properties
                                fre.platformNode = platformNode

                                # calculate and save misc values in the object
                                fre.project = fre.projectGet(opt['project'])
                                fre.freVersion = fre.platformValue('freVersion')
                                fre.compiler = fre.platformValue('compiler/@type')
                                fre.baseCsh = fre.default_platform_csh() + fre.platformValue('csh')
                                if opt.get('mail-list', False):
                                    fre.mailList = opt['mail-list']
                                elif fre.property_('FRE.mailList.default'):
                                    fre.mailList = fre.property_('FRE.mailList.default')
                                else:
                                    _log.error(("Required FRE property FRE.mailList.default "
                                        "doesn't exist; contact your local FRE support team"))
                                    return None

                                # derive the mkmf template
                                mkmfTemplate = fre.mkmfTemplateGet(caller, platformNode)
                                if mkmfTemplate:
                                    fre.mkmfTemplate = mkmfTemplate
                                    # verify compatibility of base <csh> with targets
                                    if fre.baseCshCompatibleWithTargets():
                                        # read setup-based info (for compatibility only)
                                        fre.getFmsData = fre.infoGet('setup/getFmsData')
                                        fre.fmsRelease = fre.infoGet('setup/fmsRelease')
                                        # read experiment nodes and names
                                        expNodes = rootNode.findnodes('experiment')
                                        expNames = [
                                            (lambda n: fre.nodeValue(n, '@name') if fre.nodeValue(n, '@name') else fre.nodeValue(n, '@label')) \
                                            for n in expNodes
                                        ]
                                        # check experiment names uniqueness
                                        expNamesDuplicated = FREUtil.listDuplicates(expNames)
                                        if not expNamesDuplicated:
                                            # save experiment names and nodes in the object
                                            fre.expNode = dict(zip(expNames, expNodes))
                                            fre.expNames = ' '.join(expNames)
                                            # print what we got
                                            _log.info(dedent(f"""
                                                siteDir        = {fre.siteDir},
                                                platform       = {fre.platform},
                                                target         = {fre.target},
                                                project        = {fre.project},
                                                mkmfTemplate   = {fre.mkmfTemplate},
                                                freVersion     = {fre.freVersion}
                                            """))
                                            # normal return
                                            return fre
                                        else:
                                            expNamesDuplicated = ' '.join(expNamesDuplicated)
                                            _log.error(f"Experiment names aren't unique: '{expNamesDuplicated}'")
                                            return None
                                    else: ## end if ($platformNode)
                                        _log.error("Mismatch between the platform <csh> and the target option value")
                                        return None
                                else:
                                    _log.error("A problem with the mkmf template")
                                    return None
                            else: ## end if ($properties)
                                _log.error(f"The platform with name '{opt['platform']}' isn't defined")
                                return None
                        else:
                            _log.error(f"A problem with the XML file '{xmlfileAbsPath}'")
                            return None
                    else: ## end if ( $o{target} )
                        _log.error(targetErrorMsg)
                        return None
                else: ## end if ( FREPlatforms::siteIsLocal...)
                    _log.error(f"You are not allowed to run the '{caller}' tool with the "
                        f"'{opt['platform']}' platform on this site")
                    return None
            else: ## end if ( scalar( grep( $_ ...)))
                sites = ', '.join(FREDefaults.Sites())
                _log.error(f"The site '{platformSite}' is unknown. Known sites are '{sites}'")
                return None
        else: ## end if ($platformSite)
            _log.error(f"The --platform option value '{opt['platform']}' is not valid")
            return None


    # Private methods ------------------------------------------------------------

    @classmethod
    def xmlLoad(cls, xmlfile):
        """Return the loaded document."""
        # FRE.pm l.83
        try:
            document = ET.parse(xmlfile)
            root = document.getroot()
            ElementInclude.include(root)
            return root
        except Exception as exc:
            _log.error(repr(exc))
            return None

    @classmethod
    def xmlValidateAndLoad(cls, xmlfile):
        """Return the loaded document."""
        # FRE.pm l.101
        return cls.validate(cls.xmlLoad(xmlfile))

    @classmethod
    def versionGet(cls, rootNode):
        # FRE.pm l.115
        """Return the (modified) version number."""
        version = rootNode.findvalue('@rtsVersion')
        if not version:
            versionDefault = VERSION_DEFAULT
            _log.warning((f"rtsVersion information isn't found in your configuration file. "
                f"Assuming the lowest rtsVersion={versionDefault}  A newer version is available..."))
            version = versionDefault
        elif version < VERSION_CURRENT:
            _log.warning("You are using obsolete rtsVersion.  A newer version is available...")
        elif version == VERSION_CURRENT:
            _log.info(f"You are using rtsVersion={VERSION_CURRENT}")
        else:
            _log.warning((f"rtsVersion $version is greater than latest default version "
                f"{VERSION_CURRENT}. Assuming the rtsVersion={VERSION_CURRENT}"))
            version = VERSION_CURRENT
        return version

    @classmethod
    def platformNodeGet(cls, rootNode):
        """Return the platform node."""
        # FRE.pm l.148
        nodes = rootNode.findall('setup/platform')
        if len(nodes) == 1:
            return nodes[0]
        else:
            return ""

    def infoGet(self, xPath):
        """Return a piece of info, starting from the root node."""
        # FRE.pm l.158
        nodes = self.rootNode.findnodes(xPath)
        if nodes:
            if len(nodes) > 1:
                _log.warning((f"The '{xPath}' path defines more than one data item - "
                    "all the extra definitions are ignored"))
            info = self.nodeValue(nodes[0], '.')
            info = re.sub(r'(?:^\s*|\s*$)', r"", info)
            infoList = re.split(r'\s+', info)
            if infoList:
                if len(infoList) > 1:
                    _log.warning((f"The '{xPath}' path defines the multi-piece data item '{info}'"
                        " - all the pieces besides the first one are ignored"))
                return infoList[0]
            else:
                return ""
        else:
            return ""

    def mkmfTemplateGet(self, caller, platformNode):
        """Return the mkmfTemplate file, defined on the platform level."""
        # FRE.pm l.187
        if caller == 'fremake':
            mkmfTemplates = self.dataFilesMerged(platformNode, 'mkmfTemplate', 'file')
            if mkmfTemplates:
                if len(mkmfTemplates) > 1:
                    _log.warning(("The platform mkmf template is defined more than "
                        "once - all the extra definitions are ignored"))
                return mkmfTemplates[0]
            else:
                mkFilename = self.compiler + '.mk'
                if mkFilename == '.mk':
                    mkFilename = self.property_('FRE.tool.mkmf.template.default')
                    _log.warning(("The platform mkmf template can't be derived from "
                        f"the <compiler> tag - using the default template '{mkFilename}'"))
                return f"{self.siteDir}/{mkFilename}"
        else:
            return 'NULL'

    def baseCshCompatibleWithTargets(self):
        # FRE.pm l.217
        versionsMapping = self.property_('FRE.tool.make.override.netcdf.mapping')
        baseCshNetCDF4 = (FREUtil.strFindByPattern(versionsMapping, self.baseCsh()) == 4)
        targetListHdf5  = FRETargets.containsHDF5(self.target)
        if baseCshNetCDF4 or targetListHdf5:
            return True
        else:
            _log.error(("Your platform <csh> is configured for netCDF3 - so you "
                "aren't allowed to have 'hdf5' in your targets"))
            return False

    def projectGet(self, project):
        # FRE.pm l.236
        if not project:
            project = self.platformValue('project')
            if not project and self.property_('FRE.project.required'):
                _log.fatal(("Your project name is not specified and is required on "
                    "this site; please correct your XML's platform section."))
                sys.exit(FREDefaults.STATUS_FRE_GENERIC_PROBLEM)
            elif not project:
                return ""
        else:
            return project

    # Class methods ------------------------------------------------------------

    @classmethod
    def home(cls):
        # FRE.pm l.259
        return FREUtil.home()

    @classmethod
    def curator(cls, xmlFile, expName):
        # FRE.pm l.269
        root = cls.xmlLoad(xmlFile)
        experimentNode = root.findnodes("experiment[\@label='$expName' or \@name='$expName']")->get_node(1)
        publicMetadataNode = experimentNode.findnodes("publicMetadata")->get_node(1);

        if publicMetadataNode:
            document = ET.fromstring(publicMetadataNode)
            documentURI = "publicMetadata"
            document.setURI(documentURI);

            if cls.validate(document, verbose=True, curator=True):
                return
            else:
                _log.fatal(("CMIP Curator tags are not valid; see CMIP metadata tag "
                    "documentation at http://cobweb.gfdl.noaa.gov/~pcmdi/CMIP6_Curator/xml_documentation"))
                sys.exit(FREDefaults.STATUS_FRE_GENERIC_PROBLEM)
        else:
            _log.fatal(("No CMIP Curator tags found; see CMIP metadata tag documentation "
                "at http://cobweb.gfdl.noaa.gov/~pcmdi/CMIP6_Curator/xml_documentation"))
            sys.exit(FREDefaults.STATUS_FRE_GENERIC_PROBLEM)

    @classmethod
    def validate(cls, document, verbose=True, curator=False):
        # FRE.pm l.306
        if curator:
            validateWhat = 'publicMetadata'
            schemaName   = 'curator.xsd'
        else:
            validateWhat = document
            schemaName   = 'fre.xsd'
            if isinstance(document, str):
                document = cls.xmlLoad(document)
        if document:
            schemaLocation = os.path.join(cls.home(), 'etc', 'schema', schemaName)
            if os.path.isfile(schemaLocation) and util.is_readable(schemaLocation):
                schema = # XXX XML::LibXML::Schema->new( location => $schemaLocation );
                # XXX eval { $schema->validate($document) };
            if XXX:
                _log.info(f"The XML file '{validateWhat}' has been successfully validated")
                return True
            else:
                cls._print_validation_errors(validateWhat, schemaLocation)
                _log.error(f"The XML file '{validateWhat}' is not valid")
                return False
        else:
            _log.error(f"The XML file '{validateWhat}' can't be parsed")
            return False

    @classmethod
    def _print_validation_errors(cls, xml, schema):
        # FRE.pm l.350
        """Returns nothing, prints report."""
        # Run xmllint, which is in the libxml2 module which is loaded by FRE
        # the validation errors are in standard error
        xmllint_output = util.shell(f'xmllint --xinclude --schema {schema} --xinclude --noout {xml} 2>&1')
        # the last line just says "fails to validate" which we already know
        xmllint_output = xmllint_output.split('\n')[:-1]

        # Collect the errors
        xmlerrors = collections.defaultdict
        for out_line in xmllint_output:
            file_, line_, _, _, message = out_line.split(':', maxsplit=5)
            message = re.sub(r'^ +', r'', message)

        # XXX FINISH XXX

    # Object methods -----------------------------------------------------------

    def xmlAsString(self):
        """Return the XML file with entities expanded."""
        # FRE.pm l.731
        return f'<?xml version="1.0"?>\n{ET.tostring(self.rootNode)}'

    def experimentNames(self):
        """Return list of experiment names."""
        # FRE.pm l.741
        return self.expNames.split(' ')

    def experimentNode(self, expName):
        # FRE.pm l.751
        return self.expNodes.expName

    def dataFiles(self, data_node, label):
        """Return a list of datafiles with targets."""
        # FRE.pm l.761
        results = []
        nodes = data_node.findnodes(f'dataFile[@label="{label}"]')
        for node in nodes:
            sourcesCommon = self.nodeValue(node, 'text()')
            sourcesPlatform = self.nodeValue(node, 'dataSource/text()')
            sources = re.split(r'\s+', f"{sourcesCommon}\n{sourcesPlatform}")
            target = self.nodeValue(node, '@target')
            for fileName in sources:
                if not fileName:
                    continue
                if fileName not in results:
                    results.append((fileName, target))
                    if not (os.path.isfile(fileName) and util.is_readable(fileName)):
                        line_ = node.line_number()
                        _log.warning((f"XML file line {line_}: the {label} file '{fileName}'"
                            "isn't accessible or doesn't exist"))
                    if re.match(r'^\/lustre\/fs|^\/lustre\/ltfs', fileName):
                        line_ = node.line_number()
                        _log.warning((f"XML file line {line_}: the {label} file '{fileName}' "
                            "is on a filesystem scheduled to be unmounted soon. Please move this data."))
                else:
                    line_ = node.line_number()
                    _log.warning((f"XML file line {line_}: the {label} file '{fileName}' is "
                        "defined more than once - all the extra definitions are ignored"))
        return results

    def dataFilesMerged(self, data_node, label, attrName):
        """Return a list of datafiles, merged with list of files in the
        <$label/@$attrName> format, without targets."""
        # FRE.pm l.808
        resultsFull = self.dataFiles(data_node, label)
        results = [tup[0] for tup in resultsFull]

        nodesForCompatibility = data_node.findnodes(f"{label}/@{attrName}")
        for node in nodesForCompatibility:
            fileName = self.nodeValue(node, '.')
            if not fileName:
                continue
            if fileName not in results:
                results.append(fileName)
                if not (os.path.isfile(fileName) and util.is_readable(fileName)):
                    line_ = node.line_number()
                    _log.warning((f"XML file line {line_}: the {label} {attrName} file '{fileName}'"
                        "isn't accessible or doesn't exist"))
                if re.match(r'^\/lustre\/fs|^\/lustre\/ltfs', fileName):
                    line_ = node.line_number()
                    _log.warning((f"XML file line {line_}: the {label} {attrName} file '{fileName}' "
                        "is on a filesystem scheduled to be unmounted soon. Please move this data."))
            else:
                line_ = node.line_number()
                _log.warning((f"XML file line {line_}: the {label} {attrName} file '{fileName}' is "
                    "defined more than once - all the extra definitions are ignored"))
        return results

    def configFileAbsPathName(self):
        """Return the absolute pathname of the XML config file."""
        # FRE.pm l.862
        return self.xmlfileAbsPath

    def placeholdersExpand(self, str_):
        """Expand all the global level placeholders in the given $string. The
        placeholder $name is expanded here after the setCurrentExperimentName call."""
        # FRE.pm l.808
        if self.name:
            v = self.name
            str_ = re.sub(r'\$(?:\(name\)|\{name\}|name)', rf'{v}', str_)
        return str_

    def property_(self, propertyName):
        """Return the external property value."""
        # FRE.pm l.973
        return self.placeholdersExpand(self.properties[propertyName])

    def propertyParameterized(self, propertyName, values):
        """Return the external property value, where all the '$' are replaced by
        @values."""
        # FRE.pm l.983
        s = self.placeholdersExpand(self.properties[propertyName])
        pos = 0
        i = 0
        while True:
            index_ = s.find('$', pos)
            if index_ < 0:
                break # substring not present
            value = values[i]
            if value:
                s = s[:index_] + value[0] + s[index_+1:] # XXX check indices
                pos = index_ + len(value)
                i += 1
            else:
                s = ""
        return s

    def nodeValue(self, node, xPath):
        """Return $xPath value relative to $node."""
        # FRE.pm l.1008
        return self.placeholdersExpand(' '.join([n.findvalue('.') for n in node.findnodes(xPath)]))

    def platformValue(self, xPath):
        """Return $xPath value relative to <setup/platform> node."""
        # FRE.pm l.1018
        return self.nodeValue(self.platformNode, xPath)

    def runTime(self, npes):
        """Return maximum runtime for $npes."""
        # FRE.pm l.1028
        return FREUtil.strFindByInterval(self.property_('FRE.scheduler.runtime.max'), npes)

    def mailMode(self):
        """Return mail mode for the batch scheduler."""
        # FRE.pm l.1038
        m = os.environ.get(MAIL_MODE_VARIABLE, '')
        if m:
            if re.match(r'^(?:none|begin|end|fail|requeue|all)$', m.lower()):
                return m
            else:
                _log.warning((f"The environment variable '{MAIL_MODE_VARIABLE}' has "
                    f"wrong value '{m}' - ignored..."))
                return MAIL_MODE_DEFAULT
        else:
            return MAIL_MODE_DEFAULT

    def out(self, level=None, strings=""):
        """Output @strings provided that the 0 <= $level <= $verbose + 1."""
        # FRE.pm l.1062
        # call logger directly instead
        raise NotImplementedError()

    def default_platform_csh(self):
        """Reads the site and compiler-specific default environment file. Replaces
        compiler version and fre version. Returns string containing default platform
        environment c-shell."""
        # FRE.pm l.1075

        # get compiler type and version
        if self.platformSite == 'gfdl':
            compiler = dict()
        else:
            compiler_node = self.platformNode.getChildrenByTagName('compiler')
            if not compiler_node:
                _log.error("Compiler type and version must be specified in XML platform <compiler> tag")
                sys.exit(FREDefaults.STATUS_FRE_GENERIC_PROBLEM)
            type_ = compiler_node.getAttribute('type')
            version_ = compiler_node.getAttribute('version')
            if not (type_ and version_):
                _log.error("Compiler type and version must be specified in XML platform <compiler> tag")
                sys.exit(FREDefaults.STATUS_FRE_GENERIC_PROBLEM)
            compiler = {'type': type_, 'version': version_}

        # read platform environment site file
        env_defaults_file = # XXX

        # XXX FINISH

    def check_for_fre_version_mismatch(self):
        """Checks for consistency between the fre version in the platform xml
        section and the current shell environment. Exits with error if different,
        returns nothing."""
        # FRE.pm l.1128


        raise NotImplementedError()

