"""Transliteration of main body of FRE/bin/frepp.pl.
"""
import os
from posixpath import dirname
import sys
import dataclasses as dc
import datetime
import email
import functools
import operator as op
import shlex
import shutil
import re
from textwrap import dedent

from pyFRE.lib import FRE, FREAnalysis, FREDefaults, FREExperiment, FRETargets, FREUtil, FREVersion
import pyFRE.util as util
from . import logs, sub, ts_ta

import logging
_log = logging.getLogger(__name__)

_template = util.pl_template # abbreviate


@dc.dataclass
class FREppComponent():
    # XXX finish; align with XML
    component: str = ""
    dtvars: dict = dc.field(default_factory=dict)
    sim0: str = "" # simulation start date
    cshscript: str = ""
    hsmfiles: str = ""
    depyears: str = ""

    cubic: bool = False
    sourceGrid: str = ''
    xyInterp: str = ''
    interpMethod: str = ''
    xyInterpOptions: str = ''
    nlat: int = None
    nlon: int = None

    cpiomonTS: str = ""
    startofrun: bool = False
    didsomething: bool = False

    def ts_ta_update(self, new_cshscript, new_hsmfiles, dep):
        """Add commands and dependent years corresponding to a single requested
        time series or time average.
        """
        self.cshscript += new_cshscript
        if new_hsmfiles is not None: # not passed in static case
            self.hsmfiles = self.hsmfiles + new_hsmfiles + ','
        if dep is not None: # not passed in static case
            self.depyears += dep


@dc.dataclass
class FREpp():
    freVersion: str = ""
    mailList: str = ""
    perlerrors: str = ""
    component: str = ""
    # maxyrs: int = 0 # in FREExperiment
    maxdisk: int = 0
    do_static = True
    historyfiles: str = ""
    # dtvars: # in FREppComponent
    basenpes: int = 1
    # aggregateTS = True # in FREExperiment
    # cpt.didsomething = False # in FREppComponent

    absfrepp: str = ""
    relfrepp: str = ""
    abs_xml_path: str = ""
    createdate: str = ""
    version_head: str = ""

    platform: str = ""

    t0: str = ""
    beginTime: int = 0
    tEnd: str = ""
    userstartyear: int = -1
    userstartmo: int = -1
    hDate: str = ""
    tEnd: str = ""

    writestate: str = ""

    code_root: dc.InitVar = None
    cli_dict: dc.InitVar = None

    def __post_init__(self, code_root, cli_dict):
        # frepp.pl l.123
        self.freVersion = FREVersion.VERSION
        self.absfrepp = os.path.abspath(code_root)
        self.relfrepp = os.path.basename(self.absfrepp)
        opt = {k: cli_dict.get(v, None) for k,v in (
            ("d", "dir"),
            ("f", "forceCombine"),
            ("l", "limitstatic"),
            ("o", "override"),
            ("M", "MailOnComplete"),
            ("m", "resourceManager"),
            ("q", "quiet"),
            ("Q", "debug"),
            ("t", "time"),
            ("r", "runparams"),
            ("x", "xmlfile"),
            ("s", "submit"),
            ("v", "verbose"),
            ("V", "VeryVerbose"),
            ("A", "AnalysisOnly"),
            ("B", "Basedate"),
            ("C", "Calendar"),
            ("O", "OutputFigureDir"),
            ("R", "Regenerate"),
            ("S", "Segment"),
            ("Y", "AnalysisStartYear"),
            ("Z", "AnalysisEndYear"),
            ("c", "component"),
            ("H", "refineDiagOnly"),
            # ("h", "help"),
            ("u", "unique"),
            ("z", "statistics"),
            ("D", "refineDiag"),
            ("w", "wait"),
            ("P", "platform"),
            ("T", "target"),
            # $opt{} values referenced directly below
            ("plus", "plus"),
            ("Walltime", "Walltime"),
            ("refineDiag", "refineDiag"),
            ("mppnccombine_opts", "mppnccombine_opts"),
            ("compress", "compress"),
            ("epmt", "epmt")
        )}

        if opt['r'] and opt['D']:
            _log.warning(("Both options -r and -D are given.  The -r option takes "
                "precedence."))
        self.beginTime = util.unix_epoch()
        if opt['V']:
            opt['v'] = True
        # XXX set log level = DEBUG

        if cli_dict.get('mail_list', ""):
            # get rid of the quotes that are needed to pass through pp.starter
            mail_list = cli_dict.get('mail_list', "").replace('"', '')
            self.mailList = []
            for addr in mail_list.split(','):
                _, new_addr = email.utils.parseaddr(addr)
                if not new_addr:
                    _log.error((f"The email address '{addr}' specified in "
                        "--mail-list isn't valid"))
                    sys.exit(1)
                self.mailList.append(new_addr)
            _log.info((f"The email list '{self.mailList}' will be used for FRE "
                "notifications instead of the default '\$USER\@noaa.gov'"))
        else:
            self.mailList = f"{os.environ['USER']}@noaa.gov"
            _log.info(f"'{self.mailList}' will be used for FRE notifications")

        # $opt_x = 'rts.xml' unless $opt_x;    #set default filename for experiments xml file
        if not os.exists(opt['x']):
            logs.mailuser(f"XML file does not exist: {opt['x']}")
            logs.sysmailuser()
            _log.critical(f"XML file does not exist: {opt['x']}")
            sys.exit(1)

        if not opt['t'] and not opt['A']:
            _log.warning("You did not specify a model date")

        if opt['M'] and cli_dict.get('plus', False):
            _log.info(("You have specified -M (mail on complete) and --plus; to "
                "avoid flooding your inbox, the -M option has been turned off."))
            opt['M'] = ""

        if opt['O']:
            _log.info(("There has been a change to the behavior of the -O option. "
                f"Your scripts will be written directly to '{opt['O']}' and not "
                f"to '{opt['O']}/scripts/postProcess'"))

        if not opt['c']:
            _log.info(("adding '-c split'; frepp will do each component in a "
                "separate batch job"))
            opt['c'] = 'split'

        self.abs_xml_path = os.path.abspath(opt['x'])
        self.createdate = datetime.datetime.now()
        self.version_head = _template(f"""
            # FMS postprocessing script created at $createdate via:
            # $relfrepp -x $abs_xml_path
        """, self, createdate=self.createdate)

        # variables for timing statistics
        if opt['Q']:
            opt['z'] = False
        else:
            opt['z'] = True

        self.time = {k: "" for k in ("ncatted", "ncks", "ncmerge", "ncrcat",
            "nccatm", "plevel", "splitncvars", "timavg", "uncpio", "untar",
            "mkcpio", "mktar", "taxis2mid", "mv", "rm", "dmget", "ncap", "zgrid",
            "dmput", "cp", "fregrid", "ncrename", "hsmget", "hsmput", "combine",
            "nccopy")
        }

        # When frepp is run with -A option, the -t option is not required.
        # Then set the date to a valid date.  This isn't really ideal, but
        # this is how frepp did it in the past.
        if not opt['t'] and opt['A']:
            opt['t'] = '00010101'

        #clean up opt_t
        #
        # opt_t should be of the form \d{4,}\d{2}\d{2}.  It is possible a user
        # will pass in only a year (\d{4}).  As we move forward to allow for
        # years past 9999, we need to set some guidance on how opt_t is
        # interpreted.  Thus, we somewhat arbitrarily decide that if
        # length($opt_t) < 7, we assume a year has been passed in, 8 and
        # beyond, assume the from above.
        #
        # There should also be a method to correctly know how many digits are
        # in a year, when the year is needed later on in the script.
        #
        # $t0 holds $opt_t in the Date::Manip date format 'yyyymmddhh:mm:ss'
        try:
            self.t0 = FREUtil.parseDate(opt['t'])
            # Hold required information in the correct format in these three variables
            # should not use $opt_t anywhere as it may not have the correct format.
            self.userstartyear = self.t0.year
            self.userstartmo = self.t0.month
            self.hDate = self.t0
            self.tEnd = self.t0.increment(self.t0, util.DatePrecision.YEAR)
            _log.debug(f"t0 is {self.t0} (from -t argument)")
            _log.debug(f"tEND is {self.tEND} (from t0 + 1 year)")
        except Exception:
            _log.error((f"The date passed in via the '-t' option ('{opt['t']}') "
                "is not a valid date."))
            sys.exit(1)

        #These things vary by platform
        self.platform_opt = {
            'cp': 'cp',
            'mv': 'mv',
            'mvfile': 'mv',
            'cpio': 'cpio',
            'uncpio': 'cpio',
            'timecmd': '/usr/bin/time',
            'timereal': 'e',
            'systimecmd': 'date +\%s',
            'maxruntime': '60:00:00',
            'interpreter': '/usr/bin/env python3' # ADDED in translation
        }
        if cli_dict.get('Walltime', False):
            self.platform_opt['maxruntime'] = cli_dict['Walltime']
        # frepp.pl l.402

        ## formerly global; set in setup_fre
        self.root = None
        self.project = None
        self.platform = None

        self.opt = opt

    def template_dict(self):
        """Dict of all configuration key:values for templating .csh fragments."""
        d = util.ConsistentDict()
        d.update(dc.asdict(self))
        for k, v in self.opt.items():
            d['opt_'+k] = v
        for k, v in self.time.items():
            d['time_'+k] = v
        d.update(self.platform_opt)
        return d


def setup_fre(pp):
    # frepp.pl l.406
    try:
        fre = FRE.FRE.new(**(pp.opt))
    except Exception:
        sys.exit(1)
    pp.root = fre.rootNode
    pp.project = fre.project

    # use expanded platform (e.g. gfdl.ncrc-intel rather than ncrc-intel)
    pp.opt['P'] = fre.platform

    # set default platform to "unknown", to catch error with unsupported platforms
    platform = 'unknown'
    if pp.opt['P'].startswith('gfdl'):
        platform = 'x86_64'
    pp.platform = platform.strip()
    # frepp.pl l.418
    return (fre, pp)


def setup_expt(expt, fre, pp):
    """Configure FREExperiment object. First block of code in body of frepp loop
    over expts."""
    # frepp.pl l.422
    _log.debug(f"Setting up experiment '{expt}'...")
    if not FREUtil.checkExptExists(expt):
        return (pp, None) # next;

    try:
        exp = FREExperiment.FREExperiment.new(fre, expt)
        exp.expt = expt
    except Exception:
        sys.exit(1)
    fre.setCurrentExperimentName(expt)

    # Get the refineDiag scripts from the XML if -D given
    # on the command line with no string
    if pp.opt.get('refineDiag', False) and not pp.opt['D']:
        pp.opt['D'] = ','.join(exp.extractPPRefineDiagScripts())
    if (not os.path.isdir(exp.rootDir) or not util.is_writable(exp.rootDir)) \
        and not pp.opt['O']:
        os.makedirs(exp.rootDir)
    if (not os.path.isdir(exp.rootDir) or not os.access(exp.rootDir, os.W_OK)) \
        and not pp.opt['O']:
        logs.mailuser(f"Can't write to your root directory {exp.rootDir}")
        logs.sysmailuser()
        _log.error(f"Can't write to your root directory {exp.rootDir}")
        sys.exit(1)

    exp.archivedir = exp.archiveDir
    exp.postprocessdir = exp.postProcessDir
    exp.analysisdir = exp.analysisDir
    exp.scriptsdir = exp.scriptsDir
    exp.stdoutdir = exp.stdoutDir
    exp.tempCache = exp.workDir.split('/')[0] + "/tempCache"
    exp.statedir = os.path.join(exp.stateDir, "postProcess")
    exp.outscriptdir = os.path.join(exp.scriptsDir, "postProcess")
    if pp.opt['u']:
        exp.outscriptdir = os.path.join(exp.outscriptdir, pp.opt['u'])
    if pp.opt['O']:
        exp.outscriptdir = pp.opt['O']
    if not (os.path.isdir(exp.outscriptdir) or pp.opt['A']):
        os.makedirs(exp.outscriptdir)

    if not (os.path.isdir(os.path.join(exp.stdoutDir, "postProcess")) or pp.opt['A']):
        os.makedirs(os.path.join(exp.stdoutDir, "postProcess"))
    if pp.opt['u']:
        exp.statedir = os.path.join(exp.statedir, pp.opt['u'])
    if not (os.path.isdir(exp.statedir) or pp.opt['A']):
        os.makedirs(exp.statedir)
    exp.aoutscriptdir = os.path.join(exp.scriptsDir, "analysis")
    if pp.opt['O']:
        exp.aoutscriptdir = pp.opt['O']
    #     my $shortxml = $abs_xml_path;
    # $shortxml =~ s/.+\/(.+\.xml)(\.$expt\.o.+)?/$1/; # XXX

    if not exp.workDir:
        _log.critical("No workDir from xml")
        sys.exit(1)
    else:
        exp.tmphistdir = f"{exp.workDir}/{expt}_{pp.hDate}"
        exp.workdir = f"{exp.workDir}/{expt}_{pp.hDate}/work"

    if not pp.opt['d']: #set appropriate history directory
        if pp.opt['r']:
            pp.opt['d'] = f"{exp.archiveDir}/{pp.opt['r']}/history"
        else:
            pp.opt['d'] = f"{exp.archiveDir}/history"
    if pp.opt['d'] == '$archive/$name/history':
        pp.opt['d'] = f"{exp.archiveDir}/history"
    exp.histDir = pp.opt['d']
    if not os.path.isdir(exp.histDir):
        _log.debug(f"Creating history dir {exp.histDir}")
        os.makedirs(exp.histDir)
    _log.debug((f"\nDIRECTORIES: FRE4\nworkdir {exp.workdir}\nrootdir {exp.rootDir}\n"
        f"archivedir {exp.archiveDir}\nanalysisdir {exp.analysisDir}\noutscriptdir "
        f"{exp.outscriptdir}\naoutscriptdir {exp.aoutscriptdir}\nhistDir {exp.histDir}\n"
        f"ptmpDir {exp.ptmpDir}\nstatedir {exp.statedir}\n\n"))

    # Warn users if MDBIswtich is set and give directions on how to manually
    # fredb experiment
    if exp.MDBIswitch:
        fredb = shutil.which('fredb')
        fredb_cmd = ["-x", pp.abs_xml_path, "-t", pp.opt['T'], "-p", pp.opt['P'], expt]
        _log.warning(("Frepp no longer automatically ingests experiments into "
            f"Curator. You can do this manually by running: {fredb} {' '.join(fredb_cmd)}"))
    exp.refinedir = pp.opt['d'] + "_refineDiag"
    exp.ptmpDir = os.path.join(exp.ptmpDir, exp.archiveDir)
    if bool(pp.opt['u']):
        exp.ptmpDir = os.path.join(exp.ptmpDir, pp.opt['u'])

    # set whether to aggregate time series files in archive
    agg = FREUtil.getxpathval('postProcess/@archiveTimeSeries')
    if agg == 'byVariable':
        exp.aggregateTS = False

    #set platform specific variables
    # This really should be done using the fre.properties file
    _log.debug(f"Using platform {pp.platform}")
    if pp.platform == 'x86_64':
        pp.platform_opt['batchSubmit'] = 'sbatch'
        pp.platform_opt['cp'] = 'gcp -v'
        pp.platform_opt['cpio'] = 'cpio -C 524288'
        pp.platform_opt['uncpio'] = 'cpio -C 2097152'
        pp.platform_opt['mvfile'] = 'gcp -v'
        pp.platform_opt['platformcsh'] = \
            exp.fre.default_platform_csh + exp.fre.platformValue('csh')
    else:
        logs.mailuser(f"platform {pp.platform} not supported for postprocessing")
        _log.error(f"Platform '{pp.platform}' is unsupported.")

    if pp.opt['w']:
       if pp.platform == 'x86_64':
           pp.opt['w'] = f" --dependency=afterok:{pp.opt['w']}"

    #variables for timing statistics
    if pp.opt['z']:
        for cmd in pp.time:
            pp.time[cmd] = (f'{pp.platform_opt["timecmd"]} -f "     TIME for {cmd}:'
                f'    real %{pp.platform_opt["timereal"]} user %U sys %S"')

    version_info = pp.version_head
    if pp.opt['f']:
        version_info += '-f '
    if pp.opt['P']:
        version_info += f"--platform {pp.opt['P']} "
    if pp.opt['T']:
        version_info += f"-T {pp.opt['T']} "
    if pp.opt['D']:
        version_info += f"-D {pp.opt['D']} "
    if pp.opt['t']:
        version_info += f"-t {pp.hDate} " # Using corrected $opt_t value
    if pp.opt['plus']:
        version_info += f"--plus {pp.opt['plus']} "
    if pp.opt['c']:
        version_info += f"-c {pp.opt['c']} "
    if pp.opt['d']:
        version_info += f"-d {pp.opt['d']} "
    if pp.opt['u']:
        version_info += f"-u {pp.opt['u']} "
    if pp.opt['r']:
        version_info += f"-r {pp.opt['r']} "
    if pp.opt['H']:
        version_info += "-H "
    if pp.opt['m']:
        version_info += f"-m '{pp.opt['m']}' "
    if pp.opt['mail_list']:
        version_info += f"--mail-list pp.opt['mail_list'] "
    version_info += expt
    exp.version_info = version_info
    exp.this_frepp_cmd = version_info.replace('\n', '')
    # XXX $this_frepp_cmd =~ s/.*# //smg;

    # Get post fix string for job and script name
    name_postFix = ""
    if pp.opt['u']:
        name_postFix += pp.opt['u']

    if pp.opt['r']:
        name_postFix += f"_{pp.opt['r']}"
        pp.platform_opt['maxruntime'] = "01:00:00"
    elif pp.opt['D']:
        name_postFix += "_refineDiag"
    name_postFix += f"_{pp.hDate}"

    # Set the job and script name using the postfix.  $outscript is the same
    # as the job name, but with the script directory prepended
    exp.batch_job_name = expt + name_postFix
    exp.outscript = os.path.join(exp.outscriptdir, exp.batch_job_name)

    # frepp.pl l.714
    return (pp, exp)


def expt_loop_pre_component(fre, pp, exp):
    """Construct csh runscript from template. Second block of code in body of
    frepp loop over expts."""
    # frepp.pl l.716
    exp.cshscripttmpl = sub.getTemplate(pp.platform)

    # environment setup for FRE
    freCommandsHomeDir = fre.home()
    siteconfig = _template(f"""
        setenv FRE_COMMANDS_HOME_FREPP $freCommandsHomeDir
    """, freCommandsHomeDir=freCommandsHomeDir)
    exp.cshscripttmpl.replace('#get_site_config', siteconfig)

    # platform_csh and check for FRE version mismatch
    if pp.platform_opt['platformcsh']:
        fremodule = util.shell(f"echo {os.environ['LOADEDMODULES']} | tr ':' '\n' | egrep '^fre/.+'", log=_log)
        xmlfremodule = pp.platform_opt['platformcsh']
        fremodulecsh = []

        # Loop through the CSH and remove all comments
        for line_ in xmlfremodule.split('\n'):
            noncomment = shlex.join(shlex.split(line_, comments=True))
            if noncomment:
                fremodulecsh.append(noncomment)
        xmlfremodule = '\n'.join(fremodulecsh)
        xmlfremodule = re.sub(r'.*module load (fre\/\S+)\s*.*', r'\1', xmlfremodule)
        if fremodule != xmlfremodule:
            _log.error(("FRE version mismatch. Must use the same version of FRE "
                f"in the shell and XML.\nfrepp version: {fremodule}\n"
                f"xml version: {xmlfremodule}"))

        platformcsh = "#platform_csh\n" + pp.platform_opt['platformcsh'] + _template(f"""

            if ("$xmlfremodule" == "$fremodule" && "$fremodule" == "fre/test") then
                if ( "\$FRE_COMMANDS_HOME" != "\$FRE_COMMANDS_HOME_FREPP" ) then
                echo "WARNING: FRE/test version mismatch:"
                echo "       Frepp version: \$FRE_COMMANDS_HOME_FREPP"
                echo "       XML loads version: \$FRE_COMMANDS_HOME"
                endif
            else
                if ( "\$FRE_COMMANDS_HOME" != "\$FRE_COMMANDS_HOME_FREPP" ) then
                echo "ERROR: FRE version mismatch:"
                echo "       Frepp version: \$FRE_COMMANDS_HOME_FREPP"
                echo "       XML loads version: \$FRE_COMMANDS_HOME"
                exit 1
                endif
            endif
        """, fremodule=fremodule, xmlfremodule=xmlfremodule)

    exp.cshscripttmpl = exp.cshscripttmpl.replace('#platform_csh', platformcsh)
    _subs = []
    if pp.platform == 'x86_64':
        _subs += [
            (r'(#SBATCH --job-name)', rf'\1={exp.batch_job_name}'),
            (r'(#SBATCH --time)', rf'\1={pp.platform_opt["maxruntime"]}'),
            (r'(#SBATCH --output)', rf'\1={exp.stdoutDir}/postProcess/%x.o%j'),
            (r'(#SBATCH --chdir)', rf'\1={os.environ["HOME"]}'),
            (r'(setenv FRE_STDOUT_PATH)', rf'\1 {exp.stdoutDir}/postProcess/{exp.batch_job_name}.o$JOB_ID'),
        ]
    _subs += [
        (r'#version_info', rf'# {FREVersion.VERSION}\n{exp.version_info}'),
        (r'set name', rf'set name = {exp.expt}'),
        (r'set rtsxml', rf'set rtsxml = {pp.abs_xml_path}'),
        (r'set work', rf'set work = {exp.workdir}'),
        (r'set tempCache', rf'set tempCache = {exp.tempCache}'),
        (r'set root', rf'set root = {exp.rootDir}'),
        (r'set archive', rf'set archive = {exp.archiveDir}'),
        (r'set scriptName', rf'set scriptName = {exp.outscript}'),
        (r'set oname', rf'set oname = {pp.hDate}'),
        (r'set exp.histDir', rf'set exp.histDir = {pp.opt["d"]}'),
        (r'set ptmpDir', rf'set ptmpDir = {exp.ptmpDir}'),
        (r'set platform', rf'set platform = {pp.opt["P"]}'),
        (r'set target', rf'set target = {pp.opt["T"]}'),
        (r'set segment_months', rf'set segment_months = {ts_ta.segmentLengthInMonths()}'),
        (r'(#SBATCH --mail-user).*', rf'\1={pp.mailList}'),
        (r'(#SBATCH --comment).*', rf'\1=fre/{os.environ["FRE_COMMANDS_VERSION"]}')
    ]
    for _old, _new in _subs:
        exp.cshscripttmpl = re.sub(_old, _new, exp.cshscripttmpl)

    # if using XTMP filesystem for PTMP location, let scheduler know via Slurm --comment directive
    # so it can set $TMPDIR accordingly
    if exp.ptmpDir.startswith('/xtmp'):
        exp.cshscripttmpl = re.sub(r'(#SBATCH --comment)=?(.*)', r'\1=\2,xtmp', exp.cshscripttmpl)

    exp.statefile = None
    if pp.opt['r']:    #if a regression test, no further postprocessing
        exp.cshscripttmpl = re.sub(r'(#INFO:max_years=)', r'\1{exp.maxyrs}', exp.cshscripttmpl)
        sub.writescript(
            exp.cshscripttmpl, exp.outscript,
            f"{pp.platform_opt['batchSubmit']}{pp.opt['w']} {pp.opt['m']}",
            exp.statefile, pp
        )
        pp.opt['w'] = ''
        return (pp, exp) # next;

    exp.ppNode = FREUtil.getppNode(exp.expt)
    if not pp.opt['f'] and not exp.ppNode:  #if no pp node, no further postprocessing
        if pp.platform == 'x86_64':
            exp.cshscripttmpl = re.sub(r'(#SBATCH --time).*', r'\1=01:00:00', exp.cshscripttmpl)
        exp.cshscripttmpl = re.sub(r'(#INFO:max_years=)', r'\1{exp.maxyrs}', exp.cshscripttmpl)
        sub.writescript(
            exp.cshscripttmpl, exp.outscript,
            f"{pp.platform_opt['batchSubmit']}{pp.opt['w']} {pp.opt['m']}",
            exp.statefile, pp
        )
        pp.opt['w'] = ''
        return (pp, exp) # next;

    if pp.opt['M']:
        # XXX $exp.cshscripttmpl =~ s/(#SBATCH --mail-type=)NONE/$1END/;
        exp.cshscripttmpl = re.sub(r'((#SBATCH --mail-type=)', r'\1END', exp.cshscripttmpl)

    if pp.opt['C']:
        caltype = pp.opt['C']
    else:
        _log.debug('Getting namelists...')
        nml = exp.extractNamelists()
        if nml:
            caltype = nml.namelistSingleQuotedStringGet("coupler_nml", "calendar")
            if not caltype:
                caltype = 'julian'
                _log.warning(("Using default calendar type 'julian' as couldn't "
                    "find 'coupler_nml'/'calendar' namelist value."))
        else:
            caltype = 'julian'
            _log.warning(("Couldn't populate namelists; most likely your "
                "external namelists weren't transferred, or are improperly "
                "referenced in your XML!"))
            _log.warning(("Using default calendar type 'julian' as couldn't "
                    "find 'coupler_nml'/'calendar' namelist value."))
        _log.debug(f"caltype = {caltype}")

    exp.basedate = ""
    exp.diagtablecontent = exp.extractTable('diagTable').split('\n')
    if pp.opt['B']:
        exp.basedate = pp.opt['B']
    else:
        #look in coupler_nml if exp.basedate set to $baseDate
        exp.basedate = exp.diagtablecontent[1].strip()
        if exp.basedate == "\$baseDate":
            exp.basedate = nml.namelistGet("coupler_nml")
            if not exp.basedate:
                exp.basedate = "0 0 0 0 0 0"
            else:
                exp.basedate = re.sub(r'.*current_date\s*=\s*(\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+)\s*(.*)',
                    r'\1', exp.basedate)
                exp.basedate = exp.basedate.strip()
    _log.debug(f"exp.basedate = {exp.basedate}")

    gridspec = exp.extractVariableFile('gridSpec')
    if not gridspec:
        _log.critical(f"gridSpec for platform {pp.opt['P']} was not found in {pp.opt['x']}")
        sys.exit(1)
    elif not os.exists(gridspec):
        _log.critical(f"gridSpec file does not exist: {gridspec}")
        sys.exit(1)
    else:
        _log.debug(f"Using gridSpec {gridspec}")

    # if refineDiag run, insert refineDiag csh here
    if pp.opt['D']:
        exp.cshscripttmpl += sub.refineDiag(exp.tmphistdir, exp.stdoutDir,
            exp.ptmpDir, exp.basedate, exp.refinedir, gridspec, exp.MDBIswitch)

    if pp.opt['f'] or pp.opt['D']:
        #separate mppnccombine from rest of script due to splitting components
        if not pp.opt['H']:
            next_script = (f"{pp.platform_opt['interpreter']} {pp.absfrepp} -x "
                f"{pp.abs_xml_path} -t {pp.hDate} -s -v ")
            if pp.opt['P']: next_script += f"-P {pp.opt['P']}"
            if pp.opt['T']: next_script += f"-T {pp.opt['T']}"
            if pp.opt['d']: next_script += f"-d {pp.opt['d']}"
            if pp.opt['u']: next_script += f"-u {pp.opt['u']}"
            if pp.opt['c']: next_script += f"-c {pp.opt['c']}"
            if pp.opt['m']: next_script += f"-m '{pp.opt['m']}'"
            if pp.opt['M']: next_script += f"-M "
            if pp.opt['o']: next_script += f"-o "
            next_script = next_script + exp.expt + '\n'
            next_script += logs.errorstr((f"{pp.relfrepp} had a problem creating "
                f"next script {exp.expt}_{pp.hDate}"))
            exp.cshscripttmpl += next_script

        if pp.opt['f']:
            hsmf = []
            try:
                with os.scandir(pp.opt['d']) as files:
                    hf = [f.name for f in files if (not f.name.startswith('.') \
                        and f.is_file() \
                        and f.name.split('.')[-1] in ('raw', 'nc', 'tar')
                    )]
            except Exception():
                raise
            hsmget_history = sub.hsmget_history_csh(exp.ptmpDir, exp.tmphistdir,
                exp.refinedir, exp.this_frepp_cmd,
                ' '.join(hf), ' '.join(hsmf)
            )
            exp.cshscripttmpl = exp.cshscripttmpl.replace('#hsmget_history_files', hsmget_history)
            uncompress = sub.uncompress_history_csh(exp.tmphistdir)
            exp.cshscripttmpl = exp.cshscripttmpl.replace('#uncompress_history_files', uncompress)
            hf.sort()
            check_history = sub.checkHistComplete(exp.tmphistdir, hf[0],
                exp.this_frepp_cmd, hsmf, exp.diagtablecontent)
            exp.cshscripttmpl = exp.cshscripttmpl.replace('#check_history_files', check_history)

        if pp.opt['D'] and pp.opt['plus']:
            call_frepp = sub.call_frepp(pp.abs_xml_path, exp.outscript, pp.opt['c'], "", "", pp)
            call_frepp += logs.errorstr((f"{pp.relfrepp} had a problem creating "
                f"next script {exp.expt}_{pp.hDate}"))
            exp.cshscripttmpl += call_frepp

        exp.cshscripttmpl += logs.mailerrors(exp.outscript)
        if pp.platform == 'x86_64':
            exp.cshscripttmpl = re.sub(r'(#SBATCH --time).*', rf'\1={pp.platform_opt["maxruntime"]}', exp.cshscripttmpl)
        exp.cshscripttmpl = re.sub(r'(#INFO:max_years=)', rf'\1{exp.maxyrs}', exp.cshscripttmpl)
        sub.writescript(
            exp.cshscripttmpl, exp.outscript,
            f"{pp.platform_opt['batchSubmit']}{pp.opt['w']} {pp.opt['m']}",
            exp.statefile, pp
        )
        pp.opt['w'] = ''
        return (pp, exp) # next;
    else:
        exp.cshscripttmpl = re.sub(r'set histDir = .*', rf'set histDir = {exp.tmphistdir}', exp.cshscripttmpl)

    if not pp.opt['A']:
        writeIDorINTER = _template("""
            echo $JOB_ID > $exp.statefile
        """)
        exp.cshscripttmpl = exp.cshscripttmpl.replace('#write_to_statefile', writeIDorINTER)
        pp.writestate = _template("""
            if ( "\$prevjobstate" == "ERROR" ) then
                echo FATAL > $statefile
            else
                echo ERROR > $statefile
            endif
        """, statefile=exp.statefile)

    checktransfer = _template("""
        if ( \$status ) then
                    echo ERROR: data transfer attempt failed twice, exiting.
                    Mail -s "\$name year \$historyyear cannot be postprocessed" $mailList <<END
            Your FRE post-processing job ( \$JOB_ID ) has exited because of a data transfer failure.

            This job can be resubmitted via:

            $this_frepp_cmd

            Job details:
            \$name running on \$HOST
            Batch job stdout:
            \$FRE_STDOUT_PATH
            END

            $writestate

            sleep 30
            exit 7
        endif
    """, pp, exp)

    #assemble a command to create archive directories
    _log.debug('Creating archive directories...')
    exp.ppRootDir = exp.postprocessdir
    if pp.opt['u']:
        exp.ppRootDir += f"_{pp.opt['u']}"
    if pp.opt['A'] and not os.path.isdir(exp.ppRootDir):
        _log.critical(f"Directory {exp.ppRootDir} not found. Exiting.")
        sys.exit(1)
    exp.mkdircommand = ""
    if not pp.opt['A']:
        exp.mkdircommand += f"mkdir -p {exp.ppRootDir}/.dec {exp.ppRootDir}/.checkpoint "
    nocommentver = exp.version_info
    nocommentver = nocommentver.replace('#', "")
    archive_command = _template("""
        touch $statedir/frepp.log
        echo '#$freVersion' >> $statedir/frepp.log
        cat >> $statedir/frepp.log <<END
        $nocommentver

        END

        #hsmget_history_files

        #uncompress_history_files

        #check_history_files
    """, exp, freVersion=pp.freVersion, nocommentver=nocommentver)
    exp.cshscripttmpl += archive_command

    getgridspec = f"cd \$work; dmget {gridspec}\n"
    if gridspec.endswith('cpio'):
        getgridspec += _template("""
            if ( ! -e mosaic.nc && ! -e grid_spec.nc && ! -e atmos_mosaic.nc ) then
                $time_uncpio cpio -iv < $gridspec
            endif
        """, pp, gridspec=gridspec)
    elif gridspec.endswith('tar'):
        getgridspec += _template("""
            if ( ! -e mosaic.nc && ! -e grid_spec.nc && ! -e atmos_mosaic.nc ) then
                $time_untar tar -xvf $gridspec
            endif
        """, pp, gridspec=gridspec)
    else:
        getgridspec += _template("""
            if ( ! -e mosaic.nc && ! -e grid_spec.nc && ! -e atmos_mosaic.nc ) then
                $time_cp $cp $gridspec .
            endif
        """, pp, gridspec=gridspec)

    call_tile_fregrid = getgridspec + _template("""
        if ( -e mosaic.nc ) then
            set input_mosaic = `ncks -H -v $mosaic_type mosaic.nc | sed 's/.*="//;s/"//'`
        else if ( -e grid_spec.nc ) then
            set input_mosaic = `ncks -H -v $mosaic_type grid_spec.nc | sed 's/.*="//;s/"//'`
        else
            echo ERROR: Cannot locate atm_mosaic_file
            exit 1
        endif

        ls -l $input_mosaic

        # Check for a suitable regrid file in FMS-land
        set mosaic_gridfile = `ncks -H -v gridfiles $input_mosaic | head -n 1 | sed 's/.*="//;s/"//'`
        @ remap_source_x = `ncdump -h $mosaic_gridfile | grep "nx =" | sed 's/.*=//;s/;//;s/ //g'` / 2
        @ remap_source_y = `ncdump -h $mosaic_gridfile | grep "ny =" | sed 's/.*=//;s/;//;s/ //g'` / 2
        set fms_remap_file = $fregrid_remap_file:t
        set fms_remap_file = `echo $fms_remap_file | sed 's/^\.//'`
    """)

    fms_remap_dir = fre.property_('FRE.directory.fms_shared_fregrid_remap_files') # XXX
    call_tile_fregrid += _template("""
        set fms_remap_file = "$fms_remap_dir/\$source_grid/\${remap_source_x}_by_\$remap_source_y/\$fms_remap_file"
    """, fms_remap_dir=fms_remap_dir)
    call_tile_fregrid += _template("""
        if (-e $fms_remap_file) then
            echo "NOTE: Using shared FMS remap file $fms_remap_file"
            set fregrid_remap_file = $fms_remap_file
            set remap_dir = $fms_remap_file:h
            set remap_file = $fms_remap_file:t
        else
            set remap_dir = $fregrid_remap_file:h
            set remap_file = $fregrid_remap_file:t
        endif
    """)
    call_tile_fregrid += _template("""
        if ( -e \$fregrid_remap_file ) then
            $time_cp $cp \$fregrid_remap_file \$remap_file
            if ( \$status ) then
                echo "WARNING: data transfer failure, retrying..."
                $time_cp $cp \$fregrid_remap_file \$remap_file
                $checktransfer
            endif
        endif
    """, pp, checktransfer=checktransfer)
    call_tile_fregrid += _template("""
        if ( $?variables && "$variables" != '' ) then
            # auxillary static variables such as a,b,*_bnds must be passed through fregrid for CMIP use
            set static_vars = ( `$NCVARS -s0123 $fregrid_in.tile1.nc` )
            set static_vars = `echo $static_vars |sed 's/ /,/g'`
            set interpvars = `echo "$variables,$static_vars" | sed 's/,$//'`
            unset variables static_vars
        else
            set interpvars = ( `$NCVARS $ncvars_arg $fregrid_in.tile1.nc` )
            set interpvars = `echo $interpvars |sed 's/ /,/g'`
        endif
        set order1 = ( precip prec_ls snow_ls prec_conv snow_conv uw_precip prec_uwc snow_uwc prc_deep_donner snow_deep_donner prc1_deep_donner prc_mca_donner snow_mca_donner ice_mask land_mask zsurf cld_amt_2d conv_freq IWP_all_clouds WP_all_clouds WVP tot_cloud_area tot_ice_amt tot_liq_amt swdn_sfc_clr swdn_sfc swdn_toa_clr swdn_toa swdn_tot_dif swup_sfc_clr swup_sfc swup_toa_clr swup_toa wind_ref wind_ref_max wind_ref_min area )
        set non_regrid_vars = ( lat lon )
    """)
    non_regrid_vars = fre.property_('FRE.frepp.fregrid.non_regrid_vars') # XXX
    call_tile_fregrid += _template("""
        set non_regrid_vars = ( \$non_regrid_vars $non_regrid_vars )
        set attCmds = ()
        unset echo
        if ( -e $ppRootDir/.non_default_interp_method ) then
            source $ppRootDir/.non_default_interp_method
        endif
    """, exp, non_regrid_vars=non_regrid_vars)
    call_tile_fregrid += _template("""
        foreach order1var ( $order1 )
            foreach modelvar ( `echo $interpvars | sed -e "s/,/ /g"` )
                if ( $modelvar == $order1var ) then
                    if ( `ncexists -f $fregrid_in.tile1.nc -v $order1var` ) then
                        if ( ! `ncexists -f $fregrid_in.tile1.nc -v $order1var -a interp_method` ) then
                            set attCmds = ( $attCmds -a interp_method,$order1var,a,c,"conserve_order1" )
                        endif
                    endif
                endif
            end
        end
        foreach non_regrid_var ( $non_regrid_vars )
            foreach modelvar ( `echo $interpvars | sed -e "s/,/ /g"` )
                if ( $modelvar == $non_regrid_var ) then
                    if ( `ncexists -f $fregrid_in.tile1.nc -v $non_regrid_var` ) then
                        if ( ! `ncexists -f $fregrid_in.tile1.nc -v $non_regrid_var -a interp_method` ) then
                            set attCmds = ( $attCmds -a interp_method,$non_regrid_var,a,c,"none" )
                        endif
                    endif
                endif
            end
        end
        set echo
    """)
    call_fregrid = call_tile_fregrid.replace('tile1.', '')
    call_fregrid += _template("""
        if (\$#attCmds > 0) then
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.nc \$fregrid_in.nc.tmp
            #check_ncatted
            mv -f \$fregrid_in.nc.tmp \$fregrid_in.nc
        endif

        if ( "\$fregrid_wt" != '' ) then
            set fregrid_yr = `echo \$fregrid_in_date | sed 's/[0-9][0-9][0-9][0-9]\$//'`
            foreach fregridwtfile ( `ls $opt_d/\${fregrid_yr}????.nc.*` )
                set fregridwtfile = \$fregridwtfile:t:r
                $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$fregridwtfile/\\*land_static\\*
                if ( \$status ) then
                    echo "WARNING: hsmget reported failure, retrying..."
                    $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$fregridwtfile/\\*land_static\\*
                    $checktransfer
                endif
            end
            ln -s \$histDir/\${fregrid_yr}????.nc/*land_static* .
            foreach fregrid_mo ( 02 03 04 05 06 07 08 09 10 11 12 )
                if ( ! -e \${fregrid_yr}\${fregrid_mo}01.land_static.nc ) then
                    ln -s \${fregrid_yr}0101.land_static.nc \${fregrid_yr}\${fregrid_mo}01.land_static.nc
                endif
            end
        endif

        # Get the associated_files
        foreach assocFileBase ( `ncdump -h \$fregrid_in.nc | $grepAssocFiles` )
            set assocFileYear = `echo \$assocFileBase | cut -c 1-4`
            foreach aff ( `ls -1 \$histDir/\$assocFileYear*/*\${assocFileBase}*` )
                if ( ! -e `basename \$aff` ) ln -s \$aff .
            end
        end
    """, pp, exp, checktransfer=checktransfer, grepAssocFiles=sub.grepAssocFiles)
    call_tile_fregrid += _template("""
        if (\$#attCmds > 0) then
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile1.nc
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile2.nc
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile3.nc
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile4.nc
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile5.nc
            $time_ncatted ncatted -h -O \$attCmds \$fregrid_in.tile6.nc
            #check_ncatted
        endif

        if ( "\$fregrid_wt" != '' ) then
            set fregrid_yr = `echo \$fregrid_in_date | sed 's/[0-9][0-9][0-9][0-9]\$//'`
            foreach fregridwtfile ( `ls $opt_d/\${fregrid_yr}????.nc.*` )
                set fregridwtfile = \$fregridwtfile:t:r
                $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$fregridwtfile/\\*land_static\\*
                if ( \$status ) then
                    echo "WARNING: hsmget reported failure, retrying..."
                    $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$fregridwtfile/\\*land_static\\*
                    $checktransfer
                endif
            end
            ln -s \$histDir/\${fregrid_yr}????.nc/*land_static* .
            foreach i ( 1 2 3 4 5 6 )
                foreach fregrid_mo ( 02 03 04 05 06 07 08 09 10 11 12 )
                    if ( ! -e \${fregrid_yr}\${fregrid_mo}01.land_static.tile\$i.nc ) then
                    ln -s \${fregrid_yr}0101.land_static.tile\$i.nc \${fregrid_yr}\${fregrid_mo}01.land_static.tile\$i.nc
                    endif
                end
            end
        endif

        # Correct associated_file year (for static files) and get files
        foreach f ( \$fregrid_in*.nc )
            set fregrid_yr = `echo \$fregrid_in_date | cut -c 1-4`
            set oldassoc = `ncdump -h \$f | grep ':associated_files' | cut -d'"' -f2`
            if ("\$oldassoc" != '' ) then
                set newassoc = `ncdump -h \$f | grep ':associated_files' | cut -d'"' -f2 | sed "s/: [0-9]\\{4\\}/: \$fregrid_yr/g"`
                if ("\$oldassoc" != "\$newassoc") then
                    # If file is a link, then copy and make sure it is writable
                    if ( -l \$f ) then
                        $time_cp cp \$f copy
                        $time_rm rm -f \$f
                        $time_mv $mv copy \$f
                        chmod 644 \$f
                    endif
                    $time_ncatted ncatted -h -O -a associated_files,global,m,c,"\$newassoc" \$f
                    #check_ncatted
                endif

                # For now, remove the '.tile[1-6]' from all associated files.  fregrid will add it back in
                #TODO - When fregrid is updated, remove this
                set newassoc = `ncdump -h \$f | grep ':associated_files' | cut -d'"' -f2 | sed "s/\.tile[0-6]//g"`
                if ( -l \$f ) then
                    $time_cp cp \$f copy
                    $time_rm rm -f \$f
                    $time_mv $mv copy \$f
                    chmod 644 \$f
                endif
                $time_ncatted ncatted -h -O -a associated_files,global,m,c,"\$newassoc" \$f
                #check_ncatted

                # Get the associated_files
                foreach af ( `echo \$newassoc | sed "s/\\w*://g"` )
                    foreach aff ( \$histDir/\$fregrid_yr*/*\${af:r}* )
                        if ( ! -e `basename \$aff` ) ln -s \$aff .
                    end
                end
            endif
        end
    """, pp, exp, checktransfer=checktransfer)
    if pp.basenpes == 1:
        exec_fregrid = _template("""
            $time_fregrid \$FREGRID --standard_dimension --input_mosaic \$input_mosaic --input_file \$fregrid_in --interp_method \$interp_method --remap_file \$remap_file --nlon \$nlon --nlat \$nlat --scalar_field \$interpvars \$fregrid_wt --output_file out.nc \$interp_options
        """, pp)
        call_tile_fregrid += exec_fregrid
        call_fregrid += exec_fregrid
    else:
        exec_fregrid = _template("""
            $time_fregrid mpirun -np $basenpes fregrid_parallel --standard_dimension --input_mosaic \$input_mosaic --input_file \$fregrid_in --interp_method \$interp_method --remap_file \$remap_file --nlon \$nlon --nlat \$nlat --scalar_field \$interpvars \$fregrid_wt --output_file out.nc \$interp_options
        """, pp)
        call_tile_fregrid += exec_fregrid
        call_fregrid += exec_fregrid

    rename_regridded = _template("""
        #check_fregrid

        if ( ! -e \$fregrid_remap_file && -e out.nc ) then
            ls -l \$remap_file*
            $time_hsmput hsmput -v -t -p \$remap_dir -w . \$remap_file
            if ( \$status ) then
                echo "WARNING: data transfer failure, retrying..."
                $time_hsmput hsmput -v -t -p \$remap_dir -w . \$remap_file
                $checktransfer
            endif
        endif

        if ( -e out.nc ) then
            mv out.nc \$fregrid_in.nc
        else
            rm \$fregrid_in.nc
        endif
    """, pp, checktransfer=checktransfer)
    call_tile_fregrid += rename_regridded
    call_fregrid += rename_regridded

    call_tile_fregrid += _template("""
        $time_rm rm \$fregrid_in.tile?.nc
        $time_rm rm *.grid_spec.tile*.nc
    """, pp)

    if pp.opt['A']:
        _log.info("\nANALYSIS ONLY mode, pp/ files will not be generated")

    ppComponentNodes = exp.ppNode.findnodes("component")
    if not ppComponentNodes:
        if pp.opt['c'] and pp.opt['c'] != 'split':
            _log.error(f"No such component: {pp.opt['c']}")
        else:
            _log.error("No available components")

    # frepp.pl l.1317
    #process each component
    return (pp, exp)



def expt_loop_post_component(pp, exp):
    # frepp.pl l.2438
    if not pp.opt['c']:
        _log.critical("Calling frepp without -c is no longer supported.")
        sys.exit(1)

    logs.sysmailuser()

    if exp.frepp_plus_calls:
        _N = len(exp.frepp_plus_calls)
        _log.info((f"Normal frepp processing done; about to run {_N} "
            f"frepp commands for next year, due to --plus {pp.opt['plus']}"))
        for i, cmd in enumerate(exp.frepp_plus_calls):
            _log.info(f"\n# {i}/{_N}: {cmd}\n")
            util.shell(cmd, log=_log) # XXX
    # frepp.pl l.2456
    return (pp, exp)

# //////////////////////////////////////////////////////////////////////////////#
# //////////////////////////////////////////////////////////////////////////////#
# //////////////////////////////////////////////////////////////////////////////#
# //////////////////////////////////////////////////////////////////////////////#
# //////////////////////////////////////////////////////////////////////////////#


# for ppcNode in ppComponentNodes:

def component_loop_setup(ppcNode, fre, pp, exp):
    """Start of body of loop over each component."""
    # frepp.pl l.1319
    exp.frepp_plus_calls = []
    component = ppcNode.findvalue('@type')
    _log.debug(f"Creating script for postprocessing component '{component}'")

    cpt = FREppComponent(component=component) ### XXX added
    cpt.cpiomonTS = ''
    cpt.cshscript = exp.cshscripttmpl

    this_component_cmd = exp.this_frepp_cmd.replace(' -c split ', f' -c {component} ')
    checktransfer = _template("""
        if ( \$status ) then
            echo ERROR: data transfer attempt failed twice, exiting.
            Mail -s "\$name year \$historyyear component $component cannot be postprocessed" $mailList <<END
            Your FRE post-processing job ( \$JOB_ID ) has exited because of a data transfer failure.

            This job can be resubmitted via:

            $this_component_cmd

            Job details:
                \$name running on \$HOST
            Batch job stdout:
                \$FRE_STDOUT_PATH
            END

            $writestate
            sleep 30
            exit 7
        endif
    """, pp, component=component, this_component_cmd=this_component_cmd)

    if pp.opt['c']: #append component name to job and file name
        cshscript = exp.cshscripttmpl
        origoutscript = exp.outscript
        exp.outscript = f"{exp.outscriptdir}/{exp.expt}_{component}_{pp.hDate}"
        batch_job_name = exp.expt
        if pp.opt['u']:
            batch_job_name += f"_{pp.opt['u']}"
        batch_job_name += f"_{component}_{pp.hDate}"

        _subs = [
            (r'set scriptName = .*', rf'set scriptName = {exp.outscript}'),
            (r'(setenv FRE_STDOUT_PATH).*', rf'\1 {exp.stdoutdir}/postProcess/{batch_job_name}.o$JOB_ID'),
            (r'(#SBATCH --job-name).*', rf'\1={batch_job_name}'),
            (r'(-w $expt)', rf'\1_{component}'),
            (r'(#INFO:component=)', rf'\1{component}')
        ]
        # special case: ocean_(annual|month) jobs can run into memory issues, so require bigmem node
        if ('ocean_annual' in component) or ('ocean_monthly' in component):
            _log.debug((f"Requesting large-memory node for component='{component}' "
                "due to the possibly large ocean files."))
            _subs += [(r'(#SBATCH --job-name.*)', r'\1\n#SBATCH --constraint=bigmem')]
        for _old, _new in _subs:
            cshscript = re.sub(_old, _new, cshscript)
        exp.statefile = f"{exp.statedir}/{component}.{pp.userstartyear}"

        #check status of this frepp year
        if not pp.opt['A']:
            if os.exists(exp.statefile):
                with open(exp.statefile, 'r') as f:
                    state = FREUtil.cleanstr(f.read())
                _log.info((f"This year ({pp.hDate}) has a state file with state "
                    f"'{state}' for {component}."))
                if state == 'OK':
                    if pp.opt['o']:
                        _log.info(("Redoing anyway because 'overwrite state "
                            "files' was specified..."))
                    else:
                        _log.info((f"This year ({pp.hDate}) has already been "
                            f"completed for {component}."))
                        # if --plus option is used, store next year's frepp command to call at the end
                        if pp.opt['plus']:
                            exp.frepp_plus_calls.append(
                                sub.form_frepp_call_for_plus_option(component)
                            )
                        return (pp, exp) # XXX next;
                elif state == 'FATAL':
                    _log.error((f"This year ({pp.hDate}) got an error in multiple "
                        f"attempts, skipping this component.  To retry {component} "
                        f"processing, delete the state file {exp.statefile}"))
                    return (pp, exp) # XXX next;
                elif state == 'INTERACTIVE':
                    _log.info((f"This year ({pp.hDate}) was partially run interactively "
                        f"but not completed, resubmitting {component}..."))
                elif state == "ERROR":
                    _log.info((f"This year ({pp.hDate}) got an error in the last "
                        f"frepp attempt, resubmitting {component}..."))
                elif state == "HISTORYDATAERROR":
                    _log.info((f"This year ({pp.hDate}) got an error in the last "
                        f"frepp attempt due to missing history data, resubmitting "
                        f"{component}..."))
                elif not state:
                    _log.error(f"exp.statefile {exp.statefile} exists but is empty, exiting.")
                    #what if this is the -c split job? continue to other components
                    return (pp, exp) # XXX next;
                else:
                    #check that jobid is still running
                    jobrunning = logs.isjobrunning(state)
                    _log.info((f"Checking state in {exp.statefile}: {state}: jobrunning: "
                        f"{jobrunning}"))
                    if jobrunning:
                        _log.info((f"Previous frepp job ({state}) for {pp.hDate} "
                            f"still running for {component}, exiting."))
                        return (pp, exp) # XXX next;
                    else:
                        _log.info((f"Previous frepp job for {pp.hDate} was lost, "
                            f"resubmitting {component}..."))

            cshscript = re.sub(r'set prevjobstate.*', rf"set prevjobstate = '{state}'", cshscript)
            cshscript = re.sub(r'set exp.statefile.*', rf"set exp.statefile = '{exp.statefile}'", cshscript)
    ## end if ($opt_c)

    #initialize per component
    cubic        = 0
    sourceGrid   = ''
    xyInterp     = ''
    interpMethod = ''
    xyInterpOptions = ''

    #xyInterp
    sourceGridAtt = ppcNode.findvalue('@sourceGrid')

    #backwards reproducibility
    c2l = ppcNode.findvalue('@cubicToLatLon')

    if c2l == 'none':    #leave data on cubed sphere grid
        sourceGrid  = 'cubedsphere'
        xyInterp    = ''
        mosaic_type = "atm_mosaic_file"
    elif c2l:    #convert to lat lon grid
        sourceGrid  = 'cubedsphere'
        xyInterp    = c2l
        mosaic_type = "atm_mosaic_file"
    else:                     #latlon grid
        if not sourceGridAtt:
            sourceGrid  = 'latlon'
            xyInterp    = ''
            mosaic_type = "atm_mosaic_file"

    #check if need to convert cube sphere grid to lat lon
    gridstrings = sourceGridAtt.split('-')
    if gridstrings[0] == "atmos":
        mosaic_type = "atm_mosaic_file"
    elif gridstrings[0] == "ocean":
        mosaic_type = "ocn_mosaic_file"
    elif gridstrings[0] == "land":
        mosaic_type = "lnd_mosaic_file"
    elif gridstrings[0] == "none":
        mosaic_type = "none"
    else:
        if not mosaic_type:
            _log.critical((f"sourceGrid mosaic type '{gridstrings[0]}' not supported. "
                "sourceGrid string must be 'none' or of the form: {atmos,ocean,"
                "land}-{latlon,cubedsphere,tripolar}"))
            sys.exit(1)
    cshscript += f"set mosaic_type = {mosaic_type}\n"  #this will be wrong for ocean, but not used.
    if sourceGridAtt == 'none':
        if not sourceGrid:
            sourceGrid = 'none'
    else:
        if not sourceGrid:
            sourceGrid = gridstrings[1]
    if not xyInterp:
        xyInterp = ppcNode.findvalue('@xyInterp')
    if not sourceGrid:
        _log.critical(("Must specify postprocessing component 'sourceGrid' attribute "
            "of the form: '{atmos,ocean,land}-{latlon,cubedsphere,tripolar}' or 'none'"))
        sys.exit(1)
    elif sourceGrid not in ('cubedsphere','latlon','tripolar','none'):
        _log.critical(("sourceGrid must be 'none' "
            "or '{atmos,ocean,land}-{latlon,cubedsphere,tripolar}'"))
        sys.exit(1)

    nlat = 0
    nlon = 0
    if not xyInterp:
        _log.debug("Data will be left on $sourceGrid grid")
        cshscript += f"#Grid type: {sourceGrid}\n"
    elif re.match(r'\d+,\d+', xyInterp):
        nlat, nlon = xyInterp.split(',')
        _log.debug(f"{sourceGrid} data will be converted to (nlat,nlon)=({nlat},{nlon})")
        cshscript += f"#Grid type: {sourceGrid} will be converted to latlon (nlat,nlon)=({nlat},{nlon})\n"

        # Check for a xyInterpRegridFile defined in the XML
        xyInterpRegridFiles = fre.dataFiles(ppcNode, 'xyInterpRegridFile')

        # dataFiles returns a file/target type array, we only care about the file
        if xyInterpRegridFiles:
            xyInterpRegridFile = xyInterpRegridFiles[0]
            if (not os.exists(xyInterpRegridFile) \
                and not util.is_writable(os.path.dirname(xyInterpRegridFile))):
                _log.critical((f"xyInterpRegridFile '{xyInterpRegridFile}' is specified "
                    f"for '{component}', but doesn't exist and user will be unable "
                    "to write to it's final location"))
                sys.exit(1)
        else:
            xyInterpRegridFile = f"{exp.ppRootDir}/{component}" + "/.fregrid_remap_file_${nlon}_by_${nlat}.nc"
    else:
        _log.critical("xyInterp must specify a 'lat,lon' for regridding")
        sys.exit(1)

    if sourceGrid == "cubedsphere":
        interpMethod = 'conserve_order2'
    else:
        interpMethod = 'conserve_order1'

    #check if non-default interpMethod specified
    componentIM = ppcNode.findvalue('@interpMethod')
    if componentIM:
        interpMethod = componentIM

    # make sure cmip and zInterp options aren't used together
    if ppcNode.findvalue('@cmip').lower() in ('yes', 'on', 'true') \
        and ppcNode.findvalue('@zInterp'):
        _log.critical(f"Component {component} requested incompatible cmip and zInterp options.")
        sys.exit(1)

    # if custom xyInterpOptions are requested, pass them to fregrid
    xyInterpOptions = ppcNode.findvalue('@xyInterpOptions')
    if xyInterpOptions:
        _log.info(f"Custom xyInterp options: '{xyInterpOptions}'")

    #get list of all diagnostic output files from source attributes, remove duplicates
    sourceatts = ppcNode.findnodes('*/@source')
    sourceatts += util.to_iter(ppcNode.findnodes('@source'))
    dts = [n.findvalue('.') for n in sourceatts]
    dts.append(f"{component}_month")
    dtsources = []
    seen = set()
    for dt in dts:
        if not (('monthly' in dt) or ('annual' in dt) or ('seasonal' in dt) or dt in seen):
            dtsources.append(dt)
        seen.add(dt)
    if 'land_static' not in seen:
        dtsources.append('land_static')
    _log.debug(f"diag_table source files are '{dtsources}'")

    #get list of variables for each file from diag table information.
    for srcfile in dtsources:
        dtv = []
        dtvall = []

        for dt in exp.diagtablecontent:
            if not dt.startswith('#') and re.match(rf'.*,.*,\s*"(\w*)"\s*,\s*"{srcfile}"\s*,.*,.*,.*,.*', dt):
                dtvall.append(dt)
            #omit static/instantaneous variables
            if not dt.startswith('#') and re.match(rf'.*,.*,\s*"(\w*)"\s*,\s*"{srcfile}"\s*,.*,\s*\.true\.\s*,.*,.*', dt):
                dtv.append(dt)
        varstr = ','.join(dtv)
        varstrall = ','.join(dtvall)
        srcfileall = f"all_${srcfile}"
        if varstrall:
            cpt.dtvars[srcfile] = varstr
            cpt.dtvars[srcfileall] = varstrall
            if pp.opt['V']:
                _log.debug(f"{srcfile} vars, static/instant omitted: {cpt.dtvars[srcfile]}\n")
            else:
                _log.debug(f"All {srcfile} diag_table variables: {cpt.dtvars[srcfileall]}\n")
    ## end foreach my $srcfile (@dtsources)

    startdate = ppcNode.findvalue('@start')
    if not startdate:
        startdate = exp.ppNode.findvalue('@start')
    cpt.sim0 = ""
    run0 = FREUtil.parseFortranDate(exp.basedate)
    if startdate:
        startdate = FREUtil.padzeros(startdate)
        cpt.sim0 = FREUtil.parseDate(startdate)
        if not cpt.sim0:
            _log.critical(("The start date specified in the XML postProcess/component "
                "tag is not a valid date."))
            sys.exit(1)
        if cpt.sim0 and FREUtil.dateCmp(run0, cpt.sim0) == 1:
            logs.mailuser((f"the {component} postprocessing start attribute ({cpt.sim0}) "
                f"must be equal to or later than the start of the run ({run0}). The "
                "default value of the start attribute is the start of the run.  "
                "Setting the start attribute to a later date provides the ability "
                "to skip years of data at the beginning of a run. Please check the "
                "start attribute of all postprocessing components.\n"))
            _log.warning((f"the {component} postprocessing start attribute ({cpt.sim0}) "
                f"must be equal to or later than the start of the run ({run0}). The "
                "default value of the start attribute is the start of the run.  "
                "Setting the start attribute to a later date provides the ability "
                "to skip years of data at the beginning of a run. Please check the "
                "start attribute of all postprocessing components."))
        else:
            _log.debug(f"cpt.sim0 from start attribute: {cpt.sim0}")
    else:
        cpt.sim0 = FREUtil.parseFortranDate(exp.basedate)
        _log.debug(f"cpt.sim0 from exp.basedate: {cpt.sim0}")
    if not cpt.sim0:
        logs.mailuser(f"{pp.relfrepp} had a problem calculating cpt.sim0. Please contact Amy.")
        logs.sysmailuser()
        _log.critical(f"{pp.relfrepp} had a problem calculating cpt.sim0. Please contact Amy.")
        sys.exit(1)
    startflag = FREUtil.dateCmp(pp.t0, cpt.sim0)
    cpt.startofrun = False
    if startflag == 0:
        _log.debug(f"This is the first postprocessing of the simulation for {component}")
        cpt.startofrun = True
    elif startflag == -1 and not pp.opt['A']:
        _log.debug(f"t0 < cpt.sim0, skipping {pp.t0}-{pp.tEND} for {component}")
        return (pp, exp) # XXX next;
    elif startflag == 1:
        _log.debug(f"t0 > cpt.sim0 for {component}")
    else:
        _log.warning(f"having trouble comparing t0 to cpt.sim0 ({pp.t0},{cpt.sim0}) for {component}")
    _log.debug(f"\tsim0 is {cpt.sim0} (from exp.basedate or start attribute)")

    #get simulation end date from production xml -> simEND
    simTime  = FREUtil.getxpathval('runtime/production/@simTime') #not currently used
    simUnits = FREUtil.getxpathval('runtime/production/@units') #not currently used
    segTime, segUnits = ts_ta.getSegmentLength() # global

    simEND = FREUtil.modifydate(run0, f"+ {simTime} {simUnits} - 1 sec")

    if simEND < pp.tEND and not pp.opt['A']:
        if simEND < pp.t0:
            _log.warning((f"The simulation time calculated from the exp.basedate in your "
                f"diag_table ({exp.basedate}) and the simulation length from the xml ({simTime} "
                f"{simUnits}) ends before this year of postprocessing ({pp.hDate})."))
            simEND = pp.tEND #simEND not currently used for anything else
        else:
            pp.tEND = simEND
            _log.info(f"\tadjusting tEND to simulation end: {pp.tEND}")

    scriptcopy = cshscript
    cpt.depyears = []
    standardTarget, targeterr = FRETargets.standardize(pp.opt['T'])
    # frepp.pl l.1676

# //////////////////////////////////////////////////////////////////////////////#

def timeseries_static(ppcNode, pp, exp, cpt):
    #STATIC
    # frepp.pl l.1678
    this_cshscript = ""
    monthnodes = ppcNode.findnodes('timeSeries[@freq="monthly"]')
    diag_source = ""
    if monthnodes:
        monthnode = ppcNode.findnodes('timeSeries[@freq="monthly"]')->get_node(1) # XXX
        diag_source = monthnode.getAttribute('@source')
    if not diag_source:
        diag_source = ppcNode.findvalue('@source')
    if not diag_source:
        diag_source = f"{cpt.component}_month"
    if not pp.opt['l']:
        diag_source = re.sub(r'_.*', r'', diag_source)
    staticfile = f"{exp.ppRootDir}/{cpt.component}/{cpt.component}.static.nc"
    _log.debug(f"\tstatic vars from '{diag_source}'")
    if not pp.opt['A']:
        this_cshscript += sub.staticvars(diag_source, exp.ptmpDir, exp.tmphistdir, exp.refinedir)
    # frepp.pl l.1692
    cpt.ts_ta_update(this_cshscript, new_hsmfiles=None, dep=None)
    return cpt

# //////////////////////////////////////////////////////////////////////////////#

#sort array by interval attribute
by_interval = op.methodcaller('findvalue', '@interval')

# for ta_freq in ('monthly', 'annual', 'seasonal':)

def timesaverages_setup(ppcNode, ta_freq, pp, exp, cpt):
    """Setup for loop over time averages for a given time average interval."""
    if ta_freq == 'monthly':
        xpath_expn = 'timeAverage[@source="monthly"]'
    elif ta_freq == 'annual':
        xpath_expn = 'timeAverage[@source="annual" and @interval!="1yr"]'
    elif ta_freq == 'seasonal':
        xpath_expn = 'timeAverage[@source="seasonal"]'
    else:
        raise ValueError(ta_freq)

    taNodes = sorted(ppcNode.findnodes(xpath_expn), key=by_interval)
    intervals = [n.findvalue('@interval') for n in taNodes]
    diag_source = " "

    if ta_freq == 'annual':
        # frepp.pl l.1751
        annavnodes = ppcNode.findnodes('timeAverage[@source="annual" and @interval="1yr"]')
        annCalcInterval = ''
        if annavnodes:
            taNode = ppcNode.findnodes('timeAverage[@source="annual" and @interval="1yr"]')->get_node(1) # XXX
            annCalcInterval = taNode.findvalue('@calcInterval')
            if not taNodes or annCalcInterval == "1yr":
                intervals.append(annCalcInterval)
                if not pp.opt['A']:
                    _log.debug("\tannual av int=1yr subint=history")
                    this_cshscript = ts_ta.annualAV1yrfromhist(taNode, cpt.sim0, 1)
                this_cshscript += FREAnalysis.FREAnalysis(pp, exp, node=taNode, type="timeAverage", dtvarsRef=cpt.dtvars) # XXX
                cpt.ts_ta_update(this_cshscript, new_hsmfiles=sub.jpkSrcFiles(taNode), dep=None)
    else:
        annavnodes = None
        annCalcInterval = None

    return [(n, ta_freq, intervals, ppcNode, annavnodes, annCalcInterval) for n in taNodes]

def add_timeaverage(pp, exp, cpt, ta_loop_tuple):
    """Common time average addition code."""
    taNode, ta_freq, intervals, ppcNode, annavnodes, annCalcInterval = ta_loop_tuple
    int_, subint, dep = ts_ta.get_subint(taNode, intervals, pp.t0, cpt.sim0)
    if not subint:
        _log.debug(f"\t{ta_freq} av int={int_} subint=history")
    else:
        _log.debug(f"\t{ta_freq} av int={int_} subint={subint}")
    this_cshscript = ""
    if not pp.opt['A']:
        if ta_freq == 'annual':
            if subint > 1:
                this_cshscript += ts_ta.annualAVfromav(taNode, cpt.sim0, subint)
            else:
                this_cshscript += ts_ta.annualAVxyrfromann(taNode, cpt.sim0, ppcNode, len(annavnodes), annCalcInterval)
        else:
            # monthly, seasonal
            if subint:
                this_cshscript += ts_ta.monthlyAVfromav(taNode, cpt.sim0, subint)
            else:
                this_cshscript += ts_ta.monthlyAVfromhist(taNode, cpt.sim0)
    this_cshscript += FREAnalysis.FREAnalysis(pp, exp, node=taNode, type="timeAverage", dtvarsRef=" ") # XXX
    cpt.ts_ta_update(this_cshscript, new_hsmfiles=sub.jpkSrcFiles(taNode), dep=dep)
    return cpt

# //////////////////////////////////////////////////////////////////////////////#

#sort array by chunkLength attribute
by_chunk = op.methodcaller('findvalue', '@chunkLength')

# for ts_freq in ('30min', 'hourly', '2hr', '3hr', '4hr', '6hr', '8hr', '12hr',
#   '120hr', 'daily', 'monthly', 'annual', 'seasonal'):

def timeseries_setup(ppcNode, ts_freq, pp, exp, cpt):
    """Setup for loop over time series for a given sampling frequency."""
    if ts_freq.endswith('min') or ts_freq.endswith('hr') or ts_freq == 'hourly':
        #TIMESERIES - HOURLY
        # frepp.pl l.1911
        xpath_expn = f"timeSeries[\@freq='{ts_freq}']"
    elif ts_freq == 'daily':
        xpath_expn = 'timeSeries[@freq="daily" or @freq="day"]'
    elif ts_freq == 'monthly':
        xpath_expn = 'timeSeries[@freq="monthly" or @freq="month"]'
    elif ts_freq == 'annual':
        xpath_expn = 'timeSeries[@freq="annual"]'
    elif ts_freq == 'seasonal':
        xpath_expn = 'timeSeries[@freq="seasonal"]'
    else:
        raise ValueError(ts_freq)

    tsNodes = sorted(ppcNode.findnodes(xpath_expn), key=by_chunk)
    chunks = [n.findvalue('@chunkLength') for n in tsNodes]
    diag_source = sub.diagfile(ppcNode, ts_freq)
    return [(n, ts_freq, chunks, diag_source) for n in tsNodes]

def add_timeseries(pp, exp, cpt, ts_loop_tuple):
    """Common time series addition code."""
    tsNode, ts_freq, chunks, diag_source = ts_loop_tuple

    if ts_freq.endswith('min') or ts_freq.endswith('hr') \
        or ts_freq in ('hourly', 'daily', 'monthly'):
        # hourly: frepp.pl l.1915, daily: frepp.pl l.1973, monthly: frepp.pl l.2030
        has_subchunk_func = ts_ta.TSfromts
        no_subchunk_func = ts_ta.directTS
    elif ts_freq == 'annual':
        # frepp.pl l.2086
        has_subchunk_func = ts_ta.TSfromts
        no_subchunk_func = functools.partial(
            ts_ta.annualTS,
            diagtablecontent= '\n'.join(exp.diagtablecontent)
        )
    elif ts_freq == 'seasonal':
        # frepp.pl l.2143
        has_subchunk_func = ts_ta.seaTSfromts
        no_subchunk_func = ts_ta.seasonalTS
    else:
        raise ValueError(ts_freq)

    cl, subchunk, dep = ts_ta.get_subint(tsNode, chunks, pp.t0, cpt.sim0)
    if not subchunk:
        _log.debug(f"\t{ts_freq} ts chunklength={cl} subchunk=history")
    else:
        _log.debug(f"\t{ts_freq} ts chunklength={cl} subchunk={subchunk}")
    this_cshscript = ""
    if not pp.opt['A']:
        if subchunk:
            this_cshscript += has_subchunk_func(tsNode, cpt.sim0, subchunk)
        else:
            this_cshscript += no_subchunk_func(tsNode, cpt.sim0, cpt.startofrun)
    this_cshscript += FREAnalysis.FREAnalysis(pp, exp, node=tsNode, type="timeSeries", dtvarsRef=cpt.dtvars) # XXX
    cpt.ts_ta_update(this_cshscript, new_hsmfiles=sub.jpkSrcFiles(tsNode), dep=dep)
    return cpt


# //////////////////////////////////////////////////////////////////////////////#

def component_loop_dependencies(pp, exp, cpt):
    # frepp.pl l.2194
    if cpt.didsomething:
        cpt.didsomething = False
    else:
        _log.info(f"No calculations necessary for year {pp.hDate} for {cpt.omponent}.")
        # if --plus option is used, store next year's frepp command to call at the end
        if pp.opt['plus']:
            exp.exp.frepp_plus_calls.append(sub.form_frepp_call_for_plus_option(cpt.component))
            return (pp, exp) # XXX

    #PROCESS DEPENDENCIES
    depholds = ""
    redothisyear = False

    if not pp.opt['A']:
        #sort, unique dependencies
        cpt.depyears = sorted(list(set(cpt.depyears)))
        _log.debug(f"This frepp year depends on: {cpt.depyears}")
        for depyear in cpt.depyears:
            depfile  = f"{exp.statedir}/{cpt.component}.{depyear}"
            redo = False
            depstate = ''
            if os.exists(depfile):
                with open(depfile, 'r') as f:
                    depstate = FREUtil.cleanstr(f.read())
                _log.info(f"Required year {depyear} has a state file with state "
                    "'{depstate}' for {cpt.component}.")
                if depstate == 'OK':
                    if pp.opt['o']:
                        if pp.opt['s']:
                            _log.info("Redoing anyway because 'overwrite state files' was specified...")
                            redo = True
                            redothisyear = True
                elif depstate == 'FATAL':
                    _log.info((f"Required year {depyear} got an error in multiple attempts, "
                        f"exiting. To retry, delete the state file {depfile}"))
                    return # XXX
                elif depstate == "INTERACTIVE":
                    if pp.opt['s']:
                        _log.info((f"Required year {depyear} has been partially run "
                            f"interactively but not completed, resubmitting {cpt.component}..."))
                        redo = True
                        redothisyear = True
                    else:
                        _log.warning((f"Required year {depyear} has been run interactively "
                            "but not completed, and should be rerun."))
                elif depstate == "ERROR":
                    if pp.opt['s']:
                        _log.info((f"Required year {depyear} got an error in the last "
                            f"frepp attempt, resubmitting {cpt.component}..."))
                        redo = True
                        redothisyear = True
                    else:
                        _log.warning((f"Required year {depyear} got an error in the "
                            "last frepp attempt and should be rerun."))
                elif depstate == "HISTORYDATAERROR":
                    if pp.opt['s']:
                        _log.info((f"Required year {depyear} got a history data error in the last "
                            f"frepp attempt, resubmitting {cpt.component}..."))
                        redo = True
                        redothisyear = True
                    else:
                        _log.warning((f"Required year {depyear} got a history data error in the "
                            "last frepp attempt and should be rerun."))
                elif not depstate:
                    _log.error(f"statefile {depfile} exists but is empty, exiting.")
                    #what if this is the -c split job? continue to other components
                    return # XXX
                else:
                    #check that jobid is still running
                    jobrunning = logs.isjobrunning(depstate)
                    _log.info(f"Checking state of job {depstate}: jobrunning={jobrunning}")
                    if jobrunning:
                        _log.info((f"Required year {depyear} still working, placing a hold for "
                            f"{cpt.component}"))
                        depholds = depholds + depstate + ":"
                    else:
                        if pp.opt['s']:
                            _log.info(f"Required year {depyear} job was lost, resubmitting {cpt.component}...")
                            redo = True
                            redothisyear = True
                        else:
                            _log.warning(f"Required year {depyear} job was lost and should be rerun.")
            else:
                if pp.opt['s']:
                    _log.info(f"Required year {depyear} missing, resubmitting {cpt.component}...")
                    redo = True
                    redothisyear = True
                else:
                    _log.warning(f"Required year {depyear} missing. Use 'frepp -s' to submit with dependencies.")

            if redo:
                cmd = sub.call_frepp(pp.abs_xml_path, exp.outscript, cpt.component, depyear, '', pp)
                _log.info(f"cmd")
                frepp_submit_output = util.shell(cmd, log=_log)
                _log.info(f"frepp_submit_output")
                frepp_submit_output_last_line = frepp_submit_output.splitlines()[-1]
                depjobid = re.match(r"Submitted batch job (\d+)", frepp_submit_output_last_line)
                if "has already been completed" not in frepp_submit_output_last_line:
                    if not depjobid and not frepp_submit_output_last_line:
                        _log.info(f"No jobid resulted from the job submission of {depyear}.")
                        _log.error("Unable to submit dependent jobs, exiting.")
                        sys.exit(1)
                    elif not depjobid:
                        _log.error(("the jobid returned has the wrong format: a "
                            "frepp or batch system issue occurred."))
                        sys.exit(1)
                    else:
                        depjobid = depjobid.group(1)
                        _log.info(f"Dependent job for {depyear}='{depjobid}'")
                        depstatefile = f"{exp.statedir}/{cpt.component}.{depyear}"
                        depholds = depholds + depjobid + ":"
                        with open(depstatefile, 'w') as f:
                            f.write(depstatefile)

    if redothisyear and not pp.opt['A']:
        _log.info(f"This year ({pp.hDate}) has unmet dependencies for {cpt.component}, submitting with holds.")
    else:
        _log.debug(f"All dependencies met or known for {pp.hDate} for {cpt.component}.")

    if depholds:
        depholds = depholds[0:-1]
        pp.opt['w'] = f" --dependency=afterok:{depholds}"
        _log.info(f"Setting holds for {pp.hDate}: {pp.opt['w']}")

    cpt.cshscript += logs.mailcomponent()

    #CPIO
    if cpt.cpiomonTS and exp.aggregateTS:
        cpt.cshscript += cpt.cpiomonTS

    #END OF THIS COMPONENT; REMOVE CHECKPOINT FILE
    cpt.cshscript += f"rm -f {exp.ppRootDir}/.checkpoint/$checkptfile\n"

    if pp.opt['c']:
        #set up dmget, set up to postprocess the following year if necessary, write script
        exp.mkdircommand = sub.createdirs(exp.mkdircommand)
        hf = sub.dmget_files()
        hsmf = sub.jpk_hsmget_files()
        if hf:
            hsmget_history = sub.hsmget_history_csh(
                exp.ptmpDir, exp.tmphistdir, exp.refinedir, exp.this_frepp_cmd,
                ' '.join(hf), ' '.join(hsmf)
            )
            cpt.cshscript = cpt.cshscript.replace("#hsmget_history_files", hsmget_history)
            uncompress = sub.uncompress_history_csh(exp.tmphistdir)
            cpt.cshscript = cpt.cshscript.replace("#uncompress_history_files", uncompress)
            hf.sort()
            check_history = sub.checkHistComplete(exp.tmphistdir, hf[0], exp.this_frepp_cmd, hsmf, exp.diagtablecontent)
            cpt.cshscript = cpt.cshscript.replace("#check_history_files", check_history)
        cpt.cshscript += sub.call_frepp(exp.abs_xml_path, exp.outscript, cpt.component, "", "", pp)
        cpt.cshscript += f"echo END-OF-SCRIPT for postprocessing job {pp.t0}-{pp.tEND} for {exp.expt}\n"

        # if the user sets -W, don't override the wallclock even for 1-year postprocessing
        if exp.maxyrs < 2 and not pp.opt['Walltime']:
            if pp.platform == 'x86_64':
                cshscript = re.sub(r'(#SBATCH --time).*', r'\1=20:00:00/', cpt.cshscript)
        if exp.maxyrs >= 20:
            if pp.platform == 'x86_64':
                cshscript = re.sub(r'(#SBATCH --time).*', rf"\1={pp.platform_opt['maxruntime']}", cshscript)
        cpt.cshscript = re.sub(r'(#INFO:max_years=)', rf'\1{exp.maxyrs}', cpt.cshscript)
        writefinalstate = _template("""

            if ( \$errors_found == 0 ) then
                echo OK > $exp.statefile
            else if ( "\$prevjobstate" == "ERROR" ) then
                echo FATAL > $statefile
            else
                echo ERROR > $statefile
            endif
        """, statefile=exp.statefile)
        if not pp.opt['A']:
            sub.writescript(
                cpt.cshscript + writefinalstate,
                exp.outscript,
                f"{pp.platform_opt['batchSubmit']}{pp.opt['w']} {pp.opt['m']}",
                exp.statefile, pp
            )
            pp.opt['w'] = ""
        # frepp.pl l.2436
    return (pp, exp)
