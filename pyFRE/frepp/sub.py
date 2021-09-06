"""Transliteration of utility subroutines in FRE/bin/frepp.pl.
"""

import functools
import math
import operator
from textwrap import dedent

import pyFRE.util as util
_template = util.pl_template # abbreviate

import logging
_log = logging.getLogger(__name__)

def writescript(script, outscript, batchCmd, statefile):
    """Write c-shell runscript, chmod, and optionally submit."""
    raise NotImplementedError()

def call_frepp(abs_xml_path, outscript, component, year, depjobs, csh):
    """Set up to postprocess the following year if necessary."""
    raise NotImplementedError()

def execute(host, command):
    """Execute a command on the workstation that can write to archive."""
    raise NotImplementedError()

def form_frepp_call_for_plus_option():
    """Generate the frepp command for next year's frepp call when using the
    --plus option."""
    raise NotImplementedError()

def createcpio(cache, outdir, prefix, abbrev, dmputOnly):
    """Create a cpio and dmput original files.  Also dmput only when a cpio is
    not created."""
    raise NotImplementedError()

def diagfile(ppcNode, freq, src):
    """Get the name of the diagnostic output file from the source attribute."""
    raise NotImplementedError()

def refineDiag(tmphistdir, stdoutdir, ptmpDir, basedate, refinedir, gridspec, mdbi):
    """hsmget history data into work area, provide directory for user to put new
    history files, run user scripts, package the new data into new history file.
    """
    raise NotImplementedError()

def getTemplate(platform, workdir):
    """"""
    cshscripttmpl = ""
    if platform == 'x86_64':
        cshscripttmpl += _template("""
            #!/bin/csh -f
            #SBATCH --job-name
            #SBATCH --time
            #SBATCH --ntasks=1
            #SBATCH --output
            #SBATCH --chdir
            #SBATCH --comment
            #SBATCH --mail-type=NONE
            #SBATCH --mail-user
            #INFO:component=
            #INFO:max_years=

            if ( \$?SLURM_JOBID ) then
                setenv JOB_ID \$SLURM_JOBID
            else
                setenv JOB_ID `mktemp -u INT-XXXXXX`
            endif

            setenv FRE_STDOUT_PATH
            if ( -d "$workdir" ) then
            rm -rf $workdir
            endif
            mkdir -p $workdir
        """, workdir=workdir)

    cshscripttmpl += _template("""
        #=======================================================================
        #version_info
        #=======================================================================
        unalias *
        set echo
        #get_site_config
        ########################################################################
        #-------------------- variables set by script --------------------------
        ########################################################################
        set name
        set rtsxml
        set work
        set tempCache
        set root
        set archive
        set scriptName
        set oname
        set ptmpDir
        set histDir
        set platform
        set target
        set segment_months
        set prevjobstate
        set statefile
        set experID
        set realizID
        set runID
        set tripleID

        #platform_csh

        #write_to_statefile

        limit stacksize unlimited
        setenv FMS_FRE_FREPP
        set NCVARS = list_ncvars.csh
        set TIMAVG = "timavg.csh -mb"
        set PLEVEL = plevel.sh
        set SPLITNCVARS = split_ncvars.pl
        set MPPNCCOMBINE = mppnccombine
        set FREGRID = fregrid
        set checkptfile = $scriptName:t
        set errors_found = 0
        if (! -d $work) mkdir -p $work
        if (! -d $tempCache) mkdir -p $tempCache
        which ncks
        which ncrcat

        #set up HSM
        set hsmget = "hsmget -v -m $FRE_COMMANDS_HOME/site/gfdl/hsmget.mk -t";
        set hsmput = "hsmput -v -m $FRE_COMMANDS_HOME/site/gfdl/hsmput.mk -t";
        if ( $?HSM_HOME ) then
            if ( -d $HSM_HOME ) then
                set hsmget = 'hsmget -v -t';
                set hsmput = 'hsmput -v -t';
            endif
        endif

        #checkpointing option to skip to certain point in script
        set options = ( )
        set argv = (`getopt g: $*`)
        while ("$argv[1]" != "--")
            switch ($argv[1])
                case -g:
                    set checkpt = $argv[2]; shift argv; breaksw
            endsw
            shift argv
        end
        shift argv
    """)
    return cshscripttmpl

def epmt_transform(scriptfile):
    """"""
    raise NotImplementedError()

# ------------------------------------------------------------------------------

def jpkSrcFiles(node):
    raise NotImplementedError()

def jpk_hsmget_files(hsmfiles):
    # hsmfiles: str
    hsmf = list(set(hsmfiles.split(',')))
    _log.debug(f"diagnostic files to be extracted: {hsmf}")
    return hsmf

def dmget_files(historyfiles):
    """Sort, uniq list of history files to dmget."""
    # historyfiles: str
    hf = list(set(historyfiles.split(','))).sort()
    _log.debug(f"historyfiles used: {hf}")
    return hf

def hsmget_history_csh(ptmpDir, tmphistdir, refinedir, this_frepp_cmd, hf, hsmf):
    """Return the csh to extract history files."""
    raise NotImplementedError()

def uncompress_history_csh(dir_):
    """Uncompress netcdf compression."""
    raise NotImplementedError()

def compress_csh(file, check_nccopy):
    """Compress pp files before placing in archive."""
    raise NotImplementedError()

def checkHistComplete(dir_, hf, frepp_cmd, usedfiles, diagtablecontent):
    """Check for complete history data."""
    raise NotImplementedError()

def createdirs(mkdircommand):
    """Make directories."""
    raise NotImplementedError()

# ------------------------------------------------------------------------------

def lcm(*args):
    return functools.reduce(operator.mul,args, 1) // math.gcd(args)
gcd = math.gcd

