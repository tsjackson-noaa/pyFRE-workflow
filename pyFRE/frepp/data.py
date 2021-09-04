"""Transliteration of file handling subroutines in FRE/bin/frepp.pl.
"""

import logging
_log = logging.getLogger(__name__)


def execute(host, command):
    """Execute a command on the workstation that can write to archive."""
    raise NotImplementedError()

def call_frepp(abs_xml_path, outscript, component, year, depjobs, csh):
    """Set up to postprocess the following year if necessary."""
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

def writescript(script, outscript, batchCmd, statefile):
    """Write c-shell runscript, chmod, and optionally submit."""
    raise NotImplementedError()

def getTemplate(platform):
    """"""
    raise NotImplementedError()

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