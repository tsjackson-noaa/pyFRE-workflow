"""Transliteration of logging subroutines in FRE/bin/frepp.pl.
"""

import logging
_log = logging.getLogger(__name__)

def setcheckpt(cpt_name):
    """Set up checkpointing."""
    raise NotImplementedError()

def errorstr(msg):
    """Call this to set up a check. Had to use a temp file in case strings get
    too long, want to preserve end-of-lines."""
    raise NotImplementedError()

def retryonerrorstart(cmd):
    """"""
    raise NotImplementedError()

def retryonerrorend(msg):
    """"""
    raise NotImplementedError()

def fatalerrorstr(msg, outscript):
    """Fatal error - exit script immediately, but email the user the error first."""
    raise NotImplementedError()

def mailerrors(outdir):
    """Appends the current batch of errors to a text file for mailing to the user.
    Call this at the end of each piece of postprocessing, or you'll have too
    few/many messages.
    """
    raise NotImplementedError()

def mailcomponent():
    """Mail the user the csh errors that may have accumulated in work/.errorssend.
    Call this at the end of each component."""
    raise NotImplementedError()

def mailuser(msg):
    """If a batch job, build an error string of the bad news from perl."""
    raise NotImplementedError()

def sysmailuser():
    """Mail user any errors at end of perl script execution."""
    raise NotImplementedError()

def begin_systime():
    """Begin system timings."""
    raise NotImplementedError()

def end_systime():
    """End system timings."""
    raise NotImplementedError()

def isjobrunning(jobid):
    """Check whether a job is running."""
    raise NotImplementedError()