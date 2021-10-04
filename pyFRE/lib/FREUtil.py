"""Transliteration of FRE/lib/FREUtil.pm.
"""
import os
import pwd

import logging
_log = logging.getLogger(__name__)

ARCHIVE_EXTENSION = "qr/\.(?:nc\.cpio|cpio|nc\.tar|tar)/"
MAPPING_SEPARATOR = ';'

# TODO
def checkExptExists(XXX0, XXX1):
    """Make sure experiment exists in xml."""
    # FREUtil.pm l.34
    raise NotImplementedError()

# TODO
def getxpathval(XXX0, XXX1):
    """Gets a value from xml, recurse using @inherit and optional second
    argument $expt.
    """
    # FREUtil.pm l.57
    raise NotImplementedError()

def writescript(outscript, batchCmd, defaultQueue, npes, stdoutPath, project, maxRunTime):
    """Write c-shell runscript, chmod, and optionally submit.
    """
    # FREUtil.pm l.86
    raise NotImplementedError()

# TODO
def parseDate(date):
    """Convert a date string following either yyyy or yyyymmddinto to a
    Date::Manip date ('yyyymmddhh:mm:ss').
    """
    # FREUtil.pm l.139
    # As we move forward to allow for years past 9999, we need to set some
    # guidance on how the passed in date string is interpreted.  Thus, we
    # somewhat arbitrarily decide that if length($opt_t) < 7, we assume a
    # year has been passed in, 8 and beyond assume yyyymmdd.
    raise NotImplementedError()

# TODO
def parseFortranDate(date):
    """Convert a fortran date string ( "1,1,1,0,0,0" ) to a Date::Manip date.
    """
    # FREUtil.pm l.188
    raise NotImplementedError()

def padzeros(date):
    """Pad to 4 digits."""
    # FREUtil.pm l.206
    return "%04d" % int(date)

def pad2digits(date):
    """Pad to 2 digits."""
    # FREUtil.pm l.212
    return "%02d" % int(date)

#pad to 8 digits
def pad8digits(date):
    # FREUtil.pm l.218
    return "%08d" % int(date)

# TODO
def splitDate(date):
    """splitDate separates a Date::Manip date string into yyyy and
    mmddhh or mmddhh:mm:ss components.
    """
    # FREUtil.pm l.225
    raise NotImplementedError()

def unixDate(dateTime, format):
    """unixDate is a simplified version of Date::Manip::UnixDate.  This
    simplified version will only return the date using two formats:
    "%Y%m%d", and "%Y%m%d%H".
    """
    # FREUtil.pm l.242
    # The reason for this is some versions of Date::Manip::UnixDate cannot
    # deal with year 0001 --- which is needed for several of the GFDL
    # models.  The use of Date::Manip::UnixDate in FRE is to return one of
    # these two formats.
    # This subroutine expects $dateTime to be an the format:
    #   /^\d{4,}\d{4}\d{2}:\d{2}:\d{2}$/
    # or the routine will return undef.
    raise NotImplementedError()

# TODO
def dateCalc(date1, date2):
    """Calculate the difference between two date and returns a Date::Manip
    delta format.
    """
    # FREUtil.pm l.274
    #   This is not a full wrapper for Date::Manip::DateCalc
    # as we only use it to calculate the difference between two dates.
    raise NotImplementedError()

# TODO
def modifydate(date, str_):
    """Wrapper for DateCalc handling low year numbers."""
    # FREUtil.pm l.322
    # modifydate takes a date (usually of format yyyymmddhh:mm:ss), and modifies it via the
    # instructions in $str (i.e. +1 year, -1 second --- using the manipulation rules for
    # Date::Manip.
    raise NotImplementedError()

def isaLeapYear(year):
    """Determine if a given year is a leap year."""
    # FREUtil.pm l.393
    # This routine is to be used within FREUtil.pm.  This is why no checks
    # are done to determine if the year passed in is valid.  The routine
    # calling isaLeapYear should perform the validity check.
    raise NotImplementedError()

def daysInMonth(month):
    """Return number of days in month."""
    # FREUtil.pm l.409
    # This routine is only to be used within FREUtil.pm.  This is why no
    # checks are done to determine if the input month number is valid.
    return [31,28,31,30,31,30,31,31,30,31,30,31][month-1]

def daysSince01Jan(mon, day, year):
    # FREUtil.pm l.421
    # This routine is only to be used within FREUtil.pm, this is why we
    # ignore checks to determine if the passed in date is valid
    raise NotImplementedError()

# TODO
def daysSince1BC(mon, day, year):
    """Wrapper to Date::Manip::Date_DaysSince1BC to deal with possible years
    beyond 9999.
    """
    # FREUtil.pm l.436
    raise NotImplementedError()

# TODO
def dateCmp(date1, date2):
    """Wrapper for Date::Manip::Date_Cmp."""
    # FREUtil.pm l.472
    # As Date_Cmp for DM5 "does little
    # more than use 'cmp'." However, since cmp will not work as required if
    # the two strings have different lengths, this wrapper uses cmp on the
    # separate date components.
    raise NotImplementedError()

# TODO
def graindate(date, freq):
    """Return appropriate date granularity."""
    # FREUtil.pm l.512
    raise NotImplementedError()

# TODO
def timeabbrev(freq):
    """Return appropriate abbreviation."""
    # FREUtil.pm l.586
    raise NotImplementedError()

# TODO
def getppNode(e):
    """Find correct postProcess node to use, following inherits."""
    # FREUtil.pm l.615
    raise NotImplementedError()

def cleanstr(str_):
    """Clean up a string that should be space delimited tokens."""
    # FREUtil.pm l.636
    return str_.strip().replace('\n', ' ').replace(',', ' ').replace(' +', ' ')

def makeminutes(timevar):
    """Translates $string in "HH:MM:SS" format to minutes integer."""
    # FREUtil.pm l.649
    raise NotImplementedError()

def strStripPaired(s, t):
    """Strip paired substrings, surrounding the $string. All the heading and
    tailing whitespaces will be stripped as well.
    """
    # FREUtil.pm l.665
    raise NotImplementedError()

def strFindByPattern(mapPattern, keys):
    # FREUtil.pm l.677
    raise NotImplementedError()

def strFindByInterval(m, n):
    # FREUtil.pm l.714
    raise NotImplementedError()

def listUnique(list_):
    """Return the argument @list with all the duplicates removed."""
    # FREUtil.pm l.749
    return list(set(list_))

def listDuplicates(list_):
    """Return the all the duplicates found in the argument @list."""
    # FREUtil.pm l.759
    raise NotImplementedError()

def fileOwner(filename):
    """Returns owner of the $filename."""
    # FREUtil.pm l.769
    return pwd.getpwuid(os.stat(filename).st_uid).pw_name

def fileIsArchive(filename):
    """Returns 1 if the $filename is archive."""
    # FREUtil.pm l.778
    raise NotImplementedError()

def fileArchiveExtensionStrip(filename):
    """Returns the $filename with archive extension stripped."""
    # FREUtil.pm l.787
    raise NotImplementedError()

def createDir(d):
    """Create a (multilevel) directory, passed as an argument. Return the created
    directory or an empty value."""
    # FREUtil.pm l.797
    d = os.path.abspath(d)
    os.makedirs(d)
    return d

def dirContains(d, s):
    """Return a number of times the $string is contained in the $dirName."""
    # FREUtil.pm l.817
    raise NotImplementedError()

def environmentVariablesExpand(s):
    """Expand environment variable placeholders in the given $string."""
    # FREUtil.pm l.827
    raise NotImplementedError()

def timeString(time=None):
    """Converts time to a human-decipherable string. Suitable for use in a
    filename (sortable, no spaces, colons, etc). Resolution of seconds.
    """
    # FREUtil.pm l.834
    raise NotImplementedError()

def jobID():
    """Return the current job id, if it's available."""
    # FREUtil.pm l.860
    if os.environ.get("SLURM_JOB_ID", False):
        return os.environ["SLURM_JOB_ID"]
    elif os.environ.get("JOB_ID", False):
        return os.environ["JOB_ID"]
    elif os.environ.get("PBS_JOBID", False):
        return os.environ["PBS_JOBID"]
    else:
        return '000000'

def home():
    # FREUtil.pm l.879
    return os.environ["FRE_COMMANDS_HOME"]

def optionIntegersListParse(name, value):
    # FREUtil.pm l.886
    raise NotImplementedError()

def optionValuesListParse(name, value, allowedValuesList):
    # FREUtil.pm l.919
    raise NotImplementedError()

def decodeChildStatus(child_error, os_error):
    """Returns decoded child process native error status as a string."""
    # FREUtil.pm l.950
    # ------ The child process native status is a word. The low order byte's
    # ------ lowest 7 bits hold the signal number if the child was terminated
    # ------ by a signal. The high order byte holds the exit status if the
    # ------ child exited. See ``perldoc -f system'' for more information.
    raise NotImplementedError()
