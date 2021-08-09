"""All framework-specific exceptions are placed in a single module to simplify
imports.
"""
import os
import errno
from subprocess import CalledProcessError

import logging
_log = logging.getLogger(__name__)


def exit_on_exception(exc, msg=None):
    """Prints information about a fatal exception to the console beofre exiting.
    Use case is in user-facing subcommands (``mdtf install`` etc.), since we
    have more sophisticated logging in the framework itself.
    Args:
        exc: :py:class:`Exception` object
        msg (str, optional): additional message to print.
    """
    # if subprocess failed, will have already logged its own info
    print(f'ERROR: caught exception {repr(exc)}')
    if msg:
        print(msg)
    exit(1)

def chain_exc(exc, new_msg, new_exc_class=None):
    if new_exc_class is None:
        new_exc_class = type(exc)
    try:
        if new_msg.istitle():
            new_msg = new_msg[0].lower() + new_msg[1:]
        if new_msg.endswith('.'):
            new_msg = new_msg[:-1]
        new_msg = f"{exc_descriptor(exc)} while {new_msg.lstrip()}: {repr(exc)}."
        raise new_exc_class(new_msg) from exc
    except Exception as chained_exc:
        return chained_exc

def exc_descriptor(exc):
    # MDTFEvents are raised during normal program operation; use correct wording
    # for log messages so user doesn't think it's an error
    if isinstance(exc, MDTFEvent):
        return "Received event"
    else:
        return "Caught exception"

class MDTFBaseException(Exception):
    """Base class to describe all MDTF-specific errors that can happen during
    the framework's operation."""

    def __repr__(self):
        # full repr of attrs of child classes may take lots of space to print;
        # instead just print message
        return f'{self.__class__.__name__}("{str(self)}")'

class MDTFFileNotFoundError(FileNotFoundError, MDTFBaseException):
    """Wrapper for :py:class:`FileNotFoundError` which handles error codes so we
    don't have to remember to import :py:mod:`errno` everywhere.
    """
    def __init__(self, path):
        super(MDTFFileNotFoundError, self).__init__(
            errno.ENOENT, os.strerror(errno.ENOENT), path
        )

class MDTFFileExistsError(FileExistsError, MDTFBaseException):
    """Wrapper for :py:class:`FileExistsError` which handles error codes so we
    don't have to remember to import :py:mod:`errno` everywhere.
    """
    def __init__(self, path):
        super(MDTFFileExistsError, self).__init__(
            errno.EEXIST, os.strerror(errno.EEXIST), path
        )

class MDTFCalledProcessError(CalledProcessError, MDTFBaseException):
    """Wrapper for :py:class:`subprocess.CalledProcessError`."""
    pass

class UnitsError(ValueError, MDTFBaseException):
    """Raised when trying to convert between quantities with physically
    inequivalent units.
    """
    pass

class MixedDatePrecisionException(MDTFBaseException):
    """Exception raised when we attempt to operate on :class:`Date` or
    :class:`DateRange` objects with differing levels of precision, which shouldn't
    happen with data sampled at a single frequency.
    """
    def __init__(self, func_name='', msg=''):
        self.func_name = func_name
        self.msg = msg

    def __str__(self):
        return ("Attempted datelabel method '{}' on FXDate "
            "placeholder: {}.").format(self.func_name, self.msg)

class FXDateException(MDTFBaseException):
    """Exception raised when :class:`FXDate` or :class:`FXDateRange` classes,
    which are placeholder/sentinel classes used to indicate static data with no
    time dependence, are accessed like real :class:`Date` or :class:`DateRange`
    objects.
    """
    def __init__(self, func_name='', msg=''):
        self.func_name = func_name
        self.msg = msg

    def __str__(self):
        return ("Attempted datelabel method '{}' on FXDate "
            "placeholder: {}.").format(self.func_name, self.msg)

class MetadataError(MDTFBaseException):
    """Exception signaling unrecoverable errors in variable metadata.
    """
    pass

class UnitsUndefinedError(MetadataError):
    """Exception signaling unrecoverable errors in variable metadata.
    """
    pass

