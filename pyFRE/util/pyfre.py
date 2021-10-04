"""Subroutines for FRE perl to python transliteration."""

import os
import collections
import re
import string
from textwrap import dedent
import time

from . import basic, processes, exceptions

def is_readable(path):
    """Check if filesystem says *path* is readable."""
    return os.access(path, os.R_OK)

def is_writable(path):
    """Check if filesystem says *path* is writable."""
    return os.access(path, os.W_OK)

def unix_epoch():
    """Seconds since 1 Jan 1970, equivalent of shell `date +%s`."""
    return int(time.time())

def regex_match(regex_, str_):
    pass

def regex_search():
    pass

def pl_template(cmds_template, *args, auto_escape_dollars=True, **kwargs):
    """Do perl-type string templating."""
    cmds_template = dedent(cmds_template)

    if not args and not kwargs:
        # first mode: no templating, $ aren't escaped, just concat multiline string
        if not auto_escape_dollars:
            cmds_template = cmds_template.replace('\$', '$')
        return cmds_template

    # build dict of templating replacements
    template_vals = basic.ConsistentDict
    for arg in args:
        if isinstance(arg, dict):
            template_vals.update(arg)
        if hasattr(arg, 'template_dict'):
            template_vals.update(arg.template_dict())
    template_vals.update(kwargs)

    # string.Template escapes $ delimiter as $$; replaced by single $ in output
    if auto_escape_dollars:
        cmds = string.Template(cmds_template.replace('\$', '$$'))
    else:
        # pass through '\$' as written
        cmds = string.Template(cmds_template.replace('\$', '\$$'))
    return cmds.substitute(template_vals)

def shell(cmd, log, **kwargs):
    """Replace shell one-liners."""
    proc = processes.run_shell(cmd, log=log, **kwargs)
    if proc.retcode != 0:
        raise exceptions.MDTFCalledProcessError(proc.stderr)
    return proc.stdout.rstrip('\n')

class ScriptTemplateParts(collections.OrderedDict):
    """Class to split up the csh runscript into pieces as it's being assembled."""
    def __init__(self, *args, **kwargs):
        super(ScriptTemplateParts, self).__init__(*args, **kwargs)
        self["_header"] = ""

    def prepend(self, key, value):
        """Convenience method to insert key:value at the start of the dict."""
        self[key] = value
        self.move_to_end(key, last=False)

    def strip_comments(self):
        """Remove shell comments (starting with #)."""

    def join(self):
        """Join parts to a single string."""
        return '\n'.join(self.values())

    def sub(self, old_pat, new_pat):
        """Apply a regex substitution to all parts of the script."""
        for k, v in self.items():
            self[k] = re.sub(old_pat, new_pat, v)
