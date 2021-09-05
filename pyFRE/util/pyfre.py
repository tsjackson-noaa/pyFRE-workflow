"""Subroutines for FRE perl to python transliteration."""

import collections
import re
import string
from textwrap import dedent

from . import basic, processes, exceptions

def pl_template(cmds_template, *args, **kwargs):
    """Do perl-type string templating."""
    cmds_template = dedent(cmds_template)

    # build dict of templating replacements
    template_vals = basic.ConsistentDict
    for arg in args:
        if isinstance(arg, dict):
            template_vals.update(arg)
        if hasattr(arg, 'template_dict'):
            template_vals.update(arg.template_dict())
    template_vals.update(kwargs)

    # Template escapes $ delimiter via $$; replaced by single $ in output
    cmds = string.Template(cmds_template.replace('\$', '$$'))
    if not template_vals:
        return cmds # quicker?
    return cmds.safe_substitute(template_vals)

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
