"""Transliteration of logging subroutines in FRE/bin/frepp.pl.
"""
import os
import time

import pyFRE.util as util

import logging
_log = logging.getLogger(__name__)

_template = util.pl_template # abbreviate

def _opt_Q_test():
    return (_log.level >= logging.WARNING)

def setcheckpt(ts_ta_name, cpt):
    """Set up checkpointing."""
    # frepp.pl l.3055
    checkpt = f"{cpt.component}_{ts_ta_name}"
    if ts_ta_name != "staticvars":
        cpt.didsomething = True
    csh = _template("""

        if ( \$errors_found == 0 ) then
            echo $checkpt > $ppRootDir/.checkpoint/\$checkptfile
        endif
        if ( -f /home/gfdl/flags/fre/checkpoint.\$HOST || -f /home/gfdl/flags/fre/checkpoint.all || -f /home/gfdl/flags/fre/jobs/checkpoint.\$JOB_ID || -f \$HOME/fre.checkpoint.\$JOB_ID ) then
            set now = `date +\%s`
            echo "Exiting early by HPCS request at \$now, will resume with $checkpt"
            Mail -s "\$name job \$JOB_ID has been checkpointed by frepp" $mailList <<END
                Your FRE post-processing job ( \$JOB_ID ) has been stopped and resubmitted
                to the batch queue.  It will be re-run by the operators as soon as possible
                and resume calculating $checkpt.

                Job details:
                \$name running on \$HOST
                Batch job stdout:
                \$FRE_STDOUT_PATH
            END
            sleep 30
            exit 99
        endif
        $checkpt:
    """)
    return csh

def errorstr(msg):
    """Call this to set up a check. Had to use a temp file in case strings get
    too long, want to preserve end-of-lines."""
    # frepp.pl l.3090
    if _opt_Q_test():
        return ""
    return _template("""

        if ( \$status != 0 ) then
            @ errors_found += 1
            echo "ERROR: $msg"
            echo "ERROR: $msg" >> \$work/.errors
            exit 1
        endif

    """, msg=msg)

def retryonerrorstart(cmd):
    """"""
    # frepp.pl l.3106
    if _opt_Q_test():
        return "if ( \$status != 0 ) then\n"
    return _template("""
        set thisstatus = \$status
        if ( \$thisstatus != 0 ) then
            echo ERROR: $cmd returned status \$thisstatus
            sleep 30
    """, cmd=cmd)

def retryonerrorend(msg):
    """"""
    # frepp.pl l.3118
    if _opt_Q_test():
        return "endif\n"
    return _template("""
            set thisstatus = \$status
            if ( \$thisstatus != 0 ) then
                echo ERROR ON RETRY: status \$thisstatus
                @ errors_found += 1
                echo "ERROR: $msg"
                echo "ERROR: $msg" >> \$work/.errors
            else
                echo RETRY SUCCESSFUL.
            endif
        endif
    """, msg=msg)

def fatalerrorstr(msg, outscript):
    """Fatal error - exit script immediately, but email the user the error first."""
    # frepp.pl l.3139
    if _opt_Q_test():
        return ""
    createmailfile = mailerrors(archivedir)
    sendemail = mailcomponent()
    return _template("""

        if ( \$status != 0 ) then
            @ errors_found += 1
            echo "FATAL ERROR: $msg"
            echo "FATAL ERROR: $msg" >> \$work/.errors
            $createmailfile
            cat >> \$work/.errorssend <<END

                You will need to resubmit the job when the error conditions are resolved:
                $batchSubmit $outscript
            END
            $sendemail
            exit 1
        endif

    """, locals()) # XXX need more vars

def mailerrors(outdir):
    """Appends the current batch of errors to a text file for mailing to the user.
    Call this at the end of each piece of postprocessing, or you'll have too
    few/many messages.
    """
    # frepp.pl l.3168
    if _opt_Q_test():
        return ""
    return _template("""

        if ( -e \$work/.errors ) then
            set errorlines = `cat \$work/.errors | wc -l`
            if ( \$errorlines ) then
                cat > \$work/.errorshead <<END

                    Errors are reported by the shell script created by $relfrepp working in
                    $outdir
                    Grep for 'ERROR' in
                    \$FRE_STDOUT_PATH
                    for details.
                END
                if ( -e \$work/.errorssend ) then
                cat \$work/.errorshead \$work/.errors >> \$work/.errorssend
                else
                cat \$work/.errorshead \$work/.errors > \$work/.errorssend
                endif
            endif
            $time_rm rm -rf \$work/.errors \$work/.errorshead
        endif

    """, locals()) # XXX need more vars


def mailcomponent():
    """Mail the user the csh errors that may have accumulated in work/.errorssend.
    Call this at the end of each component."""
    # frepp.pl l.3199
    if _opt_Q_test():
        return ""
    return _template("""

        if ( -e \$work/.errorssend ) then
            set errorlines = `cat \$work/.errorssend | wc -l`
            if ( \$errorlines ) then
                Mail -s "$relfrepp CSH ERROR: $expt $component $hDate" $mailList < \$work/.errorssend
                $writestate
                sleep 30
            endif
            $time_rm rm -rf \$work/.errors*
        endif

    """)  # XXX need more vars

def mailuser(msg, perlerrors):
    """If a batch job, build an error string of the bad news from perl."""
    # frepp.pl l.3219
    return f"ERROR: {msg}\n\n{perlerrors}"

def sysmailuser(perlerrors, exp):
    """Mail user any errors at end of perl script execution."""
    # frepp.pl l.3225
    if perlerrors:
        jobid = os.environ.get('SLURM_JOBID', False)
        if jobid:
            outpath = os.path.join(exp.stdoutdir, 'postProcess', f"{os.environ['SLURM_JOB_NAME']}.o{jobid}")
            str_ = _template("""
                Mail -s "$relfrepp PERL ERROR: $expt $hDate" $mailList <<END
                    Error message(s) have been reported by $relfrepp while
                    creating postprocessing scripts for year $hDate of the
                    experiment $expt, in the stdout file

                    $outpath

                    $perlerrors
                END
            """, locals(), exp) # XXX need more vars
            util.shell(str_, log=_log)
            time.sleep(30)

def begin_systime():
    """Begin system timings."""
    # frepp.pl l.3248
    return _template("""
        echo SYSTEM TIME FOR \$outdir
        set systime1 = `$systimecmd`

    """) # XXX need more vars

def end_systime():
    """End system timings."""
    # frepp.pl l.3258
    return _template("""
        set systime2 = `$systimecmd`
        @ ttlsystime = \$systime2 - \$systime1
        echo TOTAL SYSTEM TIME = \$ttlsystime
        set diskusage = `du -ksh \$TMPDIR | cut -f1`
        echo TOTAL DISK USAGE IN TMPDIR "\$TMPDIR": \$diskusage
        echo '================================================'

    """) # XXX need more vars

def isjobrunning(jobid):
    """Check whether a job is running."""
    # frepp.pl l.3272
    squeue_out = util.shell(f"squeue -u {os.environ['USER']} -o %i")
    return (jobid in squeue_out)
