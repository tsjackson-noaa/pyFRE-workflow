"""Transliteration of utility subroutines in FRE/bin/frepp.pl.
"""
import os
import sys
import collections
import itertools
import re
import tempfile
from textwrap import dedent
import time

import pyFRE.util as util
from pyFRE.lib import FREUtil
from . import logs, epmt
_template = util.pl_template # abbreviate

import logging
_log = logging.getLogger(__name__)

grepAssocFiles = """grep ':associated_files' | cut -d '"' -f2 | sed "s/\w*://g" | tr ' ' '\n' | sort -u"""
grep_netcdf_compression = """grep '_DeflateLevel' | cut -d '=' -f2 | sed s/\\;// | sort -r | head -n 1"""
grep_netcdf_shuffle = """grep '_Shuffle' | cut -d '=' -f2 | sed s/\\;// | sed s/\\"//g | sort -r | head -n 1"""

def writescript(script, outscript, batchCmd, statefile, pp):
    """Write c-shell runscript, chmod, and optionally submit."""
    # frepp.pl l.7824
    directory, filename = os.path.split(outscript)
    if not os.exists(directory):
        try:
            os.makedirs(directory)
        except Exception:
            _log.fatal(f"Cannot make directory {directory}")
            sys.exit(1)

    if pp.opt['epmt']:
        script = re.sub(r'(#SBATCH --comment)=?(.*)', r'\1=\2,epmt')

    with open(outscript, 'w') as f:
        f.write(script)

    if pp.opt('epmt'):
	    epmt.epmt_transform(outscript)

    try:
        os.chmod(outscript, 0o775)
    except Exception:
        _log.fatal(f"Sorry, I couldn't chmod {outscript}")
        sys.exit(1)

    if pp.opt['s']:
        batchCmd = "sleep 2; " + batchCmd
        _log.debug(f"Executing '{batchCmd} {outscript}'")
        batch_submit_output = util.shell(f"{batchCmd} {outscript}", log=_log).split('\n')
        newjobid = None
        if batch_submit_output:
            newjobid = re.match(r'Submitted batch job (\d+)', batch_submit_output[-1])
            if newjobid:
                newjobid = newjobid.group(1)
        #print "New jobid='$newjobid'\n";
        if not newjobid and not batch_submit_output:
            _log.error((f"No jobid resulted from the job submission of {pp.hDate}."
                "ERROR: Unable to submit job, exiting."))
            sys.exit(1)
        elif not newjobid:
            _log.error("the jobid returned has the wrong format: a frepp or batch system issue occurred.")
            sys.exit(1)
        if statefile:
            with open(statefile, 'w') as f:
                f.write(f"{newjobid}\n")
        else:
            _log.info(f"TO SUBMIT: {batchCmd} {outscript}")


def call_frepp(abs_xml_path, outscript, component, year, depjobs, pp):
    """Set up to postprocess the following year if necessary."""
    # frepp.pl l. 2971
    csh = ""
    nextyear = FREUtil.modifydate(pp.tEND, "+ 1 sec")
    nextyearf = FREUtil.graindate(nextyear, "day")

    if depjobs: # XXX never used?
        depjobs = f"-w {depjobs.strip()} "

    if year:
        year += userstartmo
        csh  += f"\n/usr/bin/env perl {pp.absfrepp} -x {abs_xml_path} -t {year} -s -q {depjobs}"
        if pp.opt['P']:
            csh += f"--platform {pp.opt['P']} "
        if pp.opt['T']:
            csh += f"-T {pp.opt['T']} "
        if pp.opt['d']:
            csh += f"-d {pp.opt['d']} "
        if pp.opt['D']:
            csh += f"-D {pp.opt['D']} "
        if pp.opt['u']:
            csh += f"-u {pp.opt['u']} "
        if pp.opt['c']:
            csh += f"-c {pp.opt['c']} "
        if pp.opt['m']:
            csh += f"-m '{pp.opt['m']}' "
        if pp.opt['M']:
            csh += "-M "
        if pp.opt['H']:
            csh += "-H "
        if pp.opt['o']:
            csh += "-o "
        if pp.opt['compress']:
            csh += "--compress "
        csh += f"{exp.expt}\n"
    else:
        # call frepp -A for analysis scripts unless we're writing a refineDiag script
        if not pp.opt['D']:
            csh += f"\n/usr/bin/env perl {pp.absfrepp} -A -x {abs_xml_path} -t {pp.hDate} -s -v ";
            if pp.opt['P']:
                csh += f"--platform {pp.opt['P']} "
            if pp.opt['T']:
                csh += f"-T {pp.opt['T']} "
            if pp.opt['d']:
                csh += f"-d {pp.opt['d']} "
            if pp.opt['u']:
                csh += f"-u {pp.opt['u']} "
            if pp.opt['c']:
                csh += f"-c {pp.opt['c']} "
            if pp.opt['m']:
                csh += f"-m '{pp.opt['m']}' "
            if pp.opt['M']:
                csh += "-M "
            if pp.opt['H']:
                csh += "-H "
            csh += f"{exp.expt}\n"
            csh += logs.errorstr((f"{pp.relfrepp} had a problem creating final "
                f"analysis script {exp.expt}_{nextyearf}"))

        if pp.opt['plus']:
            csh += ('\n' + form_frepp_call_for_plus_option(component, pp) + '\n')
            csh += logs.errorstr((f"{pp.relfrepp} had a problem creating next "
                f"analysis script {exp.expt}_{nextyearf}"))
        csh += logs.mailerrors(outscript);
    return csh

def form_frepp_call_for_plus_option(comp, pp):
    """Generate the frepp command for next year's frepp call when using the
    --plus option."""
    # frepp.pl l.3032
    nextyear  = FREUtil.modifydate(pp.tEND, "+ 1 sec")
    nextyearf = FREUtil.graindate(nextyear, "day")
    togo      = int(pp.opt['plus']) - 1
    if togo > 0:
        plus = f"--plus {togo}"
    else:
        plus = ""
    cmd = f"/usr/bin/env perl {pp.absfrepp} -x {pp.abs_xml_path} -t {nextyearf} {plus} -s -v "
    if pp.opt['P']:
        cmd += f"--platform {pp.opt['P']} "
    if pp.opt['T']:
        cmd += f"-T {pp.opt['T']} "
    if pp.opt['d']:
        cmd += f"-d {pp.opt['d']} "
    if pp.opt['D']:
        cmd += f"-D {pp.opt['D']} "
    if pp.opt['u']:
        cmd += f"-u {pp.opt['u']} "
    if pp.opt['c']:
        cmd += f"-c {comp} "
    if pp.opt['m']:
        cmd += f"-m '{pp.opt['m']}' "
    if pp.opt['M']:
        cmd += "-M "
    if pp.opt['H']:
        cmd += "-H "
    if pp.opt['o']:
        cmd += "-o "
    if pp.opt['compress']:
        cmd += "--compress "
    cmd += f"{exp.expt}\n"
    return cmd

def execute(host, command):
    """Execute a command on the workstation that can write to archive."""
    platform = util.shell('/home/gfdl/bin/gfdl_platform', log=_log)
    if platform == 'desktop':
        qloginHome = '/home/gfdl/qlogin'
        return util.shell(f"{qloginHome}/bin/hpcs_ssh_init; {qloginHome}/bin/hpcs_ssh '{host}' '{command}'", log=_log)
    else:
        return util.shell(command, log=_log)

def createcpio(cache, outdir, prefix, abbrev, dmputOnly, pp):
    """Create a cpio and dmput original files.  Also dmput only when a cpio is
    not created."""
    # frepp.pl l.3819
    if dmputOnly:
        return _template("""
            cd $outdir
            $time_dmput dmput "$prefix.*.nc"
            cd \$work
            EOF

        """, locals())
    else:
        return _template("""
            cd $cache
            set numfilestocpio = `ls $prefix.*.nc | wc -l`
            if ( \$numfilestocpio > 0 ) then

                if ( ! -e $outdir/$prefix.$abbrev.nc.cpio ) then
                    ls -1 $prefix.*.nc | $time_mkcpio $cpio -oKvO \$work/$prefix.$abbrev.nc.cpio
                    $time_mv $mvfile \$work/$prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                    if ( \$status ) then
                        echo "WARNING: data transfer failure, retrying..."
                        $time_mv $mvfile \$work/$prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                        $checktransfer
                    endif
                    $time_rm rm \$work/$prefix.$abbrev.nc.cpio
                else
                    $time_dmget dmget $outdir/$prefix.$abbrev.nc.cpio
                    $time_cp $cp $outdir/$prefix.$abbrev.nc.cpio .
                    if ( \$status ) then
                        echo "WARNING: data transfer failure, retrying..."
                        $time_cp $cp $outdir/$prefix.$abbrev.nc.cpio .
                        $checktransfer
                    endif
                    set files = ( `ls $prefix.*.nc` )
                    set filesincpio = ( `$cpio -itI $prefix.$abbrev.nc.cpio` )
                    set exist = ()
                    foreach file ( \$files )
                        foreach fileincpio ( \$filesincpio )
                            if ( "\$file" == "\$fileincpio" ) then
                                set exist = ( \$exist \$file )
                            endif
                        end
                    end
                    if ( \$#exist == 0 ) then
                        ls -1 $prefix.*.nc | $time_mkcpio $cpio -oKvAO $prefix.$abbrev.nc.cpio
                        $time_mv $mvfile $prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                        if ( \$status ) then
                            echo "WARNING: data transfer failure, retrying..."
                            $time_mv $mvfile $prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                            $checktransfer
                        endif
                        $time_rm rm $prefix.$abbrev.nc.cpio
                    else
                        mkdir \$work/mkcpio
                        cd \$work/mkcpio
                        $time_uncpio $cpio -ivI $outdir/$prefix.$abbrev.nc.cpio
                        cd $outdir
                        #this is a local cp within vftmp
                        $time_cp cp \$files \$work/mkcpio
                        cd \$work/mkcpio
                        ls -1 $prefix.*.nc | $time_mkcpio $cpio -oKvO $prefix.$abbrev.nc.cpio
                        $time_mv $mvfile $prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                        if ( \$status ) then
                            echo "WARNING: data transfer failure, retrying..."
                            $time_mv $mvfile $prefix.$abbrev.nc.cpio $outdir/$prefix.$abbrev.nc.cpio
                            $checktransfer
                        endif
                        cd \$work
                        $time_rm rm -rf \$work/mkcpio
                    endif
                endif

                cd $outdir
                $time_dmput dmput "$prefix.*.nc"
                $time_dmput dmput  $prefix.$abbrev.nc.cpio

            endif
            cd \$work

        """, locals(), pp)

def diagfile(ppcNode, freq, src, component):
    """Get the name of the diagnostic output file from the source attribute."""
    diag_source = ""
    # frepp.pl l.3948
    #STATIC
    #TIMESERIES - ANNUAL or SEASONAL
    if src in ("annual", "seasonal"):
        monthnodes = ppcNode.findnodes('timeSeries[@freq="monthly"]')
        if monthnodes:
            monthnode = ppcNode.findnodes('timeSeries[@freq="monthly"]')->get_node(1);
            diag_source = monthnode.getAttribute('@source')
    else:
        #TIMESERIES - from smaller timeSeries
        nodes = ppcNode.findnodes(f"timeSeries[\@freq='{freq}']")
        if nodes:
            node = ppcNode.findnodes(f"timeSeries[\@freq='{freq}']")->get_node(1);
            diag_source = node.getAttribute('@source')
    if not diag_source:
        diag_source = ppcNode.findvalue('@source')
    if not diag_source:
        diag_source = f"{component}_month"
    return diag_source

def refineDiag(tmphistdir, stdoutdir, ptmpDir, basedate, refinedir, gridspec, mdbi):
    """hsmget history data into work area, provide directory for user to put new
    history files, run user scripts, package the new data into new history file.
    """
    # frepp.pl l.7727
    newhistorydir = f"{pp.opt['d']}_refineDiag"
    csh = _template("""

        set refineRequestsExit = 0
        set refineError = 0
        #make additional data available to the refineDiag user scripts
        set basedate = '$basedate'
        set gridspec = '$gridspec'

        set historyyear = `echo $hDate | sed 's/[0-9][0-9][0-9][0-9]\$//'`
        cd \$histDir
        foreach historyfile ( `ls \$historyyear????.nc.cpio \$historyyear????.nc.tar`)
            set hsmdate = \$historyfile:r

            #set up refineDiag input data and output directory
            set refineDiagDir = "$tmphistdir/history_refineDiag/\$hsmdate"
            mkdir -p \$refineDiagDir

            $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w \$work \$hsmdate/\\*
            if ( \$status ) then
                echo "WARNING: hsmget reported failure, retrying..."
                $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w \$work \$hsmdate/\\*
                $checktransfer
            endif
            cd \$work/\$hsmdate
    """, locals(), exp)
    refineScripts = pp.opt['D'].split(',')
    for s in refineScripts:
        if not os.exists(s):
            _log.error(f"Your refineDiag script does not exist: {s}")
        csh += _template("""
            #source user refineDiag script: $s
            source $s
            set refineStatus = \$status
            if ( \$refineStatus < 0 ) then
                echo "NOTE: RefineDiag script $s requested no further processing after refineDiag scripts complete"
                set refineRequestsExit = 1
            else if ( \$refineStatus ) then
                echo "ERROR: RefineDiag script got an error status \$refineStatus"
                set refineError = 1
            endif
        """)
    csh += _template("""
            #append the new variables to the refineDiag.log file
            cd \$refineDiagDir
            set refinedCount = `ls -1 *nc | wc -l`
            if ( \$refinedCount ) then

                foreach refinedFile ( `ls *nc` )
                    echo "$refinedir/\$hsmdate.tar,\$refinedFile" >> $stdoutdir/postProcess/refineDiag.log
                    \$NCVARS -st01234 \$refinedFile >> $stdoutdir/postProcess/refineDiag.log
                end

                #save new or modified refineDiag history file
                if ( -f $newhistorydir/\$hsmdate.tar ) then
                    $time_hsmget \$hsmget -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/modify_refineDiag \$hsmdate/\\*
                    if ( \$status ) then
                        echo "WARNING: hsmget reported failure, retrying..."
                        $time_hsmget \$hsmget -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/modify_refineDiag \$hsmdate/\\*
                        $checktransfer
                    endif
                    mv -f * $tmphistdir/modify_refineDiag/\$hsmdate/
                    mv -f $tmphistdir/modify_refineDiag/\$hsmdate/* .
                    rm -rf $tmphistdir/modify_refineDiag
                    $time_hsmput \$hsmput -s tar -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/history_refineDiag \$hsmdate
                    if ( \$status ) then
                        echo "WARNING: hsmput reported failure, retrying..."
                        $time_hsmput \$hsmput -s tar -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/history_refineDiag \$hsmdate
                        $checktransfer
                    endif
                else
                    $time_hsmput \$hsmput -s tar -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/history_refineDiag \$hsmdate
                    if ( \$status ) then
                        echo "WARNING: hsmput reported failure, retrying..."
                        $time_hsmput \$hsmput -s tar -a $newhistorydir -p $ptmpDir/history_refineDiag -w $tmphistdir/history_refineDiag \$hsmdate
                        $checktransfer
                    endif
                endif
            endif
        end

        if ( \$refineError ) exit \$refineError
        if ( \$refineRequestsExit ) exit 0

    """)
    return csh

def getTemplate(platform, workdir):
    """"""
    # frepp.pl l.7875
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



# ------------------------------------------------------------------------------

def jpkSrcFiles(node):
    # frepp.pl l.2462
    jpkElem = node.getName
    if jpkElem == "timeAverage":
        jpkSrc = node.findvalue('../@source')
        if not jpkSrc:
            _log.warning(f"no diagnostic source file specified for {node.toString()}")
            return jpkSrc
    else:
        jpkSrc = node.findvalue('@source')
        if not jpkSrc:
            jpkSrc = node.findvalue('../@source')
        if not jpkSrc:
            _log.warning(f"no diagnostic source file specified for {node.toString()}")
            return jpkSrc

def jpk_hsmget_files(hsmfiles):
    # frepp.pl l.2483
    # hsmfiles: str
    hsmf = list(set(hsmfiles.split(',')))
    _log.debug(f"diagnostic files to be extracted: {hsmf}")
    return hsmf

def dmget_files(historyfiles):
    """Sort, uniq list of history files to dmget."""
    #frepp.pl l.2496
    # historyfiles: str
    hf = list(set(historyfiles.split(','))).sort()
    _log.debug(f"historyfiles used: {hf}")
    return hf

def hsmget_history_csh(ptmpDir, tmphistdir, refinedir, this_frepp_cmd, hf, hsmf):
    """Return the csh to extract history files."""
    # frepp.pl l.2510

    #this keeps users to 1,2,3,4,6,12 mo segments, but if <1yr, it should be allowed.
    #my @reqStartMonths = segStartMonths($segTime,$segUnits);
    #my $histPerYear = scalar(@reqStartMonths);
    beginCombineTime = util.unix_epoch()

    #check for and combine raw history files online
    histfiles = hf.split(' ')

    #print "histfiles:".join(", ",@histfiles)."\n";
    #print "hsmf $hsmf\n";

    # Retrieve hidden debug mppnccombine flag if available or use default combiner flags
    mppnccombineOptsDefault = '-64 -h 16384 -m'
    mppnccombineOptString   = mppnccombineOptsDefault
    if pp.opt.get('mppnccombine-opts', False):
        mppnccombineOptString = pp.opt['mppnccombine-opts']

    for h in histfiles:
        os.chdir(pp.opt['d'])
        year = re.match(r'(\d{4,})\d{4}\.', h)
        if year:
            year = int(year.group(1))

        #print "\nyear:$year\n";
        availraw = util.shell(f'ls -1 | egrep "{year}....\.raw\.nc\.cpio$|{year}....\.raw\.nc\.tar$"').split('\n')

        #print "availraw:".join(", ", @availraw)."\n";
        availhf = util.shell(f'ls -1 | egrep "{year}....\.nc\.cpio$|{year}....\.nc\.tar$"').split('\n')

        #print "availhf:".join(", ", @availhf)."\n";

        #if both raw and combined data exist, and raw is newer, replace. Otherwise exit on error.
        if availraw and availhf:
            count = collections.defaultdict(int)
            modavailraw = [s.replace('.raw.nc', '.nc') for s in availraw]

            #print "modavailraw:".join(", ", @modavailraw)."\n";
            for file_ in itertools.chain(modavailraw, availhf):
                count[file_] += 1
            for file_ in count:
                if count[file_] > 1:
                    combinedate = os.path.getmtime(file_)
                    rawfile = file_.replace('.nc.', '.raw.nc.')
                    rawdate = os.path.getmtime(rawfile)

                    #print "combined=$combinedate raw=$rawdate\n";
                    if combinedate > rawdate:
                        logs.mailuser(("Date of uncombined (raw) history file is older "
                            f"than date of combined history file for {file_}. This may "
                            "indicate a problem; please remove the incorrect history file "
                            "and relaunch frepp with the following command, using '-s' to "
                            f"submit:\n\n{exp.this_frepp_cmd}"))
                        logs.sysmailuser()
                        _log.error(("Date of uncombined (raw) history file is older than "
                            f"date of combined history file for {file_}. This may indicate "
                            "a problem; please remove the incorrect history file and relaunch frepp."))
                        sys.exit(1)
        elif not availraw and not availhf:
                logs.mailuser((f"No history data found for year {year} in {pp.opt['d']}\n\n"
                    "To resubmit this frepp job when the data is available, run the following "
                    f"command with '-s' to submit:\n\n{exp.this_frepp_cmd}"))
                logs.sysmailuser()
                _log.error(f"No history data found for year {year} in {pp.opt['d']}")

        #combine raw data
        jobid = os.environ.get('SLURM_JOBID', False)
        wallTime = 0
        if jobid:
            try:
                wallTime = util.shell(f'os.environ["FRE_COMMANDS_HOME"]/sbin/batch.scheduler.time -t {jobid}')
            except Exception:
                _log.error("Could not obtain wallclock time")
                sys.exit(1)
            _log.info(f"Requested walltime is {wallTime} sec")

        for file_ in availraw:
            histdate = re.sub(r'(\d+)\..*', r'\1', file_)
            histdate += '.nc'
            # File::Temp->newdir( 'combinehistXXXXX', DIR => $ENV{TMPDIR}, CLEANUP => 1 );
            combinedir = tempfile.TemporaryDirectory(prefix='combinehistXXXXX', dir=os.environ['TMPDIR'])

            #my $combinedir = File::Temp->tempdir( 'combinehistXXXXX', DIR => $ENV{TMPDIR});
            os.makedirs(f"{combinedir}/{histdate}")
            os.chdir(f"{combinedir}/{histdate}")
            _log.info(f"Copying to {combinedir}, combining, and archiving file {file_}")

            #set initial timing stats
            segmentStart = util.unix_epoch()
            combine = _template("""
                $time_cp $cp $opt_d/$file .
                MYSTATUS=\$?
                if [ \$MYSTATUS -ne 0 ]; then
                    echo WARNING: copy filed for raw history file $file, retrying.
                    $time_cp $cp $opt_d/$file .
                    MYSTATUS=\$?
                    if [ \$MYSTATUS -ne 0 ]; then
                        echo ERROR: copy failed twice for raw history file $file, exiting.
                        exit 7
                    fi
                fi

                $time_untar tar -xf $file
                MYSTATUS=\$?
                if [ \$MYSTATUS -ne 0 ]; then
                    echo ERROR: history tar extraction failed for $file, exiting.
                    exit 1
                fi
                $time_rm rm $file

                export mppnccombineOptString='$mppnccombineOptString'

                $time_combine $ENV{FRE_COMMANDS_HOME}/site/$ENV{FRE_SYSTEM_SITE}/bin/combinehist
                MYSTATUS=\$?
                if [ \$MYSTATUS -ne 0 ]; then
                    echo ERROR: combining history files failed for $file, exiting.
                    exit 1
                fi

                $time_hsmput hsmput -v -t -s tar -a $opt_d -p $ptmpDir/history -w $combinedir $histdate
                MYSTATUS=\$?
                if [ \$MYSTATUS -eq 0 ]; then
                    if [ -f $opt_d/$file ] && [ -f $opt_d/$histdate.tar ]; then
                        $time_rm rm $opt_d/$file
                    fi
                fi

            """, locals(), pp)
            try:
                util.shell(combine, log=_log)
            except Exception:
                _log.error('Could not combine history data')
                sys.exit(1)

            #calculate timings, resubmit if necessary
            segmentEnd = util.unix_epoch()
            timeSoFar = segmentEnd - beginCombineTime
            _log.info(f"Finished a segment, timeSoFar = {timeSoFar}")
            segmentTime = segmentEnd - segmentStart

            #if not enough time for 2 x segmentTime, resubmit
            segmentTime = 2 * segmentTime
            if wallTime > 0:
                timeRemaining = wallTime - timeSoFar
                if segmentTime > timeRemaining:
                    _log.info(f"Only {timeRemaining} seconds left, resubmitting.")
                    time.sleep(2)
                    sys.exit(99)

    #gets all avail hist files
    hsmget_history = _template("""
        cd $opt_d
        foreach h ( $hf )
            set historyyear = `echo \$h | sed 's/[0-9][0-9][0-9][0-9].nc.tar//'`
            set availhf = ( `ls \$historyyear????.nc.cpio \$historyyear????.nc.tar` )
            if ( "\$availhf" == "" ) then
                Mail -s "\$name year \$historyyear cannot be postprocessed" $mailList <<END
                    Your FRE post-processing job ( \$JOB_ID ) has exited because no history files
                    were found for year \$historyyear in directory:
                    $opt_d

                    FRE will attempt to transfer the history files on the remote side by retrying
                    the failed output stager transfers.  If later postprocessing jobs require
                    this postprocessing interval, this year of postprocessing will be rerun.

                    To recover manually, please transfer the history data with gcp,
                    and then resubmit this postprocessing job via:

                    $this_frepp_cmd

                    Job details:
                    \$name running on \$HOST
                    Batch job stdout:
                    \$FRE_STDOUT_PATH
                END

                echo HISTORYDATAERROR > \$statefile
                sleep 30
                exit 6
            endif

            foreach historyfile ( \$availhf )
                foreach hsmsrc ( $hsmf )
                    set hsmdate = \$historyfile:r
                    $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$hsmdate/\\*.\$hsmsrc.\\*
                    if ( \$status ) then
                        echo "WARNING: hsmget reported failure, retrying..."
                        $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$hsmdate/\\*.\$hsmsrc.\\*
                        $checktransfer
                    endif
                    # Set original history compression variables to restore before placing in archive.
                    # (Have to use the ptmp version as the vftmp version may already be uncompressed
                    # from a previous run attempt.)
                    foreach ptmpfile ( `ls $ptmpDir/history/\$hsmdate/*.\$hsmsrc.*` )
                        if (! \$?history_deflation) then
                            set -r history_deflation = `ncdump -sh \$ptmpfile | $grep_netcdf_compression`
                            set -r history_shuffle = `ncdump -sh \$ptmpfile | $grep_netcdf_shuffle`
                        endif
                    end
                    # Get files listed as associated_files
                    foreach hsmsrcfile ( `ls $tmphistdir/\$hsmdate/*.\$hsmsrc.*` )
                        # Get a list of all associated files
                        set assocFiles = `ncdump -h \$hsmsrcfile | $grepAssocFiles`
                        foreach assocFile ( \$assocFiles )
                            $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$hsmdate/\\*\${assocFile:r}.\\*
                        end
                    end
                end
            end
            #end loop over history year
        end

        # Set nccopy netcdf compression flags
        if ($opt{compress}) then
            set -r nc_compression_flags = "-d 2 -s"
        else if (\$?history_deflation) then
            if (\$history_deflation) then
                if (\$history_shuffle == "true") then
                    set -r nc_compression_flags = "-d \$history_deflation -s"
                else
                    set -r nc_compression_flags = "-d \$history_deflation"
                endif
            else
                set -r nc_compression_flags = ""
            endif
        else
            set -r nc_compression_flags = ""
        endif

        if ( -d $refinedir ) then
            cd $refinedir
            foreach h ( $hf )
                foreach hsmsrc ( $hsmf )
                    set historyyear = `echo \$h | sed 's/[0-9][0-9][0-9][0-9].nc.tar//'`
                    foreach historyfile ( `ls \$historyyear????.nc.cpio \$historyyear????.nc.tar`)
                        set hsmdate = \$historyfile:r
                        $time_hsmget \$hsmget -a $refinedir -p $ptmpDir/history_refineDiag -w $tmphistdir \$hsmdate/\\*.\$hsmsrc.\\*
                        if ( \$status ) then
                            echo "WARNING: hsmget reported failure, retrying..."
                            $time_hsmget \$hsmget -a $refinedir -p $ptmpDir/history_refineDiag -w $tmphistdir \$hsmdate/\\*.\$hsmsrc.\\*
                            $checktransfer
                        endif
                        # Get files listed as associated_files
                        foreach hsmsrcfile ( `ls $tmphistdir/\$hsmdate/*.\$hsmsrc.*` )
                            # Get a list of all associated files
                            set assocFiles = `ncdump -h \$hsmsrcfile | $grepAssocFiles`
                            foreach assocFile ( \$assocFiles )
                                $time_hsmget \$hsmget -a $refinedir -p $ptmpDir/history_refineDiag -w $tmphistdir \$hsmdate/\\*\${assocFile:r}.\\*
                                $time_hsmget \$hsmget -a $opt_d -p $ptmpDir/history -w $tmphistdir \$hsmdate/\\*\${assocFile:r}.\\*
                            end
                        end
                    end
                end
            end
        endif
    """, locals(), pp, grep_netcdf_compression=grep_netcdf_compression,
        grep_netcdf_shuffle=grep_netcdf_shuffle)
    return hsmget_history

def uncompress_history_csh(dir_):
    """Uncompress netcdf compression."""
    # frepp.pl l.2792
    check_nccopy = logs.errorstr("NCCOPY (uncompress history files)")
    return _template("""
        # uncompress all netcdf-compressed files
        if (\$?history_deflation) then
            foreach file (`find $dir -type f -name "*.nc"`)
                if (`ncdump -sh \$file | $grep_netcdf_compression`) then
                    $time_nccopy nccopy -d 0 \$file \$file.uncompressed
                    $check_nccopy
                    mv -f \$file.uncompressed \$file
                    chmod 444 \$file
                endif
            end
        endif
    """, locals(), dir=dir_, grep_netcdf_compression=grep_netcdf_compression)

def compress_csh(file_, check_nccopy):
    """Compress pp files before placing in archive."""
    # frepp.pl l.2812
    return _template("""
        if ("\$nc_compression_flags" != "") then
            $time_nccopy nccopy \$nc_compression_flags $file $file.compressed
            $check_nccopy
            mv $file.compressed $file
        endif
    """, locals(), file=file_)

def checkHistComplete(dir_, hf, frepp_cmd, usedfiles, diagtablecontent):
    """Check for complete history data."""
    # frepp.pl l.2825
    script = f"cd {dir_}\n";

    firsthisty = re.match(r'(\d{4,})\d{4}\.', hf)
    if firsthisty:
        firsthisty = firsthisty.group(1)
    firsthistm = str(userstartmo)[0:2]
    firsthistd = str(userstartmo)[2:4]
    m = re.match(r'^(\d{4,})(\d{2})(\d{2})(?:\d{2}:\d{2}:\d{2})?$', tEND)
    if m:
        tENDy, tENDm, tENDd = m.groups()
    else:
        raise ValueError(tEND)

    days_firsthist = FREUtil.daysSince1BC(firsthistm, firsthistd, firsthisty)
    days_tEND = FREUtil.daysSince1BC(tENDm, tENDd, tENDy)

    firsthist = FREUtil.parseDate(firsthisty + userstartmo)
    delta = FREUtil.dateCalc(firsthist, FREUtil.modifydate(tEND, "+1 sec"))

    for dt in diagtablecontent:
        m = re.match(r'"(\w*)"\s*,\s*(\d*)\s*,\s*"(\w*)"\s*,.*,.*,.*,?', dt)
        # if ( /"(\w*)"\s*,\s*(\d*)\s*,\s*"(\w*)"\s*,.*,.*,.*,?/ and not /^#/ ) {
        if m:
            diagfile, freq, units = m.groups()
            freq = int(freq)

            if diagfile not in usedfiles:
                continue

            #get tEND-firsthist in hours, months, (years, days)
            efields  = 0
            efields2 = 0
            if units == "months":
                efields = FREUtil.Delta_Format(delta, 0, "%Mt")
                efields = efields // freq
                fields2 = efields
            elif units == "years":
                efields = FREUtil.Delta_Format(delta, 0, "%yt")
                efields = efields // freq
                efields2 = efields
            elif units == "days":
                if caltype == "julian":
                    efields  = days_tEND - days_firsthist + 1
                    efields2 = days_tEND - days_firsthist + 2
                    efields = efields // freq
                    efields2 = efields2 // freq
            elif units == "hours":
                if caltype == "julian":
                    efields  = ( days_tEND - days_firsthist + 1 ) * 24
                    efields2 = ( days_tEND - days_firsthist + 2 ) * 24
                    efields = efields // freq
                    efields2 = efields2 // freq

            if efields == 0:
                continue
            _log.debug((f"Will check that {diagfile} ({freq} {units}) history data has "
                f"{efields} time levels ({firsthisty}{firsthistm}{firsthistd}-{tENDy}{tENDm}{tENDd})"))

            script += _template("""

                echo NOTE: Check $diagfile time levels: compare expected and actual fields
                set afields = 0
                foreach file (`ls -1 */*$diagfile\.*nc | egrep -v '\.tile[2-6]\.nc'`)
                    set nf = `ncdump -h \$file | grep UNLIMITED | sed 's/.*(//;s/ .*//'`
                    @ afields = \$afields + \$nf
                end
                if ( \$afields == $efields ) then
                    echo NOTE: History data has the expected number of time levels for $diagfile
                else if ( \$afields == $efields2 ) then
                    echo NOTE: History data has the expected number of time levels for $diagfile
                else
                    echo ERROR: Incomplete history data
                    Mail -s "\$name year \$historyyear cannot be postprocessed" $mailList <<END
                        Your FRE post-processing job ( \$JOB_ID ) has exited because of incomplete
                        history data.  FRE expected $efields time levels in $diagfile data, but
                        found \$afields time levels for the interval $firsthisty$firsthistm$firsthistd-$tENDy$tENDm$tENDd.

                        FRE will attempt to transfer the history files on the remote side by retrying
                        the failed output stager transfers.  If later postprocessing jobs require
                        this postprocessing interval, this year of postprocessing will be rerun.

                        To recover manually, please transfer the history data with gcp,
                        and then resubmit this postprocessing job via:

                        $frepp_cmd

                        Job details:
                        \$name running on \$HOST
                        Batch job stdout:
                        \$FRE_STDOUT_PATH
                    END
                    echo HISTORYDATAERROR > \$statefile
                    sleep 30
                    exit 7

                endif
            """, locals())
    return script


def createdirs(mkdircommand):
    """Make directories."""
    # frepp.pl l.2927
    if mkdircommand and mkdircommand != "mkdir -p ":
        #if ( $opt_v ) { print "length of mkdircommand: ".length("$mkdircommand")."\n"; }
        if len(mkdircommand) > 5000:
            mkdircommand = re.sub(r"^mkdir -p ", r"", mkdircommand)
            i = mkdircommand.index(' ', 5000)
            f = mkdircommand[:i]
            s = mkdircommand[i:]
            #if($opt_v){print "executing mkdircommand:\nfirst half:\n'$f'\n\nsecond half:\n'$s'\n";}
            execute('ac-arch', f"mkdir -p {f}")
            execute('ac-arch', f"mkdir -p {s}")
        else:
            #if ( $opt_v ) { print "executing mkdircommand: $mkdircommand\n"; }
            execute('ac-arch', mkdircommand)
        mkdircommand = "mkdir -p "
    return mkdircommand

