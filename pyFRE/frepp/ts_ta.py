"""Transliteration of time series/time average subroutines in FRE/bin/frepp.pl.
"""
import sys
from textwrap import dedent

from pyFRE.lib import FREUtil
import pyFRE.util as util
from . import logs

import logging
_log = logging.getLogger(__name__)

_template = util.pl_template # abbreviate

def zInterpolate(zInterp, infile, outfile, caltype, variables, source, pp):
    """Set up interpolation on z levels."""
    csh = ""

    #set up atmospheric pressure levels
    ncep_levels = dedent("""
        echo 'Using zInterp ncep'
        set levels = (100000 92500 85000 70000 60000 50000 40000 30000 25000 20000 15000 \\
                    10000  7000  5000  3000  2000  1000                               )
    """)
    am3_levels = dedent("""
        echo 'Using zInterp am3'
        set levels = (100000 92500 85000 70000 60000 50000 40000 30000 25000 20000 15000 \\
                      10000  7000  5000  3000  2000  1000   500   300   200   100       )
    """)
    hs20_levels = dedent("""
        echo 'Using zInterp hs20'
        set levels = ( 2500  7500 12500 17500 22500 27500 32500 37500 42500 47500 \\
                       52500 57500 62500 67500 72500 77500 82500 87500 92500 97500 )
    """)
    era40_levels = dedent("""
        echo 'Using zInterp era40'
        set levels = (100000 92500 85000 77500 70000 60000 50000 40000 30000 25000 20000 15000 \\
                        10000  7000  5000  3000  2000  1000   700   500   300   200   100       )
    """)
    narcaap_levels = dedent("""
        echo 'Using zInterp narcaap'
        set levels = ( 2500  5000  7500  10000  12500  15000 17500 20000 22500 25000 27500 30000 \\
                      32500 35000 37500  40000  42500  45000 47500 50000 52500 55000 57500 60000 \\
                      62500 65000 67500  70000  72500  75000 77500 80000 82500 85000 87500 90000 \\
                      92500 95000 97500 100000 102500 105000 )
    """)
    ar5daily_levels = dedent("""
        echo 'Using zInterp ar5daily'
        set levels = (100000 85000 70000 50000 25000 10000 5000 1000)
    """)
    ncepSubset_levels = dedent("""
        echo 'Using zInterp ncep_subset'
        set levels = ( 925 850 700 500 250 )
    """)

    if zInterp == "ncep": set_plevels = ncep_levels
    elif zInterp == "am3": set_plevels = am3_levels
    elif zInterp == "hs20": set_plevels = hs20_levels
    elif zInterp == "era40": set_plevels = era40_levels
    elif zInterp == "narcaap": set_plevels = narcaap_levels
    elif zInterp == "ar5daily": set_plevels = ar5daily_levels
    elif zInterp == "ncep_subset": set_plevels = ncepSubset_levels
    elif zInterp == "zgrid": pass
    else:
        if zInterp:
            logs.mailuser(f"zInterp {zInterp} not recognized, not interpolating {outfile}")
            _log.error(f"zInterp {zInterp} not recognized, not interpolating {outfile}")
    check_plevel  = logs.errorstr(f"PLEVEL ({outfile})")
    check_ncdump  = logs.errorstr(f"NCDUMP ({outfile})")
    check_ncks    = logs.errorstr(f"NCKS ({outfile})")
    check_zgrid   = logs.errorstr(f"ZGRID (Calling Resample_on_Z for {outfile})")
    check_ncatted = logs.errorstr(f"NCATTED ({outfile})")
    plev_command  = '$PLEVEL -a'

    if variables:
        variables = variables.replace(',', ' ')
        variables = variables.replace("'", '')

        plev_command  = '$PLEVEL'
        if pp.opt['v']:
            count = len(variables.split())
            _log.info(f"will interpolate {count} variables to pressure levels for {infile}")

    if zInterp in ("ncep", "am3", "narcaap", "hs20", "era40", "ar5daily", "ncep_subset"):
        csh += _template("""
            $set_plevels

            set reqvars = `\$NCVARS -st12 $infile | grep -e '^ *bk\$' -e '^ *pk\$' -e '^ *ps\$' -c`
            set hgtvars = `\$NCVARS -st23 $infile | grep -e '^ *temp\$' -e '^ *sphum\$' -e '^ *zsurf\$' -c`

            set vars3d  = `\$NCVARS -st3  $infile`

            if ( \$reqvars == 3 && \$#vars3d > 0 ) then
                set vlist = ()
                if ( \$hgtvars == 3 ) then
                    set vlist = (divv rvort hght slp)
                endif
                $time_plevel $plev_command -p "\$levels" -i $infile -o plev.nc \$vlist $variables
                $check_plevel
                set string = `ncdump -h plev.nc | grep UNLIMITED`
                $check_ncdump
                set timename   = `echo \$string[1]`
                set string = `ncdump -h plev.nc | grep calendar_type`
                set caltype   = `echo \$string[3] | sed 's/"//g'`
                if ( "\$caltype" == "" ) set caltype = $caltype
                $time_ncatted ncatted -h -O -a calendar,\$timename,c,c,\$caltype plev.nc
                $check_ncatted
                $time_mv $mv plev.nc $outfile
                $time_rm rm -f $infile
            else if ( \$reqvars < 3 && \$#vars3d > 0 ) then
                echo ERROR: zInterp requested for $source, but missing one or more required variables
                exit 1
            else
                $time_mv $mv $infile $outfile
            endif
        """, locals(), pp)
    elif zInterp == "zgrid": # ocean
        if not variables:
            variable = "temp,salt,age,u,v";
            _log.info(f"No variables specified, resampling {variables} to zgrid")
        else:
            pass # already cleaned $variables
        if pp.platform == "x86_64":
            csh += _template("""
                set taxis = `ncdump -h $infile | grep -i '.*=.*unlimited.*currently' | awk '{print \$1}'`
                #set hasclimbounds = `ncdump -h $infile | grep 'climatology_bounds' | wc -l`
                if ( `ncdump -h $infile | grep -c " climatology_bounds("` == 1 ) then
                    $time_zgrid /home/rwh/data/regrid_MESO/Resample_on_Z_new -d/home/rwh/data/regrid_MESO/OM3_zgrid.nc -V:$variables -T:average_T1,average_T2,average_DT,climatology_bounds -ee -o$outfile $infile
                    $check_zgrid
                else
                    $time_zgrid /home/rwh/data/regrid_MESO/Resample_on_Z_new -d/home/rwh/data/regrid_MESO/OM3_zgrid.nc -V:$variables -T:average_T1,average_T2,average_DT,\${taxis}_bounds -ee -o$outfile $infile
                    $check_zgrid
                endif
            """, locals(), pp)
    else:
        csh += _template("""
            $time_mv $mv $infile $outfile
        """, locals(), pp)


def segStartMonths(segTime, segUnits):
    """"""
    if (segTime == 1 and segUnits == 'years') or \
        (segTime == 12 and segUnits == 'months'):
        return ['0101']
    elif segTime == 6:
        return ['0101', '0701']
    elif segTime == 4:
        return ['0101', '0501', '0901']
    elif segTime == 3:
        return ['0101', '0401', '0701', '1001']
    elif segTime == 2:
        return ['0101', '0301', '0501', '0701', '0901', '1101']
    elif segTime == 1 and segUnits == 'months':
        return ['0101', '0201', '0301', '0401', '0501', '0601', '0701', '0801',
            '0901', '1001', '1101', '1201']
    else:
        _log.fatal((f"segTime {segTime} not supported for timeAverages. "
            "Try 1,2,3,4,6 or 12 month segments."))
        sys.exit(1)

def convertSegments(segTime, segUnits, diag_source, type, sourceGrid, pp):
    """Make csh for splitting history files into monthly files."""
    if sourceGrid == 'cubedsphere':
        cubicLoopStart = "set i = 1\nwhile ( \$i <= 6 )";
        cubicLoopEnd   = "@ i ++\nend";
        diag_source    = "$diag_source.tile\$i";

    if segTime == 6:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0201.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0301.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0401.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0501.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0601.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0801.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0901.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1001.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1101.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1201.grid_spec.tile\$i.nc
            """)
        convertSeg = _template("""
            $cubicLoopStart
            set string = `ncdump -h \${hDate}0101.$diag_source.nc | grep UNLIMITED`
            set timename   = `echo \$string[1]`
            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0101.$diag_source.nc \${hDate}0201.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0101.$diag_source.nc \${hDate}0301.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0101.$diag_source.nc \${hDate}0401.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,5,5 \${hDate}0101.$diag_source.nc \${hDate}0501.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,6,6 \${hDate}0101.$diag_source.nc \${hDate}0601.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0101.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0701.$diag_source.nc tmp07.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0701.$diag_source.nc \${hDate}0801.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0701.$diag_source.nc \${hDate}0901.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0701.$diag_source.nc \${hDate}1001.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,5,5 \${hDate}0701.$diag_source.nc \${hDate}1101.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,6,6 \${hDate}0701.$diag_source.nc \${hDate}1201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp07.nc \${hDate}0701.$diag_source.nc
            $cubicLinkGridSpec
            $cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            $time_ncks ncks \$ncksopt -d \$timename,6,6 \${prevyear}0701.$diag_source.nc \${prevyear}1201.$diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 2:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0201.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0401.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0601.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0801.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1001.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1201.grid_spec.tile\$i.nc
            """)
        convertSeg = _template("""
            $cubicLoopStart
            set string = `ncdump -h \${hDate}0101.$diag_source.nc | grep UNLIMITED`
            set timename   = `echo \$string[1]`
            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0101.$diag_source.nc \${hDate}0201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0101.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0301.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0301.$diag_source.nc \${hDate}0401.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0301.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0501.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0501.$diag_source.nc \${hDate}0601.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0501.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0701.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0701.$diag_source.nc \${hDate}0801.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0701.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0901.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0901.$diag_source.nc \${hDate}1001.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0901.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}1101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}1101.$diag_source.nc \${hDate}1201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}1101.$diag_source.nc
            $cubicLinkGridSpec
            $cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${prevyear}1101.$diag_source.nc \${prevyear}1201.$diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 3:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0201.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0301.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0501.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0601.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0801.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0901.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1101.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1201.grid_spec.tile\$i.nc
            """)
        convertSeg = _template("""
            $cubicLoopStart
            set string = `ncdump -h \${hDate}0101.$diag_source.nc | grep UNLIMITED`
            set timename   = `echo \$string[1]`
            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0101.$diag_source.nc \${hDate}0201.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0101.$diag_source.nc \${hDate}0301.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0101.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0401.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0401.$diag_source.nc \${hDate}0501.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0401.$diag_source.nc \${hDate}0601.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0401.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0701.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0701.$diag_source.nc \${hDate}0801.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0701.$diag_source.nc \${hDate}0901.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0701.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}1001.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}1001.$diag_source.nc \${hDate}1101.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}1001.$diag_source.nc \${hDate}1201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}1001.$diag_source.nc
            $cubicLinkGridSpec
            $cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${prevyear}1001.$diag_source.nc \${prevyear}1201.$diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 4:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0201.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0301.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0401.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0601.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0701.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0801.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1001.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1101.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1201.grid_spec.tile\$i.nc
            """)
        convertSeg = _template("""
            $cubicLoopStart
            set string = `ncdump -h \${hDate}0101.$diag_source.nc | grep UNLIMITED`
            set timename   = `echo \$string[1]`
            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0101.$diag_source.nc \${hDate}0201.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0101.$diag_source.nc \${hDate}0301.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0101.$diag_source.nc \${hDate}0401.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0101.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0501.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0501.$diag_source.nc \${hDate}0601.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0501.$diag_source.nc \${hDate}0701.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0501.$diag_source.nc \${hDate}0801.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0501.$diag_source.nc

            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0901.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0901.$diag_source.nc \${hDate}1001.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0901.$diag_source.nc \${hDate}1101.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0901.$diag_source.nc \${hDate}1201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0901.$diag_source.nc
            $cubicLinkGridSpec
            $cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${prevyear}0901.$diag_source.nc \${prevyear}1201.$diag_source.nc > ncks.out
        """, locals(), pp)
    elif (segTime == 1 and segUnits == 'years') or \
        (segTime == 12 and segUnits == 'months'):
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0201.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0301.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0401.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0501.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0601.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0701.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0801.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}0901.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1001.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1101.grid_spec.tile\$i.nc
                ln -s \${hDate}0101.grid_spec.tile\$i.nc \${hDate}1201.grid_spec.tile\$i.nc
            """)
        convertSeg = _template("""
            $cubicLoopStart
            set string = `ncdump -h \${hDate}0101.$diag_source.nc | grep UNLIMITED`
            set timename   = `echo \$string[1]`
            $time_ncks ncks \$ncksopt -d \$timename,1,1 \${hDate}0101.$diag_source.nc tmp01.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,2,2 \${hDate}0101.$diag_source.nc \${hDate}0201.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,3,3 \${hDate}0101.$diag_source.nc \${hDate}0301.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,4,4 \${hDate}0101.$diag_source.nc \${hDate}0401.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,5,5 \${hDate}0101.$diag_source.nc \${hDate}0501.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,6,6 \${hDate}0101.$diag_source.nc \${hDate}0601.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,7,7 \${hDate}0101.$diag_source.nc \${hDate}0701.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,8,8 \${hDate}0101.$diag_source.nc \${hDate}0801.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,9,9 \${hDate}0101.$diag_source.nc \${hDate}0901.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,10,10 \${hDate}0101.$diag_source.nc \${hDate}1001.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,11,11 \${hDate}0101.$diag_source.nc \${hDate}1101.$diag_source.nc > ncks.out
            $time_ncks ncks \$ncksopt -d \$timename,12,12 \${hDate}0101.$diag_source.nc \${hDate}1201.$diag_source.nc > ncks.out
            $time_mv $mv -f tmp01.nc \${hDate}0101.$diag_source.nc
            $cubicLinkGridSpec
            $cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            $time_ncks ncks \$ncksopt -d \$timename,12,12 \${prevyear}0101.$diag_source.nc \${prevyear}1201.$diag_source.nc > ncks.out
        """, locals(), pp)
    else:
        _log.error((f"{diag_source}: segTime {segTime} not supported for seasonal "
            "calculations.  Try 1,2,3,4,6 or 12 month segments."))

    if type == "dec":
        return convertDec
    return convertSeg

def get_subint(node, pp, *args):
    """Return appropriate subinterval."""
    name = node.getName()
    if name == "timeSeries":
        interval = node.findvalue('@chunkLength')
    elif name == "timeAverage":
        interval = node.findvalue('@interval')
    else:
        _log.error((f"nodes of type {name} are not supported, only timeSeries "
            "and timeAverage"))
    interval = int(interval.replace('yr', ''))
    from_ = node.findvalue('@from')
    if from_:
        from_ = int(from_.replace('yr', ''))
        return (interval, from_)

    ints = [int(i.replace('yr', '')) for i in args]
    subint = None
    for i in ints: # XXX
        if (interval and i and interval > i and (interval % i) == 0):
            subint = i

    # get dependent years
    depyears = []
    thisyear = FREUtil.splitDate(pp.t0)
    simstart = FREUtil.splitDate(sim0) # XXX

    if subint:
        i = 1
        while ((thisyear - (subint * i) ) > (thisyear - interval) \
            and (thisyear - (subint * i) ) >= simstart):
            if (((thisyear - simstart + 1 ) % interval) == 0):
                depyears.append(FREUtil.padzeros(thisyear - (subint * i)))
            i += 1

    return (interval, subint, depyears)

def gettimelevels(freq, cl):
    """Get appropriate number of time levels in a time series file."""
    cl = int(cl.replace('yr', ''))
    if freq == util.DateFrequency('daily'):
        return 365 * cl
    elif freq == util.DateFrequency('monthly'):
        return 12 * cl
    elif freq == util.DateFrequency('yearly'):
        return cl

    # XXX FINISH

def segmentLengthInMonths(pp):
    """"""
    segTime, segUnits = getSegmentLength(pp)
    if segUnits == 'months':
        return segTime
    elif segUnits == 'years':
        return 12 * segTime
    else:
        _log.warning((f"Unable to convert segment unit {segUnits} into months; "
            "setting segment_months = 1"))
        return 1

def getSegmentLength(pp):
    """"""
    if pp.opt['S']:
        return (pp.opt['S'], "months")
    else:
        segTime  = FREUtil.getxpathval('runtime/production/segment/@simTime')
        segUnits = FREUtil.getxpathval('runtime/production/segment/@units')
        if segUnits == 'month':
            segUnits = 'months'
        elif segUnits == 'year':
            segUnits = 'years'
        return (int(segTime), segUnits)


# ------------------------------------------------------------------------------

def annualTS(tsNode, sim0, startofrun, diagtablecontent):
    """TIMESERIES - ANNUAL"""
    raise NotImplementedError()

def seasonalTS(tsNode, sim0):
    """TIMESERIES - SEASONAL"""
    raise NotImplementedError()

def monthlyAVfromhist(taNode, sim0):
    """TIMEAVERAGES - MONTHLY"""
    raise NotImplementedError()

def annualAV1yrfromhist(taNode, sim0, write2arch, yr2do):
    """TIMEAVERAGES - ANNUAL 1YR"""
    raise NotImplementedError()

def annualAVxyrfromann(taNode, sim0, ppcNode, annavnodes, annCalcInterval):
    """TIMEAVERAGES - ANNUAL XYR"""
    raise NotImplementedError()

def monthlyTSfromdailyTS(tsNode, sim0, startofrun):
    """TIMESERIES - monthly from daily ts"""
    raise NotImplementedError()

def directTS(tsNode, sim0, startofrun):
    """TIMESERIES - HOURLY, DAILY, MONTHLY, ANNUAL"""
    raise NotImplementedError()

def monthlyAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - MONTHLY"""
    raise NotImplementedError()

def annualAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - ANNUAL XYR"""
    raise NotImplementedError()

def staticvars(diag_source, ptmpDir, tmphistdir, refinedir):
    """Create static variables file."""
    raise NotImplementedError()

def TSfromts(tsNode, sim0, subchunk):
    """TIMESERIES - from smaller timeSeries"""
    raise NotImplementedError()

def seaTSfromts(tsNode, sim0, subchunk):
    """TIMESERIES - from smaller timeSeries"""
    raise NotImplementedError()

def seasonalAVfromhist(taNode, sim0):
    """TIMEAVERAGES - SEASONAL"""
    raise NotImplementedError()

def seasonalAVfromav(taNode, sim0, subint):
    """TIMEAVERAGES - SEASONAL (doesn't support cubedsphere)"""
    raise NotImplementedError()
