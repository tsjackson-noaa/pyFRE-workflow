"""Transliteration of time series/time average subroutines in FRE/bin/frepp.pl.
"""
import sys
import functools
import glob
import operator
import math
import re
from textwrap import dedent

from pyFRE.lib import FREUtil
import pyFRE.util as util
from . import logs, sub

import logging
_log = logging.getLogger(__name__)

_template = util.pl_template # abbreviate

def zInterpolate(zInterp, infile, outfile, caltype, variables, source, pp):
    """Set up interpolation on z levels."""
    # frepp.pl l.3286
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
    plev_command  = 'PLEVEL -a'

    if variables:
        variables = variables.replace(',', ' ')
        variables = variables.replace("'", '')

        plev_command  = 'PLEVEL'
        if pp.opt['v']:
            count = len(variables.split())
            _log.info(f"will interpolate {count} variables to pressure levels for {infile}")

    if zInterp in ("ncep", "am3", "narcaap", "hs20", "era40", "ar5daily", "ncep_subset"):
        csh += _template("""
            set_plevels

            set reqvars = `\NCVARS -st12 infile | grep -e '^ *bk\' -e '^ *pk\' -e '^ *ps\' -c`
            set hgtvars = `\NCVARS -st23 infile | grep -e '^ *temp\' -e '^ *sphum\' -e '^ *zsurf\' -c`

            set vars3d  = `\NCVARS -st3  infile`

            if ( \reqvars == 3 && \#vars3d > 0 ) then
                set vlist = ()
                if ( \hgtvars == 3 ) then
                    set vlist = (divv rvort hght slp)
                endif
                time_plevel plev_command -p "\levels" -i infile -o plev.nc \vlist variables
                check_plevel
                set string = `ncdump -h plev.nc | grep UNLIMITED`
                check_ncdump
                set timename   = `echo \string[1]`
                set string = `ncdump -h plev.nc | grep calendar_type`
                set caltype   = `echo \string[3] | sed 's/"//g'`
                if ( "\caltype" == "" ) set caltype = caltype
                time_ncatted ncatted -h -O -a calendar,\timename,c,c,\caltype plev.nc
                check_ncatted
                time_mv mv plev.nc outfile
                time_rm rm -f infile
            else if ( \reqvars < 3 && \#vars3d > 0 ) then
                echo ERROR: zInterp requested for source, but missing one or more required variables
                exit 1
            else
                time_mv mv infile outfile
            endif
        """, locals(), pp)
    elif zInterp == "zgrid": # ocean
        if not variables:
            variable = "temp,salt,age,u,v";
            _log.info(f"No variables specified, resampling {variables} to zgrid")
        else:
            pass # already cleaned variables
        if pp.platform == "x86_64":
            csh += _template("""
                set taxis = `ncdump -h infile | grep -i '.*=.*unlimited.*currently' | awk '{print \1}'`
                #set hasclimbounds = `ncdump -h infile | grep 'climatology_bounds' | wc -l`
                if ( `ncdump -h infile | grep -c " climatology_bounds("` == 1 ) then
                    time_zgrid /home/rwh/data/regrid_MESO/Resample_on_Z_new -d/home/rwh/data/regrid_MESO/OM3_zgrid.nc -V:variables -T:average_T1,average_T2,average_DT,climatology_bounds -ee -ooutfile infile
                    check_zgrid
                else
                    time_zgrid /home/rwh/data/regrid_MESO/Resample_on_Z_new -d/home/rwh/data/regrid_MESO/OM3_zgrid.nc -V:variables -T:average_T1,average_T2,average_DT,\{taxis}_bounds -ee -ooutfile infile
                    check_zgrid
                endif
            """, locals(), pp)
    else:
        csh += _template("""
            time_mv mv infile outfile
        """, locals(), pp)


def segStartMonths(segTime, segUnits):
    """"""
    # frepp.pl l.3451
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
    # frepp.pl l.3485
    if sourceGrid == 'cubedsphere':
        cubicLoopStart = "set i = 1\nwhile ( \i <= 6 )";
        cubicLoopEnd   = "@ i ++\nend";
        diag_source    = "diag_source.tile\i";

    if segTime == 6:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0201.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0301.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0401.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0501.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0601.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0801.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0901.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1001.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1101.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1201.grid_spec.tile\i.nc
            """)
        convertSeg = _template("""
            cubicLoopStart
            set string = `ncdump -h \{hDate}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0101.diag_source.nc \{hDate}0201.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0101.diag_source.nc \{hDate}0301.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0101.diag_source.nc \{hDate}0401.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,5,5 \{hDate}0101.diag_source.nc \{hDate}0501.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,6,6 \{hDate}0101.diag_source.nc \{hDate}0601.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0101.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0701.diag_source.nc tmp07.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0701.diag_source.nc \{hDate}0801.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0701.diag_source.nc \{hDate}0901.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0701.diag_source.nc \{hDate}1001.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,5,5 \{hDate}0701.diag_source.nc \{hDate}1101.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,6,6 \{hDate}0701.diag_source.nc \{hDate}1201.diag_source.nc > ncks.out
            time_mv mv -f tmp07.nc \{hDate}0701.diag_source.nc
            cubicLinkGridSpec
            cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            time_ncks ncks \ncksopt -d \timename,6,6 \{prevyear}0701.diag_source.nc \{prevyear}1201.diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 2:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0201.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0401.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0601.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0801.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1001.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1201.grid_spec.tile\i.nc
            """)
        convertSeg = _template("""
            cubicLoopStart
            set string = `ncdump -h \{hDate}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0101.diag_source.nc \{hDate}0201.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0101.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0301.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0301.diag_source.nc \{hDate}0401.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0301.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0501.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0501.diag_source.nc \{hDate}0601.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0501.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0701.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0701.diag_source.nc \{hDate}0801.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0701.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0901.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0901.diag_source.nc \{hDate}1001.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0901.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}1101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}1101.diag_source.nc \{hDate}1201.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}1101.diag_source.nc
            cubicLinkGridSpec
            cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            time_ncks ncks \ncksopt -d \timename,2,2 \{prevyear}1101.diag_source.nc \{prevyear}1201.diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 3:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0201.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0301.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0501.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0601.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0801.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0901.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1101.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1201.grid_spec.tile\i.nc
            """)
        convertSeg = _template("""
            cubicLoopStart
            set string = `ncdump -h \{hDate}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0101.diag_source.nc \{hDate}0201.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0101.diag_source.nc \{hDate}0301.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0101.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0401.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0401.diag_source.nc \{hDate}0501.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0401.diag_source.nc \{hDate}0601.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0401.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0701.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0701.diag_source.nc \{hDate}0801.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0701.diag_source.nc \{hDate}0901.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0701.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}1001.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}1001.diag_source.nc \{hDate}1101.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}1001.diag_source.nc \{hDate}1201.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}1001.diag_source.nc
            cubicLinkGridSpec
            cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            time_ncks ncks \ncksopt -d \timename,3,3 \{prevyear}1001.diag_source.nc \{prevyear}1201.diag_source.nc > ncks.out
        """, locals(), pp)
    elif segTime == 4:
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0201.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0301.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0401.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0601.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0701.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0801.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1001.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1101.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1201.grid_spec.tile\i.nc
            """)
        convertSeg = _template("""
            cubicLoopStart
            set string = `ncdump -h \{hDate}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0101.diag_source.nc \{hDate}0201.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0101.diag_source.nc \{hDate}0301.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0101.diag_source.nc \{hDate}0401.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0101.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0501.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0501.diag_source.nc \{hDate}0601.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0501.diag_source.nc \{hDate}0701.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0501.diag_source.nc \{hDate}0801.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0501.diag_source.nc

            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0901.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0901.diag_source.nc \{hDate}1001.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0901.diag_source.nc \{hDate}1101.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0901.diag_source.nc \{hDate}1201.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0901.diag_source.nc
            cubicLinkGridSpec
            cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            time_ncks ncks \ncksopt -d \timename,4,4 \{prevyear}0901.diag_source.nc \{prevyear}1201.diag_source.nc > ncks.out
        """, locals(), pp)
    elif (segTime == 1 and segUnits == 'years') or \
        (segTime == 12 and segUnits == 'months'):
        if sourceGrid == 'cubedsphere':
            cubicLinkGridSpec = dedent("""
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0201.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0301.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0401.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0501.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0601.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0701.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0801.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}0901.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1001.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1101.grid_spec.tile\i.nc
                ln -s \{hDate}0101.grid_spec.tile\i.nc \{hDate}1201.grid_spec.tile\i.nc
            """)
        convertSeg = _template("""
            cubicLoopStart
            set string = `ncdump -h \{hDate}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            time_ncks ncks \ncksopt -d \timename,1,1 \{hDate}0101.diag_source.nc tmp01.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,2,2 \{hDate}0101.diag_source.nc \{hDate}0201.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,3,3 \{hDate}0101.diag_source.nc \{hDate}0301.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,4,4 \{hDate}0101.diag_source.nc \{hDate}0401.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,5,5 \{hDate}0101.diag_source.nc \{hDate}0501.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,6,6 \{hDate}0101.diag_source.nc \{hDate}0601.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,7,7 \{hDate}0101.diag_source.nc \{hDate}0701.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,8,8 \{hDate}0101.diag_source.nc \{hDate}0801.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,9,9 \{hDate}0101.diag_source.nc \{hDate}0901.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,10,10 \{hDate}0101.diag_source.nc \{hDate}1001.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,11,11 \{hDate}0101.diag_source.nc \{hDate}1101.diag_source.nc > ncks.out
            time_ncks ncks \ncksopt -d \timename,12,12 \{hDate}0101.diag_source.nc \{hDate}1201.diag_source.nc > ncks.out
            time_mv mv -f tmp01.nc \{hDate}0101.diag_source.nc
            cubicLinkGridSpec
            cubicLoopEnd
        """, locals(), pp)
        convertDec = _template("""
            time_ncks ncks \ncksopt -d \timename,12,12 \{prevyear}0101.diag_source.nc \{prevyear}1201.diag_source.nc > ncks.out
        """, locals(), pp)
    else:
        _log.error((f"{diag_source}: segTime {segTime} not supported for seasonal "
            "calculations.  Try 1,2,3,4,6 or 12 month segments."))

    if type == "dec":
        return convertDec
    return convertSeg

def get_subint(node, intervals, t0, sim0):
    """Return appropriate subinterval."""
    # frepp.pl l.3738
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

    ints = [int(i.replace('yr', '')) for i in intervals]
    subint = None
    for i in ints: # XXX
        if (interval and i and interval > i and (interval % i) == 0):
            subint = i

    # get dependent years
    depyears = []
    thisyear = FREUtil.splitDate(t0)
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
    # frepp.pl l.3910
    """Get appropriate number of time levels in a time series file."""
    cl = int(cl.replace('yr', ''))
    if 'daily' in freq or 'day' in freq:
        return 365 * cl
    elif 'mon' in freq:
        return 12 * cl
    elif 'ann' in freq or 'yr' in freq or 'year' in freq:
        return cl
    elif 'hour' in freq or 'hr' in freq:
        n = int(freq.replace('hr', ''))
        if n != 0:
            return 365 * 24 * cl // n
        else:
            return 365 * 24 * cl
    elif 'min' in freq:
        count_per_hour = int(freq.replace('min', ''))
        return 365 * 24 * count_per_hour * cl
    elif 'season' in freq:
        return cl #only one season per year per file, so this must be 1
    else:
        _log.warning(f"frequency {freq} not recognized in gettimelevels")
        return cl

def segmentLengthInMonths(pp):
    """"""
    # frepp.pl l.7985
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
    # frepp.pl l.7994
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


def lcm(*args):
    # frepp.pl l. 3793
    return functools.reduce(operator.mul,args, 1) // math.gcd(args)

# ------------------------------------------------------------------------------

def annualTS(tsNode, exp, cpt):
    """TIMESERIES - ANNUAL"""
    # frepp.pl l.3980
    sim0 = cpt.sim0
    startofrun = cpt.startofrun
    diagtablecontent = exp.diagtablecontent
    ppcNode = tsNode.parentNode()
    freq   = 'annual'
    source = 'monthly'    #always use monthly, don't need tsNode->findvalue('@source');
    chunkLength = tsNode.findvalue('@chunkLength')
    outdirpath  = f"{cpt.component}/ts/{freq}/{chunkLength}"
    outdir      = f"{exp.ppRootDir}/{outdirpath}"
    if not chunkLength:
        logs.mailuser(f"Cannot create {cpt.component} {freq} timeSeries unless you set a chunkLength.")
        _log.error(f"Cannot create {cpt.component} {freq} timeSeries unless you set a chunkLength.")
        return ""

    #need timeSeries freq=monthly chunkLength=something.  Then split apart into yearly data.
    #prefer identical chunklength if available
    TSchunkLength = ""
    for node in ppcNode.findnodes(f"timeSeries[\@freq='monthly' and \@chunkLength='{chunkLength}']"):
        TSchunkLength = node.findvalue('@chunkLength')
        if TSchunkLength:
            _log.debug(f'will use monthly_ts cl={TSchunkLength}')
            break

    #otherwise use whatever is available, but we don't support 1yr for now
    if not TSchunkLength:
        for node in ppcNode.findnodes("timeSeries[\@freq='monthly']"):
            TSchunkLength = node.findvalue('@chunkLength')
            if TSchunkLength != "" and TSchunkLength != "1yr":
                _log.debug(f'will use monthly_ts cl={TSchunkLength}')
                break

    reqpath = f"tempCache/{cpt.component}/ts/monthly/{TSchunkLength}"

    if not TSchunkLength:
        existing = glob.glob(f"reqpath/*/*")
        if not existing:
            #if have annual diag data, this TS can be created with directTS
            mysource =tsNode.findvalue('@source')
            if not mysource:
                mysource = ppcNode.findvalue('@source')
            dtc = exp.diagtablecontent.split('\n')
            sourcelines = [l_ for l_ in dtc if re.match(rf"""^\s*['"]{mysource}['"]""", l_)]
            fd = sourcelines[0].split(',')
            if (fd[1] == "1" and 'year' in fd[2]) or (fd[1] == "12" and 'month' in fd[2]):
                _log.debug(f"using history file {mysource}")
                return directTS(tsNode, exp, cpt)
            else:
                logs.mailuser((f"Cannot create {cpt.component} {freq} timeSeries "
                    "unless you generate <timeSeries freq='monthly' chunkLength='Xyr'>"))
                _log.error((f"Cannot create {cpt.component} {freq} timeSeries "
                    "unless you generate <timeSeries freq='monthly' chunkLength='Xyr'>"))
                return ""
        else:
            reqfiles = glob.glob(f"reqpath/*")
            TSchunkLength = util.shell(f"ls -1 {reqpath} | sort -g | head -1", log=_log)
            TSchunkLength = TSchunkLength.replace(reqpath,"")
            TSchunkLength = TSchunkLength.replace('/',"")
            _log.debug(f"{cpt.component} {freq} timeSeries calculation found data at ts/monthly/{TSchunkLength}")
        reqpath = f"\tempCache/{cpt.component}/ts/monthly/{TSchunkLength}"

    tmp = FREUtil.modifydate(exp.tEND, "+1 sec")
    yrsSoFar = FREUtil.Delta_Format(FREUtil.dateCalc(cpt.sim0, tmp), 0, "%yd")
    int_ = int(TSchunkLength.replace('yr',''))
    mod_ = int(yrsSoFar) % int_
    if mod_ != 0:
        return ""
    mkdircommand = exp.mkdircommand + f"{outdir} "
    cl = int(chunkLength.replace('yr', ""))
    if int_ > exp.maxyrs:
        exp.maxyrs = int_

    #this is not exactly right.  You need to make it not do any annualTS till
    # cl and to leave it in TMP till everything's done. then change this to cl

    #check that all files up to current time exist
    monthnodes  = ppcNode.findnodes('timeSeries[@freq="monthly"]')
    diag_source = ""
    if monthnodes:
        monthnode = ppcNode.findnodes('timeSeries[@freq="monthly"]')->get_node(1); # XXX
        diag_source = monthnode.getAttribute('@source')
    if not diag_source:
        diag_source = tsNode.findvalue('../@source')
    if not diag_source:
        diag_source = f"{cpt.component}_month"

    #get variables
    variables = FREUtil.cleanstr(tsNode.findvalue('variables'))
    _log.debug(f"\t\tfrom xml, vars are '{variables}'")
    variables = [f"{s}.nc" for s in variables.split(' ')]
        # if ( "variables" ne "" ) { variables =~ s//.nc/g } # XXX

    tBEG = FREUtil.modifydate(pp.tEND, f"-{int_} years +1 sec")
    tBEGf = FREUtil.graindate(tBEG, 'monthly')
    tENDf = FREUtil.graindate(pp.tEND, 'monthly')
    check_ncdump  = logs.errorstr(f"NCDUMP ({cpt.component} {freq} ts from {source})")
    check_ncks    = logs.errorstr(f"NCKS ({cpt.component} {freq} ts from {source})")
    check_timavg  = logs.retryonerrorend(f"TIMAVG ({cpt.component} {freq} ts from {source})")
    retry_timavg  = logs.retryonerrorstart(f"TIMAVG")
    check_ncrcat  = logs.errorstr(f"NCRCAT ({cpt.component} {freq} ts from {source})")
    check_ncatted = logs.errorstr(f"NCATTED ({cpt.component} {freq} ts from {source})")
    check_dmget   = logs.errorstr(f"DMGET ({cpt.component} {freq} ts from {source})")
    check_nccopy  = logs.errorstr(f"NCCOPY ({cpt.component} {freq} ts from {source})")
    csh           = logs.setcheckpt(f"annualTS_{chunkLength}")
    csh += _template("""

        #####################################
        echo 'timeSeries (component freq ts from source)'
        cd \work
        find \work/* -maxdepth 1 -exec rm -rf {} \\;
        set outdir = outdir
        if ( ! -e \outdir ) mkdir -p \outdir
        if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath
    """, locals(), cpt)
    if pp.opt['z']:
        csh += logs.begin_systime()

    for chunkyear in range(int_):
        tYEAR = FREUtil.modifydate(tBEG, f"+ {chunkyear} years" )
        tYEARf = FREUtil.graindate(tYEAR, 'annual')
        startmonth = chunkyear * 12 + 1
        endmonth   = startmonth + 11

        #if it is time, chunk the files`
        #if tYEARf-sim0 % chunklength == 0 then cat files to outfile
        catfiles       = ""
        makecpio       = ""
        getlist        = ""
        chunkedoutfile = ""

        #print "tYEARf=tYEARf sim0=sim0 cl=cl\n";
        if (tYEARf - FREUtil.graindate(cpt.sim0, 'annual')) % cl == 0:
            n = cl -1
            begin = FREUtil.modifydate(tYEAR, f"- {n} years" )
            chunkedoutfile = f"{cpt.component}.{begin}-{tYEARf}.\var"
            filelist = ""
            for year in range(begin, tYEARf + 1):
                year = FREUtil.padzeros(year)
                filelist += f"\tempCache/{cpt.component}.{year}.\var ";
                getlist  += f"{cpt.component}.{year}.*.nc ";
            }
            if exp.aggregateTS:
                makecpio = sub.createcpio(
                    "\tempCache/outdirpath",
                    outdir,
                    f"{cpt.component}.{begin}-{tYEARf}",
                    FREUtil.timeabbrev(freq),
                    1
                )
            }
            compress = sub.compress_csh(chunkedoutfile, check_nccopy)
            catfiles = _template("""
                if ( -e chunkedoutfile ) rm -f chunkedoutfile
                time_ncrcat ncrcat \ncrcatopt filelist chunkedoutfile
                check_ncrcat
                time_ncatted ncatted -h -O -a filename,global,m,c,"chunkedoutfile" chunkedoutfile
                check_ncatted
                compress
                time_mv mvfile chunkedoutfile outdir/
                if ( \status ) then
                    echo "WARNING: data transfer failure, retrying..."
                    time_mv mvfile chunkedoutfile outdir/
                    checktransfer
                endif
                time_mv mv chunkedoutfile \tempCache/outdirpath/
                time_rm rm -f filelist
            """, locals())

        #set up loop over variables
        if not variables:
            forloop = _template("""
                foreach file ( reqpath/component.tBEGf-tENDf.*.nc )
                    set var = `echo \file | sed "s#.*/##;s/component.tBEGf-tENDf.//"`
            """, locals())
        else:
            forloop = _template("""
                foreach var ( variables )
                    set file = reqpath/component.tBEGf-tENDf.\var
            """, locals())
        csh += _template("""

            forloop
                if ( chunkyear == 0 ) then
                time_cp cp \file .
                if ( \status ) then
                    echo "WARNING: data transfer failure, retrying..."
                    time_cp cp \file .
                    checktransfer
                endif
                endif
                set file = "./\file:t"
                set string = `ncdump -h \file | grep UNLIMITED`
                check_ncdump
                set timename   = `echo \string[1]`
                time_ncks ncks \ncksopt -d \timename,startmonth,endmonth \file year.nc > ncks.out
                check_ncks
                time_timavg \TIMAVG -o \tempCache/component.tYEARf.\var year.nc
                retry_timavg
                time_timavg \TIMAVG -o \tempCache/component.tYEARf.\var year.nc
                check_timavg
                time_rm rm -f year.nc
                catfiles
            end
            makecpio
            else
                echo ERROR: Error: input files do not exist
            endif

        """, locals(), cpt)

        if pp.opt['z']:
            csh += logs.end_systime()
        csh += logs.mailerrors(outdir)
        return csh

def seasonalTS(tsNode, sim0):
"""TIMESERIES - SEASONAL"""
# frepp.pl l.4217
    #tsNode = _[0] ;
    sim0    = _[1];
    ppcNode = _[0]->parentNode;
    freq    = 'seasonal';
    source = 'monthly';    #always use monthly, don't need tsNode->findvalue('@source');
    chunkLength = _[0]->findvalue('@chunkLength');
    outdirpath  = "component/ts/freq/chunkLength";
    outdir      = "ppRootDir/outdirpath";

    #check if need to convert cube sphere grid to lat lon
    if ( "sourceGrid" eq 'cubedsphere' ) {
        print STDERR
            "WARNING: Calculating seasonal timeseries from cubed sphere data is not supported in this version of frepp.  Skipping.  To avoid this error message, edit your xml to remove the request for seasonal timeseries for cubed sphere components.\n";
        mailuser(
            "WARNING: Calculating seasonal timeseries from cubed sphere data is not supported in this version of frepp.  Skipping.  To avoid this error message, edit your xml to remove the request for seasonal timeseries for cubed sphere components."
        );
        return "";
    }

    if ( "chunkLength" eq "" ) {
        mailuser("Cannot create component freq timeSeries unless you set a chunkLength.");
        print STDERR
            "ERROR: Cannot create component freq timeSeries unless you set a chunkLength.\n";
        return "";
    }

    #need timeSeries freq=monthly chunkLength=something.  Then split apart into yearly data.
    requiredts    = "timeSeries[\@freq='monthly']\n";
    TSchunkLength = "";
    foreach node ( ppcNode->findnodes("requiredts") ) {
        TSchunkLength = node->findvalue('@chunkLength');
        if ( "TSchunkLength" ne "" ) {
            print STDERR "will use monthly_ts cl=TSchunkLength\n" if opt_v;
            last;
        }
    }
    reqpath = "component/ts/monthly/TSchunkLength";
    if ( "TSchunkLength" eq "" ) {
        my @existing = <ppRootDir/reqpath/*/*>;
        if ( scalar(@existing) eq 0 ) {
            mailuser(
                "Cannot create component freq timeSeries unless you generate <timeSeries freq='monthly' chunkLength='Xyr'>"
            );
            print STDERR
                "\nERROR: Cannot create component freq timeSeries unless you generate <timeSeries freq='monthly' chunkLength='Xyr'>\n";
            return "";
        }
        else {
            my @reqfiles = <ppRootDir/reqpath/*>;
            chomp( TSchunkLength = `ls -1 ppRootDir/reqpath | sort -g | head -1` );
            TSchunkLength =~ s/ppRootDir\/reqpath//;
            TSchunkLength =~ s/\///g;
            print STDERR
                "component freq timeSeries calculation found data at ts/monthly/TSchunkLength\n"
                if opt_v;
        }
        reqpath = "component/ts/monthly/TSchunkLength";
    } ## end if ( "TSchunkLength" ...)
    if (opt_v) { print "      TSchunkLength is TSchunkLength\n"; }
    tmp = FREUtil::modifydate( tEND, "+1 sec" );
    yrsSoFar = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int = TSchunkLength;
    int =~ s/yr//;
    cl = chunkLength;
    cl =~ s/yr//;
    lcmchunk = lcm( int, cl );
    mod = yrsSoFar % lcmchunk;
    if ( mod != 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( cl > maxyrs ) { maxyrs = cl; }

    #check that all files up to current time exist
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    diag_source = "";
    if ( scalar @monthnodes ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    #set dates
    tENDprev = FREUtil::modifydate( tEND,     "- lcmchunk years" );
    tBEG     = FREUtil::modifydate( tENDprev, "+ 1 sec" );
    tBEGf     = FREUtil::graindate( tBEG,     'monthly' );
    tENDf     = FREUtil::graindate( tEND,     'monthly' );
    tENDprevf = FREUtil::graindate( tENDprev, 'monthly' );
    prevyear  = FREUtil::graindate( tENDprev, 'year' );

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    forloop   = "";
    if ( "variables" eq "" ) {
        forloop = <<EOF;
foreach file ( \tempCache/reqpath/component.tBEGf-tENDf.*.nc )
set var = `echo \file | sed "s#.*/##;s/component.tBEGf-tENDf.//"`
EOF
    }
    else {
        print STDERR "           from xml, vars are 'variables'\n" if opt_v;
        variables =~ s/ /.nc /g;
        if ( "variables" ne "" ) { variables =~ s//.nc/g }
        forloop .= <<EOF;
foreach var ( variables )
set file = \tempCache/reqpath/component.tBEGf-tENDf.\var
EOF
    }

    check_ncks        = errorstr("NCKS (component seasonal ts)");
    check_ncdump      = errorstr("NCDUMP (component seasonal ts)");
    check_ncrcat      = errorstr("NCRCAT (component seasonal ts)");
    check_timavg      = retryonerrorend("TIMAVG (component seasonal ts)");
    retry_timavg      = retryonerrorstart("TIMAVG");
    check_ncatted     = errorstr("NCATTED (component seasonal ts)");
    check_dmget       = errorstr("DMGET (component seasonal ts)");
    check_splitncvars = errorstr("SPLITNCVARS (component seasonal ts)");
    check_cpio        = errorstr("CPIO/TAR (component seasonal ts)");
    check_nccopy      = errorstr("NCCOPY (component seasonal ts)");
    csh               = setcheckpt("seasonalTS_chunkLength");
    csh .= <<EOF;

#####################################
echo 'timeSeries (component seasonal from monthly ts )'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath
#time_dmget dmget -d reqpath "component.tBEGf-tENDf.*.nc"
EOF
    if (opt_z) { csh .= begin_systime(); }
    totalseasons = 4 * lcmchunk;
    startseason  = 1;
    startflag    = FREUtil::dateCmp( tBEG, sim0 );

    foreach season ( startseason .. totalseasons ) {
        nummonths = season * 3 - 4;
        tSEASON   = FREUtil::modifydate( tBEG, "+ nummonths months" );
        tSEASONf  = FREUtil::graindate( tSEASON, 'seasonal' );
        my (year)    = tSEASONf =~ /(\d{4,})\./;
        catfiles  = "";
        makecpio  = "";
        tmp       = FREUtil::graindate( sim0, 'annual' );

        #if it is time, chunk the files.  only put chunked files into archive now.
        if ( ( year - tmp + 1 ) % cl == 0 ) {
            tmp = year - tmp + 1;
            my (abbrev)       = tSEASONf =~ /\d{4,}\.(\w{3})/;
            begin          = FREUtil::padzeros( year - cl + 1 );
            chunkedoutfile = "component.begin-year.abbrev.\var";
            filelist       = "";
            foreach year ( begin .. year ) {
                year = FREUtil::padzeros(year);
                filelist .= "component.year.abbrev.\var ";
            }
            if ( abbrev =~ "SON" and aggregateTS ) {
                makecpio = createcpio( "\tempCache/outdirpath", outdir,
                    "component.begin-year", FREUtil::timeabbrev(freq), 1 );
            }
            compress = compress_csh( chunkedoutfile, check_nccopy );
            catfiles = <<EOF;
if ( -e chunkedoutfile ) rm -f chunkedoutfile
time_ncrcat ncrcat \ncrcatopt filelist chunkedoutfile
check_ncrcat
compress
time_mv mvfile chunkedoutfile \outdir/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile chunkedoutfile \outdir/
checktransfer
endif
time_mv mv chunkedoutfile \tempCache/outdirpath/
time_rm rm -f filelist

EOF
        } ## end if ( ( year - tmp + ...))
        if ( season == 1 ) {
            if ( startflag == 0 ) {
########################## FIRST DJF OF RUN ################################
                csh .= <<EOF;
#season season (tSEASONf) note: december used twice
forloop
if ( ! -f \file ) then
    if ( -f ppRootDir/reqpath/\file:t ) then
        time_cp cp ppRootDir/reqpath/\file:t \file
        if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/reqpath/\file:t \file
        checktransfer
        endif
    else
        echo ERROR: necessary file not found: ppRootDir/reqpath/\file:t
    endif
endif
set string = `ncdump -h \file | grep UNLIMITED`
set timename   = `echo \string[1]`
time_ncks ncks \ncksopt -d \timename,1,2 -d \timename,12,12 \file janfeb.nc > ncks.out
check_ncks
time_timavg \TIMAVG -o component.tSEASONf.\var janfeb.nc
retry_timavg
    time_timavg \TIMAVG -o component.tSEASONf.\var janfeb.nc
check_timavg
time_rm rm -f janfeb.nc
catfiles
end

EOF
            } ## end if ( startflag == 0 )
            else {
########################## FIRST DJF OF THIS PP ################################
                check_prev = errorstr("Could not acquire previous december");
                check_hist
                    = errorstr("Could not acquire previous december from history file");
                csh .= <<EOF;
#season season (tSEASONf)
#time_dmget dmget -d reqpath "component.*-tENDprevf.*.nc"
forloop
if ( ! -f \file ) then
    if ( -f ppRootDir/reqpath/\file:t ) then
        time_cp cp ppRootDir/reqpath/\file:t \file
        if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/reqpath/\file:t \file
        checktransfer
        endif
    else
        echo ERROR: necessary file not found: ppRootDir/reqpath/\file:t
    endif
endif
set string = `ncdump -h \file | grep UNLIMITED`
set timename   = `echo \string[1]`
time_ncks ncks \ncksopt -d \timename,1,2 \file janfeb.nc > ncks.out
check_ncks
#get december from previous file
set prev = (`ls ./component.*-tENDprevf.\var`)
if ( "\prev" == "" ) then
    set prev = (`ls ppRootDir/reqpath/component.*-tENDprevf.\var`)
    time_cp cp \prev .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp \prev .
        checktransfer
    endif
    set prev = "./\prev:t"
endif
if ( "\prev" == "" && -e "ppRootDir/reqpath/component.*-tENDprevf.mon.nc.cpio") then
    time_dmget dmget -d ppRootDir/reqpath "component.*-tENDprevf.mon.nc.cpio"
    time_cp cp ppRootDir/reqpath/component.*-tENDprevf.mon.nc.cpio .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/reqpath/component.*-tENDprevf.mon.nc.cpio .
        checktransfer
    endif
    time_uncpio uncpio -ivI component.*-tENDprevf.mon.nc.cpio '*12.*.nc'
    time_dmput dmput reqpath "component.*-tENDprevf.mon.nc.cpio"
    set prev = (`ls ./component.*-tENDprevf.\var`)
endif
if ( "\prev" != "" ) then
    set decstring = `ncdump -h \prev | grep UNLIMITED`
    check_ncdump
    @ len = \#decstring - 1
    set length = `echo \decstring[\len] | cut -c2-`
    time_ncks ncks \ncksopt -d \timename,\length,\length \prev dec.nc > ncks.out
    check_ncks
EOF

                #might need to get the data from the history file if previous pp is not done
                convertDec = convertSegments( segTime, segUnits, diag_source, 'dec' );

                #check for zInterp
                zInterp     = ppcNode->findvalue('@zInterp');
                zInterp_csh = "";
                if ( "zInterp" ne "" ) {
                    zInterp_csh = zInterpolate( zInterp, "\{prevyear}1201.diag_source.nc",
                        'tmp.nc', caltype, variables, diag_source );
                    zInterp_csh .= "\nmv -f tmp.nc \{prevyear}1201.diag_source.nc";
                }

                prevhistcpio = "prevyear" . "0101.nc.cpio";
                prevhisttar  = "prevyear" . "0101.nc.tar";
                csh .= <<EOF;
else
    if ( ! -e \var ) then
        set prevyear = prevyear
        if ( ! -e \{prevyear}1201.diag_source.nc ) then
            if ( -e opt_d/prevhistcpio ) then
                time_dmget dmget "opt_d/prevhistcpio"
                time_uncpio uncpio -ivI opt_d/prevhistcpio '*.diag_source.nc'
                check_cpio
                time_dmput dmput "opt_d/prevhistcpio"
            else if ( -e opt_d/prevhisttar ) then
                time_dmget dmget "opt_d/prevhisttar"
                time_untar tar -xvf opt_d/prevhisttar --wildcards '*.diag_source.nc'
                check_cpio
                time_dmput dmput "opt_d/prevhisttar"
            else
                echo ERROR: Previous December (\{prevyear}1201.diag_source.nc) is not available for seasonal calculations
                exit 1
            endif
            set string = `ncdump -h \{prevyear}0101.diag_source.nc | grep UNLIMITED`
            set timename   = `echo \string[1]`
            convertDec
            zInterp_csh
        endif
        time_splitncvars \SPLITNCVARS -v \var:r \{prevyear}1201.diag_source.nc
        check_splitncvars
    endif
    test -e \var
    check_hist
    time_mv mv \var dec.nc
endif
test -e dec.nc
check_prev

if ( -e decjanfeb.nc ) rm -f decjanfeb.nc
time_ncrcat ncrcat \ncrcatopt dec.nc janfeb.nc decjanfeb.nc
check_ncrcat
time_timavg \TIMAVG -o component.tSEASONf.\var decjanfeb.nc
retry_timavg
    time_timavg \TIMAVG -o component.tSEASONf.\var decjanfeb.nc
check_timavg
time_rm rm -f janfeb.nc dec.nc decjanfeb.nc
catfiles
end

EOF
            } ## end else [ if ( startflag == 0 )]
        } ## end if ( season == 1 )
        else {
########################## REMAINING SEASONS ################################
            startmonth = season * 3 - 3;
            endmonth   = season * 3 - 1;
            csh .= <<EOF;
#season season (tSEASONf)
forloop
if ( ! -f \file ) then
    if ( -f ppRootDir/reqpath/\file:t ) then
        time_cp cp ppRootDir/reqpath/\file:t \file
        if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/reqpath/\file:t \file
        checktransfer
        endif
    else
        echo ERROR: necessary file not found: ppRootDir/reqpath/\file:t
    endif
endif
set string = `ncdump -h \file | grep UNLIMITED`
set timename   = `echo \string[1]`
time_ncks ncks \ncksopt -d \timename,startmonth,endmonth \file season.nc > ncks.out
check_ncks
time_timavg \TIMAVG -o component.tSEASONf.\var season.nc
retry_timavg
    time_timavg \TIMAVG -o component.tSEASONf.\var season.nc
check_timavg
time_rm rm -f season.nc
catfiles
end
makecpio

EOF
        } ## end else [ if ( season == 1 ) ]
    } ## end foreach season ( startseason...)
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);
    return csh;
} ## end sub seasonalTS

def monthlyAVfromhist(taNode, sim0):
"""TIMEAVERAGES - MONTHLY"""
# frepp.pl l.4588
    #taNode = _[0] ;
    sim0    = _[1];
    ppcNode = _[0]->parentNode;

    #check for appropriate segment lengths
    my @reqStartMonths = segStartMonths( segTime, segUnits );

    src         = 'monthly';
    interval    = _[0]->findvalue('@interval');
    outdir      = "ppRootDir/component/av/src" . "_interval";
    chunkLength = _[0]->findvalue('@chunkLength');
    tmp         = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar    = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int         = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }

    hDateyr = userstartyear;
    my @hDates  = ( FREUtil::padzeros( hDateyr - int + 1 ) .. "hDateyr" );

    sim0f = FREUtil::graindate( sim0, src );
    t0f   = FREUtil::graindate( t0,   src );
    tENDf = FREUtil::graindate( tEND, src );
    range = FREUtil::graindate( tEND, 'year' );
    if ( scalar @hDates > 1 ) { range = FREUtil::padzeros( range - int + 1 ) . "-range"; }
    foreach d (@hDates) { historyfiles .= "d" . "0101.nc.tar "; }

    diag_source = _[0]->findvalue('@diag_source');
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    zInterp    = ppcNode->findvalue('@zInterp');
    do_zInterp = 0;
    if ( "zInterp" ne "" ) { do_zInterp = 1; }
    convertSeg = convertSegments( segTime, segUnits, diag_source );
    check_ncatted = errorstr("NCATTED (component src interval averages)");

    #check_cpio = errorstr("CPIO (component src interval averages)");
    check_cpio_msg = "CPIO (component src interval averages)";
    check_ncrcat   = errorstr("NCRCAT (component src interval averages)");
    check_timavg   = retryonerrorend("TIMAVG (component src interval averages)");
    retry_timavg   = retryonerrorstart("TIMAVG");
    check_plevel   = errorstr("PLEVEL (component src interval averages)");
    check_ncdump   = errorstr("NCDUMP (component src interval averages)");
    check_fregrid  = errorstr("FREGRID (component src interval averages)");
    check_ncrename = errorstr("NCRENAME (component src interval averages)");
    check_nccopy   = errorstr("NCCOPY (component src interval averages)");

    csh = setcheckpt("monthlyAVfromhist_interval");
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src interval averages)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
EOF
    if (opt_z) { csh .= begin_systime(); }
    if ( "sourceGrid" eq 'cubedsphere' ) {    #IF CUBIC
        tile = '.tilei';

        csh .= <<EOF;
foreach hDate ( @hDates )
    foreach sourcefile ( `ls \histDir/\hDate*/*.diag_source.tile*nc`)
    ln -s \sourcefile .
    end
    foreach file ( `ls \histDir/\hDate*/*.grid_spec.tile*.nc`)
    ln -s \file .
    end
end
foreach hDate ( @hDates )
convertSeg
end

if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 \sourcefile`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
unset static_vars avg_vars
endif
set month = 1
while (\month <= 12)
set i = 1
while ( \i <= 6 )

set monthf = `echo \month | sed 's/.*/0&/;s/.\\(..\\)/\\1/'`
set histmonth = "\monthf"

if ( "variables" != '' ) then
    time_ncrcat ncrcat \ncrcatopt -v \vars *\{histmonth}01.diag_sourcetile.nc month.nc
else
    time_ncrcat ncrcat \ncrcatopt *\{histmonth}01.diag_sourcetile.nc month.nc
endif
check_ncrcat
time_rm rm -f *\{histmonth}01.diag_sourcetile.nc
EOF

        if (do_zInterp) {
            csh .= <<EOF;
    time_timavg \TIMAVG -o modellevels.nc month.nc
    retry_timavg
        time_timavg \TIMAVG -o modellevels.nc month.nc
    check_timavg
    time_rm rm -f month.nc
EOF
            csh
                .= zInterpolate( zInterp, 'modellevels.nc',
                "hDates[0]\{histmonth}01.diag_sourcetile.nc",
                caltype, variables, diag_source );

        }
        else {    #no zinterp
            csh .= <<EOF;
    time_timavg \TIMAVG -o hDates[0]\{histmonth}01.diag_sourcetile.nc month.nc
    retry_timavg
        time_timavg \TIMAVG -o hDates[0]\{histmonth}01.diag_sourcetile.nc month.nc
    check_timavg
    time_rm rm -f month.nc
EOF
        }

        if ( "xyInterp" ne '' ) {    #CUBIC to LATLON

            fregrid_wt = '';
            if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                print
                    "       land_frac found, weighting exchange grid cell with hDates[0]\{histmonth}01.land_static\n"
                    if opt_v;
                fregrid_wt
                    = "--weight_file hDates[0]\{histmonth}01.land_static --weight_field land_frac";
            }
            call_and_check_fregrid = call_tile_fregrid;
            call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
            call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
            call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
            compress = compress_csh( "component.range.\monthf.nc", check_nccopy );
            csh .= <<EOF;
@ i ++
end

set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDates[0]\{histmonth}01
set fregrid_in = hDates[0]\{histmonth}01.diag_source
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

mv \fregrid_in.nc component.range.\monthf.nc
time_ncatted ncatted -h -O -a filename,global,m,c,"component.range.\monthf.nc" component.range.\monthf.nc
check_ncatted

compress

time_mv mvfile component.range.\monthf.nc \outdir/component.range.\monthf.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile component.range.\monthf.nc \outdir/component.range.\monthf.nc
    checktransfer
endif
time_rm rm component.range.\monthf.nc
time_dmput dmput \outdir/component.range.\monthf.nc

EOF

        } ## end if ( "xyInterp" ne '')
        else {    #CUBIC - no conversion
            compress = compress_csh( "component.range.\monthftile.nc", check_nccopy );
            csh .= <<EOF;
mv hDates[0]\{histmonth}01.diag_sourcetile.nc component.range.\monthftile.nc
time_ncatted ncatted -h -O -a filename,global,m,c,"component.range.\monthftile.nc" component.range.\monthftile.nc
check_ncatted
compress
time_mv mvfile component.range.\monthftile.nc \outdir/component.range.\monthftile.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile component.range.\monthftile.nc \outdir/component.range.\monthftile.nc
    checktransfer
endif
time_rm rm component.range.\monthftile.nc
time_dmput dmput \outdir/component.range.\monthftile.nc
@ i++
end
EOF
        }

        csh .= <<EOF;
@ month++
end

EOF

    } ## end if ( "sourceGrid" eq ...)
    else {    #IF NOT CUBIC

        csh .= <<EOF;
foreach hDate ( @hDates )
foreach file ( `ls \histDir/\hDate*/*.diag_source.nc`)
    ln -s \file .
end
convertSeg
end
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 \file`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
unset static_vars avg_vars
endif

set month = 1
while (\month <= 12)
set monthf = `echo \month | sed 's/.*/0&/;s/.\\(..\\)/\\1/'`
set histmonth = "\monthf"

if ( -e month.nc ) rm -f month.nc
if ( "variables" != '' ) then
    time_ncrcat ncrcat \ncrcatopt -v \vars *\{histmonth}01.diag_source.nc month.nc
else
    time_ncrcat ncrcat \ncrcatopt *\{histmonth}01.diag_source.nc month.nc
endif
check_ncrcat
time_rm rm -f *\{histmonth}01.diag_source.nc
EOF

        if (do_zInterp) {
            csh .= <<EOF;
time_timavg \TIMAVG -o modellevels.nc month.nc
retry_timavg
    time_timavg \TIMAVG -o modellevels.nc month.nc
check_timavg
EOF
            csh .= zInterpolate( zInterp, 'modellevels.nc', "component.range.\monthf.nc",
                caltype, variables, component );
        }
        else {
            csh .= <<EOF;
time_timavg \TIMAVG -o component.range.\monthf.nc month.nc
retry_timavg
    time_timavg \TIMAVG -o component.range.\monthf.nc month.nc
check_timavg
EOF
        }

        # convert latlon/tripolar to latlon
        if ( "xyInterp" ne '' ) {
            fregrid_wt = '';
            if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                print
                    "       land_frac found, weighting exchange grid cell with hDates[0]\{histmonth}01.land_static\n"
                    if opt_v;
                fregrid_wt
                    = "--weight_file hDates[0]\{histmonth}01.land_static --weight_field land_frac";
            }
            call_and_check_fregrid = call_fregrid;
            call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
            call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
            call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
            csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDates[0]\{histmonth}01
set fregrid_in = "component.range.\monthf"
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid
mv \fregrid_in.nc component.range.\monthf.nc

EOF
        } ## end if ( "xyInterp" ne '')

        compress = compress_csh( "component.range.\monthf.nc", check_nccopy );

        csh .= <<EOF;
time_ncatted ncatted -h -O -a filename,global,m,c,"component.range.\monthf.nc" component.range.\monthf.nc
check_ncatted
compress
time_mv mvfile component.range.\monthf.nc \outdir/component.range.\monthf.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile component.range.\monthf.nc \outdir/component.range.\monthf.nc
    checktransfer
endif
time_rm rm component.range.\monthf.nc
time_dmput dmput \outdir/component.range.\monthf.nc

time_rm rm -f month.nc
@ month++
end

EOF
    } ## end else [ if ( "sourceGrid" eq ...)]

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub monthlyAVfromhist

def annualAV1yrfromhist(taNode, sim0, write2arch, yr2do):
"""TIMEAVERAGES - ANNUAL 1YR"""
# frepp.pl l.4911
        #taNode = _[0] ;
    sim0       = _[1];
    write2arch = _[2];
    yr2do      = _[3];

    #check for appropriate segment lengths
    my @reqStartMonths = segStartMonths( segTime, segUnits );

    ppcNode     = _[0]->parentNode;
    src         = 'annual';
    interval    = _[0]->findvalue('@interval');
    chunkLength = _[0]->findvalue('@chunkLength');

    if ( "yr2do" eq '' ) { yr2do = hDate; }
    yr2do0   = FREUtil::parseDate(yr2do);
    sim0f    = FREUtil::graindate( sim0, src );
    t0f      = FREUtil::graindate( yr2do0, src );
    yr2doEND = FREUtil::modifydate( yr2do0, '+1 year -1 sec' );
    tENDf    = FREUtil::graindate( yr2doEND, src );
    if ( "yr2do" eq "00010101" ) { tENDf = "0001" }

    #print "annualAV1yrfromhist yr2do yr2do yr2do0 yr2do0 yr2doEND yr2doEND tENDf tENDf\n";
    my (hDateyr) = FREUtil::splitDate(yr2do);

    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    historyfiles .= "yr2do" . ".nc.tar ";
    interval   = "1yr";
    outdirpath = "component/av/src" . "_interval";
    outdir     = "ppRootDir/outdirpath";

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    if ( 1 > maxyrs ) { maxyrs = 1; }
    zInterp    = ppcNode->findvalue('@zInterp');
    do_zInterp = 0;
    if ( "zInterp" ne "" ) { do_zInterp = 1; }
    check_cpio     = errorstr("CPIO (component src averages)");
    check_ncrcat   = errorstr("NCRCAT (component src averages)");
    check_timavg   = retryonerrorend("TIMAVG (component src averages)");
    retry_timavg   = retryonerrorstart("TIMAVG");
    check_ncatted  = errorstr("NCATTED (component src averages)");
    check_plevel   = errorstr("PLEVEL (component src averages)");
    check_ncdump   = errorstr("NCDUMP (component src averages)");
    check_fregrid  = errorstr("FREGRID (component src averages)");
    check_ncrename = errorstr("NCRENAME (component src averages)");
    check_nccopy   = errorstr("NCCOPY (component src averages)");
    csh            = setcheckpt('annualAV1yrfromhist');
    if ( "write2arch" eq "1" ) {
        csh          .= "set write2arch = 1\n";
        mkdircommand .= "outdir ";
    }
    else {
        csh .= "set write2arch = 0\n";
    }
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src averages)'
set outdir = outdir
if ( ! -e \outdir && \write2arch ) mkdir -p \outdir
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
EOF
    if (opt_z) { csh .= begin_systime(); }

    if ( "sourceGrid" eq "cubedsphere" ) {    #IF CUBIC
        tile = '.tilei';
        csh .= <<EOF;
foreach sourcefile ( `ls \histDir/hDateyr*/*.diag_source.tile*nc`)
ln -s \sourcefile .
end
foreach file ( `ls \histDir/hDateyr*/*.grid_spec.tile*.nc`)
ln -s \file .
end
set i = 1
while ( \i <= 6 )
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 \sourcefile`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
time_ncrcat ncrcat \ncrcatopt -v \vars *.diag_sourcetile.nc annualtile.nc
unset static_vars avg_vars vars
else
time_ncrcat ncrcat \ncrcatopt *.diag_sourcetile.nc annualtile.nc
endif
check_ncrcat
time_rm rm -f *.diag_sourcetile.nc
EOF

        if (do_zInterp) {
            csh .= <<EOF;
time_timavg \TIMAVG -o modellevelstile.nc annualtile.nc
retry_timavg
time_timavg \TIMAVG -o modellevelstile.nc annualtile.nc
check_timavg

EOF
            csh .= zInterpolate( zInterp, "modellevelstile.nc",
                "yr2do.diag_sourcetile.nc", caltype, variables, diag_source );

        }
        else {
            csh .= <<EOF;
time_timavg \TIMAVG -o yr2do.diag_sourcetile.nc annualtile.nc
retry_timavg
time_timavg \TIMAVG -o yr2do.diag_sourcetile.nc annualtile.nc
check_timavg
time_rm rm -f annualtile.nc

EOF
        }

        if ( "xyInterp" ne '' ) {    #CUBIC to LATLON

            fregrid_wt = '';
            if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                print
                    "       land_frac found, weighting exchange grid cell with yr2do.land_static\n"
                    if opt_v;
                fregrid_wt = "--weight_file yr2do.land_static --weight_field land_frac";
            }
            call_and_check_fregrid = call_tile_fregrid;
            call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
            call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
            call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
            compress = compress_csh( "\tempCache/outdirpath/component.tENDf.ann.nc",
                check_nccopy );
            csh .= <<EOF;
@ i ++
end

set fregrid_wt = "fregrid_wt"
set fregrid_in_date = yr2do
set fregrid_in = yr2do.diag_source
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

mv \fregrid_in.nc component.tENDf.ann.nc
time_ncatted ncatted -h -O -a filename,global,m,c,"component.tENDf.ann.nc" component.tENDf.ann.nc
check_ncatted

time_mv mv component.tENDf.ann.nc \tempCache/outdirpath/component.tENDf.ann.nc
if ( \write2arch ) then
compress
time_mv mvfile \tempCache/outdirpath/component.tENDf.ann.nc \outdir/component.tENDf.ann.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile \tempCache/outdirpath/component.tENDf.ann.nc \outdir/component.tENDf.ann.nc
    checktransfer
endif
endif

EOF

        } ## end if ( "xyInterp" ne '')
        else {    #CUBIC - no conversion
            compress
                = compress_csh( "\tempCache/outdirpath/component.tENDf.anntile.nc",
                check_nccopy );
            csh .= <<EOF;
mv yr2do.diag_sourcetile.nc component.tENDf.anntile.nc
time_ncatted ncatted -h -O -a filename,global,m,c,"component.tENDf.anntile.nc" component.tENDf.anntile.nc
check_ncatted

time_mv mv component.tENDf.anntile.nc \tempCache/outdirpath/component.tENDf.anntile.nc
if ( \write2arch ) then
compress
time_mv mvfile \tempCache/outdirpath/component.tENDf.anntile.nc \outdir/component.tENDf.anntile.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile \tempCache/outdirpath/component.tENDf.anntile.nc \outdir/component.tENDf.anntile.nc
    checktransfer
endif
endif

@ i ++
end
EOF

        } ## end else [ if ( "xyInterp" ne '')]

    } ## end if ( "sourceGrid" eq ...)
    else {    #NOT CUBIC

        csh .= <<EOF;
foreach file ( `ls \histDir/hDateyr*/*.diag_source.nc`)
ln -s \file .
end
if ( -e annual.nc ) rm -f annual.nc
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 \file`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
time_ncrcat ncrcat \ncrcatopt -v \vars *.diag_source.nc annual.nc
unset static_vars avg_vars vars
else
time_ncrcat ncrcat \ncrcatopt *.diag_source.nc annual.nc
endif
check_ncrcat
time_rm rm -f *.diag_source.nc
EOF

        if (do_zInterp) {
            csh .= <<EOF;
time_timavg \TIMAVG -o modellevels.nc annual.nc
retry_timavg
time_timavg \TIMAVG -o modellevels.nc annual.nc
check_timavg

EOF
            csh .= zInterpolate( zInterp, 'modellevels.nc', "component.tENDf.ann.nc",
                caltype, variables, component );

        }
        else {
            csh .= <<EOF;
time_timavg \TIMAVG -o component.tENDf.ann.nc annual.nc
retry_timavg
time_timavg \TIMAVG -o component.tENDf.ann.nc annual.nc
check_timavg

EOF
        }

        # convert latlon/tripolar to latlon
        if ( "xyInterp" ne '' ) {
            fregrid_wt = '';
            if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                print
                    "       land_frac found, weighting exchange grid cell with yr2do.land_static\n"
                    if opt_v;
                fregrid_wt = "--weight_file yr2do.land_static --weight_field land_frac";
            }
            call_and_check_fregrid = call_fregrid;
            call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
            call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
            call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
            csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = yr2do
set fregrid_in = annual
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid
mv \fregrid_in.nc component.tENDf.ann.nc

EOF
        } ## end if ( "xyInterp" ne '')

        compress
            = compress_csh( "\tempCache/outdirpath/component.tENDf.ann.nc", check_nccopy );

        csh .= <<EOF;
time_ncatted ncatted -h -O -a filename,global,m,c,"component.tENDf.ann.nc" component.tENDf.ann.nc
check_ncatted
time_mv mv component.tENDf.ann.nc \tempCache/outdirpath/component.tENDf.ann.nc
if ( \write2arch ) then
compress
time_mv mvfile \tempCache/outdirpath/component.tENDf.ann.nc \outdir/component.tENDf.ann.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile \tempCache/outdirpath/component.tENDf.ann.nc \outdir/component.tENDf.ann.nc
    checktransfer
endif
endif
time_rm rm -f annual.nc

EOF

    } ## end else [ if ( "sourceGrid" eq ...)]
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);
    return csh;
} ## end sub annualAV1yrfromhist

def annualAVxyrfromann(taNode, sim0, ppcNode, annavnodes, annCalcInterval):
"""TIMEAVERAGES - ANNUAL XYR"""
# frepp.pl l.5212

    #taNode = _[0] ;
    sim0            = _[1];
    ppcNode         = _[2];
    annavnodes      = _[3];
    annCalcInterval = _[4];

    src         = 'annual';
    interval    = _[0]->findvalue('@interval');
    chunkLength = _[0]->findvalue('@chunkLength');
    sim0f       = FREUtil::graindate( sim0, src );
    t0f         = FREUtil::graindate( t0, src );
    tENDf       = FREUtil::graindate( tEND, src );
    outdirpath  = "component/av/src" . "_interval";
    outdir      = "ppRootDir/outdirpath";
    tmp         = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar    = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int         = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }
    first = FREUtil::padzeros( t0f - int + 1 );

    #check for missing files
    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = _[0]->parentNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode
            = _[0]->parentNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    csh = '';

    #always do 1yr ann av calculation, but only archive if annCalcInterval not 1yr
    if ( "annCalcInterval" ne "1yr" and not annavnodes ) { annavnodes = 0; }
    newnode = XML::LibXML::Element->new('timeAverage');
    newnode->setAttribute( 'source',   'annual' );
    newnode->setAttribute( 'interval', '1yr' );
    ppcNode->appendChild(newnode);
    annNode
        = ppcNode->findnodes('timeAverage[@source="annual" and @interval="1yr"]')->get_node(1);
    foreach i ( 0 .. ( int - 1 ) ) {
        annyr = userstartyear;
        annyr -= i;
        annyr = FREUtil::padzeros(annyr) . "0101";

        #print "\nNOTE: Calling annualAV1yrfromhist( annNode, sim0, annavnodes, annyr )\n\n";
        csh .= annualAV1yrfromhist( annNode, sim0, annavnodes, annyr );
        last if "annCalcInterval" eq "1yr";
    }

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    csh .= setcheckpt("annualAVxyrfromann_interval");
    csh .= <<EOF;

#####################################
echo 'timeAverage (component src interval averages)'
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
if ( ! -e \tempCache/component/av/annual_1yr ) mkdir -p \tempCache/component/av/annual_1yr
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath
set anndir = ppRootDir/component/av/annual_1yr
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
cd \tempCache/component/av/annual_1yr
EOF
    if (opt_z) { csh .= begin_systime(); }

    endinterval = FREUtil::padzeros( first + int - 1 );
    until ( endinterval > tENDf ) {

        #print "in tA, endinterval(endinterval) tENDf(tENDf)\n";
        filelist = "";
        getlist  = "";
        foreach year ( first .. endinterval ) {
            year = FREUtil::padzeros(year);
            if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {    # CUBIC
                foreach i ( 1 .. 6 ) {
                    filelist .= "\anndir/component.year.ann.tilei.nc ";
                    getlist  .= "component.year.ann.tilei.nc ";
                }
            }
            else {                                                           # LATLON
                filelist .= "\anndir/component.year.ann.nc ";
                getlist  .= "component.year.ann.nc ";
            }
        }
        first_file    = (split ' ', getlist)[0];
        check_ncrcat  = errorstr("NCRCAT (component src interval averages)");
        check_timavg  = retryonerrorend("TIMAVG (component src interval averages)");
        retry_timavg  = retryonerrorstart("TIMAVG");
        check_ncatted = errorstr("NCATTED (component src interval averages)");
        check_dmget   = errorstr("DMGET (component src interval averages)");
        check_nccopy  = errorstr("NCCOPY (component src interval averages)");

        if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {    # CUBIC
            tile = '.tilei';
            compress
                = compress_csh(
                "\tempCache/outdirpath/component.first-endinterval.anntile.nc",
                check_nccopy );
            csh .= <<EOF;
foreach file (filelist)
set f = \file:t
if ( ! -f \f ) then
    time_cp cp \file .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp \file .
        checktransfer
    endif
endif
end
set i = 1
while ( \i <= 6 )
if ( -e xyears.nc ) rm -f xyears.nc
set filelist = ""
foreach file (getlist)
    if (\file =~ \*tile\*) set filelist = ( \filelist \file )
end
if ( "variables" != '' ) then
    set static_vars = (`\NCVARS -s0123 \file`)
    set static_vars = `echo \static_vars |sed 's/ /,/g'`
    set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
    set vars = ("variables","\static_vars","\avg_vars")
    time_ncrcat ncrcat \ncrcatopt -v \vars \filelist \work/xyears.nc
    unset static_vars avg_vars vars
else
    time_ncrcat ncrcat \ncrcatopt \filelist \work/xyears.nc
endif
check_ncrcat
time_timavg \TIMAVG -o \tempCache/outdirpath/component.first-endinterval.anntile.nc \work/xyears.nc
retry_timavg
    time_timavg \TIMAVG -o \tempCache/outdirpath/component.first-endinterval.anntile.nc \work/xyears.nc
check_timavg
time_ncatted ncatted -h -O -a filename,global,m,c,"component.first-endinterval.anntile.nc" \tempCache/outdirpath/component.first-endinterval.anntile.nc
check_ncatted
compress
time_mv mvfile \tempCache/outdirpath/component.first-endinterval.anntile.nc \outdir/component.first-endinterval.anntile.nc
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile \tempCache/outdirpath/component.first-endinterval.anntile.nc \outdir/component.first-endinterval.anntile.nc
    checktransfer
endif
time_rm rm -f xyears.nc
@ i++
end
EOF

        } ## end if ( "sourceGrid" eq ...)
        else {    # LATLON
            compress
                = compress_csh( "\tempCache/outdirpath/component.first-endinterval.ann.nc",
                check_nccopy );
            csh .= <<EOF;
foreach file (filelist)
set f = \file:t
if ( ! -f \f ) then
    time_cp cp \file .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp \file .
        checktransfer
    endif
endif
end
if ( -e xyears.nc ) rm -f xyears.nc
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 first_file`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
time_ncrcat ncrcat \ncrcatopt -v \vars getlist \work/xyears.nc
unset static_vars avg_vars vars
else
time_ncrcat ncrcat \ncrcatopt getlist \work/xyears.nc
endif
check_ncrcat
time_timavg \TIMAVG -o \tempCache/outdirpath/component.first-endinterval.ann.nc \work/xyears.nc
retry_timavg
time_timavg \TIMAVG -o \tempCache/outdirpath/component.first-endinterval.ann.nc \work/xyears.nc
check_timavg
time_ncatted ncatted -h -O -a filename,global,m,c,"component.first-endinterval.ann.nc" \tempCache/outdirpath/component.first-endinterval.ann.nc
check_ncatted
compress
time_mv mvfile \tempCache/outdirpath/component.first-endinterval.ann.nc \outdir/component.first-endinterval.ann.nc
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile \tempCache/outdirpath/component.first-endinterval.ann.nc \outdir/component.first-endinterval.ann.nc
checktransfer
endif
time_rm rm -f \work/xyears.nc
EOF
        } ## end else [ if ( "sourceGrid" eq ...)]

        first       = FREUtil::padzeros( endinterval + 1 );
        endinterval = FREUtil::padzeros( first + int - 1 );
    } ## end until ( endinterval > tENDf)
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);
    return csh;
} ## end sub annualAVxyrfromann

def monthlyTSfromdailyTS(tsNode, sim0, startofrun):
"""TIMESERIES - monthly from daily ts"""
# frepp.pl l.5425
        #tsNode = _[0] ;
    sim0        = _[1];
    startofrun  = _[2];
    ppcNode     = _[0]->parentNode;
    avgatt      = _[0]->findvalue('@averageOf');
    freq        = _[0]->findvalue('@freq');
    t0f         = FREUtil::graindate( t0, freq );
    tENDf       = FREUtil::graindate( tEND, freq );
    chunkLength = _[0]->findvalue('@chunkLength');
    if ( "chunkLength" eq "" ) {
        print STDERR
            "ERROR: Cannot create component freq timeSeries unless you set a chunkLength.\n";
        mailuser("Cannot create component freq timeSeries unless you set a chunkLength.");
        return "";
    }
    outdir = "ppRootDir/component/ts/freq/chunkLength";
    chunkLength =~ s/yr//;
    yrsSoFar = &Delta_Format( FREUtil::dateCalc( sim0, t0 ), 0, "%yd" );
    mod = ( yrsSoFar + 1 ) % chunkLength;
    if ( mod != 0 ) { return ""; }    #don't do any calculations until a chunk is ready to go.
    mkdircommand .= "outdir ";
    if ( chunkLength > maxyrs ) { maxyrs = chunkLength; }
    indir    = "ppRootDir/component/ts/avgatt/chunkLength" . "yr";
    hDateyr  = userstartyear;
    my @hDates   = ( FREUtil::padzeros( hDateyr - chunkLength + 1 ) .. "hDateyr" );
    in_start = FREUtil::graindate( FREUtil::modifydate( tEND, "-chunkLength yr +1 sec" ),
        avgatt );
    in_end = FREUtil::graindate( tEND, avgatt );
    out_start
        = FREUtil::graindate( FREUtil::modifydate( tEND, "-chunkLength yr +1 sec" ), freq );
    out_end = FREUtil::graindate( tEND, freq );
    #
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    if ( "variables" eq "" ) {
        variables = dtvars{"all_component"};
        variables =~ s/,/ /g;
    }
    dmgetvars = variables;
    if ( "variables" ne "" ) {
        print STDERR "           from xml, vars are 'variables'\n" if opt_v;
        dmgetvars =~ s/ /.nc \in./g;
        dmgetvars =~ s/^/time_dmget dmget -d indir \in./;
        dmgetvars =~ s//.nc/;
    }

    numtimelevels = gettimelevels( freq, chunkLength );
    check_ncrcat  = errorstr("NCRCAT (component freq ts calculated from avgatt ts)");
    check_cpio    = errorstr("CPIO (component freq ts calculated from avgatt ts)");
    check_dmget   = errorstr("DMGET (component freq ts calculated from avgatt ts)");
    check_ncatted = errorstr("NCATTED (component freq ts calculated from avgatt ts)");
    check_ncdump  = errorstr("NCDUMP (component freq ts calculated from avgatt ts)");
    check_timavg
        = retryonerrorend("TIMAVG (component freq ts calculated from avgatt ts)");
    retry_timavg = retryonerrorstart("TIMAVG");
    check_ncks   = errorstr("NCKS (component freq ts calculated from avgatt ts)");
    check_ncap   = errorstr("NCAP (component freq ts calculated from avgatt ts)");
    check_nccopy = errorstr("NCCOPY (component freq ts calculated from avgatt ts)");
    check_levels = '';

    if ( "caltype" eq "NOLEAP" or "caltype" eq "noleap" ) {
        check_levels
            = errorstr(
            "WRONG NUMBER OF TIME LEVELS (contains \length, should be numtimelevels) IN \outdir/\out.\var.nc"
            );
    }
    csh = setcheckpt('monthlyTSfromdailyTS');
    csh .= <<EOF;
#####################################
echo 'timeSeries (component freq calculated from avgatt TS)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir

EOF
    if (opt_z) { csh .= begin_systime(); }
    compress = compress_csh( "\out.\var.nc", check_nccopy );
    csh .= <<EOF;
set in = 'component.in_start-in_end'
set out = 'component.out_start-out_end'

dmgetvars
foreach var ( variables )
if ( ! -e indir/\in.\var.nc ) then
    time_dmget dmget indir/\in.day.nc.cpio
    time_cp cp indir/\in.day.nc.cpio .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp indir/\in.day.nc.cpio .
        checktransfer
    endif
    time_uncpio uncpio -ivI \in.day.nc.cpio \in.\var.nc
    check_cpio
    time_dmput dmput "indir/\in.day.nc.cpio"
else
    time_cp cp indir/\in.\var.nc .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp indir/\in.\var.nc .
        checktransfer
    endif
endif

set d1 = 0
set d2 = 0

foreach y1 ( @hDates )
    set monthi = 0

    foreach days ( 31 28 31 30 31 30 31 31 30 31 30 31 )
    @ d1 = \d2 + 1
    @ d2 = \d2 + \days

    @ monthi ++
    set m1 = `printf '%02i' \monthi`

    set t = `ncdump -h \in.\var.nc | grep -i '.*=.*unlimited.*currently' | awk '{print \1}'`
    time_ncks ncks \ncksopt -d \t,\d1,\d2 \in.\var.nc \y1\m1.\var.daily.nc
    check_ncks
    time_timavg \TIMAVG -o \y1\m1.\var.nc \y1\m1.\var.daily.nc
    retry_timavg
        time_timavg \TIMAVG -o \y1\m1.\var.nc \y1\m1.\var.daily.nc
    check_timavg
    time_rm rm -f \y1\m1.\var.daily.nc
    end
end

time_rm rm -f \in.\var.nc
if ( -e \out.\var.nc ) rm -f \out.\var.nc
time_ncrcat ncrcat \ncrcatopt *??.\var.nc \out.\var.nc
check_ncrcat
time_ncatted ncatted -h -O -a filename,global,m,c,"\out.\var.nc" \out.\var.nc
check_ncatted
compress
set tmpstring = `ncdump -h \out.\var.nc | grep UNLIMITED`
time_mv mvfile \out.\var.nc outdir/
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile \out.\var.nc outdir/
    checktransfer
endif
time_rm rm \out.\var.nc
time_rm rm -f *??.\var.nc
@ len = \#tmpstring - 1
set length = `echo \tmpstring[\len] | cut -c2-`
test \length = numtimelevels
check_levels
end

EOF

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub monthlyTSfromdailyTS

def directTS(tsNode, sim0, startofrun):
"""TIMESERIES - HOURLY, DAILY, MONTHLY, ANNUAL"""
# frepp.pl l.5585
        #tsNode = _[0] ;
    sim0       = _[1];
    startofrun = _[2];
    ppcNode    = _[0]->parentNode;
    avgatt     = _[0]->findvalue('@averageOf');
    freq       = _[0]->findvalue('@freq');
    source     = _[0]->findvalue('@source');
    if ( "source" eq "" ) { source = ppcNode->findvalue('@source'); }
    if ( "avgatt" ne "" ) {
        if ( "avgatt" eq "daily" ) {
            return monthlyTSfromdailyTS( _[0], sim0, startofrun );
        }
        else {
            print STDERR
                "WARNING: freq TS calculated from avgatt TS is not supported. Skipping.\n";
            return "";
        }
    }
    sim0f = FREUtil::graindate( sim0, freq );
    t0f   = FREUtil::graindate( t0,   freq );
    tENDf = FREUtil::graindate( tEND, freq );
    chunkLength = _[0]->findvalue('@chunkLength');
    chunkstr    = chunkLength;
    if ( "chunkLength" eq "" ) {
        print STDERR
            "ERROR: Cannot create component freq timeSeries unless you set a chunkLength.\n";
        mailuser("Cannot create component freq timeSeries unless you set a chunkLength.");
        return "";
    }
    outdirpath = "component/ts/freq/chunkLength";
    outdir     = "ppRootDir/outdirpath";

    iunit;
    if ( chunkLength =~ /(\d*)(?:y|yr|years?)/i ) {
        iunit       = "years";
        chunkLength = 1;
    }
    elsif ( chunkLength =~ /(\d*)(?:mo|mon|months)/i ) {
        iunit       = "months";
        chunkLength = 1;
    }
    else {
        print STDERR
            "ERROR: Cannot create component freq timeSeries because can't parse chunkLength=chunkLength.\n";
        mailuser(
            "ERROR: Cannot create component freq timeSeries because can't parse chunkLength=chunkLength."
        );
        return;
    }

    yrsSoFar = &Delta_Format( FREUtil::dateCalc( sim0, t0 ), 0, "%yd" );
    mod = ( yrsSoFar + 1 ) % chunkLength;
    if ( mod != 0 and "iunit" eq "years" ) {
        return "";
    }    #don't do any calculations until a chunk is ready to go.
    mkdircommand .= "outdir ";
    if ( chunkLength > maxyrs ) { maxyrs = chunkLength; }

    hDateyr = userstartyear;
    hDatemo = substr( userstartmo, 2, 2 );
    my @hDates  = ();
    start   = '';
    if ( "iunit" eq "years" ) {
        @hDates = ( FREUtil::padzeros( hDateyr - chunkLength + 1 ) .. "hDateyr" );
        start
            = FREUtil::graindate( FREUtil::modifydate( tEND, "-chunkLength iunit +1 sec" ),
            freq );
    }
    else {
        @hDates = ("hDateyr");

    #the following gives wrong results, bug in Date:Manip
    #start = FREUtil::graindate(FREUtil::modifydate(tEND,"-chunkLength months +1 sec"),freq);
    #the following is a cheap trick to fix the problem
        start = FREUtil::modifydate( tEND, "-chunkLength months + 5 days" );
        start = FREUtil::graindate( start, "mon" );
        start .= "01";
        start = FREUtil::graindate( start, freq );
    }

    #print "start start, tENDf tENDf, chunkLength chunkLength, iunit iunit\n";

    foreach d (@hDates) { historyfiles .= "dhDatemo" . "01.nc.tar "; }

    #determine whether to interpolate z levels
    zInterp    = ppcNode->findvalue('@zInterp');
    do_zInterp = 0;
    if ( "zInterp" ne "" ) { do_zInterp = 1; }

    #get variables
    uservariables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    uservariables =~ s/ /,/g;
    availablevars = dtvars{"all_source"};
    availablevars =~ s/^/,/g;
    availablevars =~ s//,/g;
    availablevars .= 'hght,slp,' if do_zInterp;
    variables = '';
    foreach v ( split( ',', uservariables ) ) {

        if ( availablevars =~ /,v,/ ) {
            variables = "variables v";
        }
        else {
            variables = "variables v";
            print STDERR
                "WARNING: freq component post-processing requested for variable v but it does not exist in the source diag table.\n"
                ; #Don't actually skip the variable until refineDiag supported as available variables.
        }
    }
    variables =~ s/^ //g;
    variables =~ s/ /,/g;
    if ( "variables" ne "" ) {
        print STDERR "        from xml, vars are 'variables'\n" if opt_v;
    }

    numtimelevels = gettimelevels( freq, chunkLength );

    #check_cpio = errorstr("CPIO (component freq ts from source)");
    check_cpio_msg    = "CPIO (component freq ts from source)";
    check_plevel      = errorstr("PLEVEL (component freq ts from source)");
    check_splitncvars = errorstr("SPLITNCVARS (component freq ts from source)");
    check_ncrcat      = errorstr("NCRCAT (component freq ts from source)");
    check_ncatted     = errorstr("NCATTED (component freq ts from source)");
    check_ncdump      = errorstr("NCDUMP (component freq ts from source)");
    check_timavg      = retryonerrorend("TIMAVG (component freq ts from source)");
    retry_timavg      = retryonerrorstart("TIMAVG");
    check_ncks        = errorstr("NCKS (component freq ts from source)");
    check_filesexist
        = errorstr("NO USABLE VARIABLES EXIST (component freq ts from source)");
    check_fregrid  = errorstr("FREGRID (component freq ts from source)");
    check_ncrename = errorstr("NCRENAME (component freq ts from source)");
    check_nccopy   = errorstr("NCCOPY (component freq ts from source)");
    check_levels   = '';

    if ( "caltype" eq "NOLEAP" or "caltype" eq "noleap" ) {
        check_levels
            = errorstr(
            "WRONG NUMBER OF TIME LEVELS (contains \length, should be numtimelevels) IN \outdir/component.start-tENDf.\file"
            );
    }
    csh = setcheckpt( "directTS_freq" . "_chunkstr" );
    csh .= <<EOF;

#####################################
echo 'timeSeries (component freq from source)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath

EOF
    if (opt_z) { csh .= begin_systime(); }

    tmp_month = '01';
    tmpsrcstr = "source.nc";
    if ( "sourceGrid" eq 'cubedsphere' ) { tmpsrcstr = "source.tile*.nc"; }

    csh .= <<EOF;
foreach hDate ( @hDates )
    set nhistfiles = 0
    foreach file ( `ls \histDir/\hDate*/*.tmpsrcstr`)
    ln -s \file .
    @ nhistfiles ++
    end
    foreach file ( `ls \histDir/\hDate*/*.grid_spec.tile*.nc`)
    ln -s \file .
    end
    if ( \nhistfiles == 0 ) then
        echo 'ERROR: No history files matching \hDate*/*.tmpsrcstr'
    endif
end
mkdir -p byVar
EOF

    if (do_zInterp) {
        if ( "sourceGrid" eq 'cubedsphere' ) {

            # cat, call plevel, on tiles
            csh .= <<EOF;
if ( -e modellevels.nc ) rm -f modellevels*.nc
set i = 1
while ( \i <= 6 )
time_ncrcat ncrcat \ncrcatopt *.source.tile\i.nc hDate.modellevels.tile\i.nc
check_ncrcat
@ i ++
end
EOF
            foreach i ( 1 .. 6 ) {
                csh .= zInterpolate( zInterp, "hDate.modellevels.tilei.nc",
                    "hDate.source.tilei.nc", caltype, variables, source );
            }

            # convert tiles to latlon
            if ( "xyInterp" ne '' ) {

                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_tile_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                csh .= <<EOF;

set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = hDate.source
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

mv \fregrid_in.nc all.nc

EOF
            } ## end if ( "xyInterp" ne '')
        } ## end if ( "sourceGrid" eq ...)
        else {    # not cubic
            csh .= <<EOF;
if ( -e modellevels.nc ) rm -f modellevels.nc
time_ncrcat ncrcat \ncrcatopt *.source.nc modellevels.nc
check_ncrcat
time_rm rm -f *.source.nc
EOF
            csh .= zInterpolate( zInterp, 'modellevels.nc', 'all.nc', caltype, variables,
                "all" );

            # convert latlon/tripolar to latlon
            if ( "xyInterp" ne '' ) {
                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = 'all'
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st23
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

EOF
            } ## end if ( "xyInterp" ne '')
        } ## end else [ if ( "sourceGrid" eq ...)]
    } ## end if (do_zInterp)
    else {    # no zInterpolation
        if ( "sourceGrid" eq 'cubedsphere' ) {
            csh .= <<EOF;
#if ( -e all.nc ) rm -f all.nc
set i = 1
while ( \i <= 6 )
time_ncrcat ncrcat \ncrcatopt *.source.tile\i.nc hDate.source.tile\i.nc
check_ncrcat
@ i ++
end
EOF

            # convert tiles to latlon
            if ( "xyInterp" ne '' ) {

                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_tile_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = hDate.source
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

mv \fregrid_in.nc all.nc

EOF
            } ## end if ( "xyInterp" ne '')
        } ## end if ( "sourceGrid" eq ...)
        else {    #not cubic, just ncrcat
            csh .= <<EOF;
if ( -e all.nc ) rm -f all.nc
time_ncrcat ncrcat \ncrcatopt *.source.nc all.nc
check_ncrcat
time_rm rm -f *.source.nc

EOF

            # convert latlon/tripolar to latlon
            if ( "xyInterp" ne '' ) {

                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = 'all'
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st23
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

EOF
            } ## end if ( "xyInterp" ne '')
        } ## end else [ if ( "sourceGrid" eq ...)]
    } ## end else [ if (do_zInterp) ]
    if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {
        csh .= "set filestosplit = ( `ls -1 | egrep \"hDate.source.tile..nc\"` )\n";
    }
    else {
        csh .= "set filestosplit = ( all.nc )\n";
    }

    # make sure file has bounds, splitncvars, adjust output, send to archive
    variablesopt = '';
    variablesopt = "-v variables" if "variables" ne '';
    compress = compress_csh( "\file", check_nccopy );
    csh .= <<EOF;
foreach filetosplit ( \filestosplit )

# Determine if fields average_T1 and ( *_bounds or *_bnds ) exist in the
# netCDF file.  If not, add in time_bounds.
if ( `ncdump -h \filetosplit | grep -c " average_T1("` == 1 && `ncdump -h \filetosplit | grep -c "_b\\(ou\\)\\?nds("` == 0) then
ncdump -v average_T1,average_T2 \filetosplit | /home/fms/bin/addbounds.pl | ncgen -o tmp.nc
set taxis = `ncdump -h \filetosplit | grep -i '.*=.*unlimited.*currently' | awk '{print \1}'`
time_ncks ncks \ncksopt -C -A -v \{taxis}_bounds tmp.nc \filetosplit
check_ncks
time_ncatted ncatted -h -O -a bounds,\taxis,c,c,"\{taxis}_bounds" \filetosplit
check_ncatted
endif

time_splitncvars \SPLITNCVARS -o byVar variablesopt \filetosplit
check_splitncvars

cd byVar
test `ls | wc -l` -gt 0
check_filesexist
if ( `ls | wc -l` > 0 ) then
foreach file ( *.nc )
    set label = "\file:r.nc"
    time_ncatted ncatted -h -O -a filename,global,m,c,"component.start-tENDf.\label" \file
    check_ncatted
    set tmpstring = `ncdump -h \file | grep UNLIMITED`
    @ len = \#tmpstring - 1
    set length = `echo \tmpstring[\len] | cut -c2-`
    test \length = numtimelevels
    check_levels
    compress
    time_mv mvfile \file \outdir/component.start-tENDf.\label
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_mv mvfile \file \outdir/component.start-tENDf.\label
        checktransfer
    endif
    time_mv mv \file \tempCache/outdirpath/component.start-tENDf.\label
end
cd \work
time_rm rm -rf byVar

end
EOF

    #cpio the timeseries
    if (aggregateTS) {
        abbrev = FREUtil::timeabbrev(freq);
        cpioTS = '';
        if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" ne '' ) {
            cpioTS
                = createcpio( "\tempCache/outdirpath", outdir,
                "component.start-tENDf.tile?",
                abbrev, 1 );
        }
        else {
            cpioTS
                = createcpio( "\tempCache/outdirpath", outdir, "component.start-tENDf",
                abbrev, 1 );
        }
        if   ( "freq" eq "monthly" ) { cpiomonTS .= cpioTS; }
        else                          { csh       .= cpioTS; }
    }

    csh .= "\nendif\n";

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub directTS

def monthlyAVfromav(taNode, sim0, subint):
"""TIMEAVERAGES - MONTHLY"""
# frepp.pl l.6021
    #taNode = _[0] ;
    sim0    = _[1];
    subint  = _[2];
    ppcNode = _[0]->parentNode;

    src         = 'monthly';
    interval    = _[0]->findvalue('@interval');
    outdir      = "ppRootDir/component/av/src" . "_interval";
    srcdir      = "ppRootDir/component/av/src" . "_subint" . "yr";
    chunkLength = _[0]->findvalue('@chunkLength');

    tmp = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    #check for missing files
    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    sim0f = FREUtil::graindate( sim0, src );
    t0f   = FREUtil::graindate( t0,   src );
    tENDf = FREUtil::graindate( tEND, src );
    end   = FREUtil::graindate( tEND, 'year' );
    if ( end < int ) { return ""; }    #nothing to do yet
    start = FREUtil::padzeros( end - int + 1 );

    substart = start;
    subend   = FREUtil::padzeros( start + subint - 1 );
    filelist = "";
    until ( subend > end ) {
        if ( substart == subend ) { filelist .= "component.substart.\monthf\tile.nc "; }
        else { filelist .= "component.substart-subend.\monthf\tile.nc "; }
        substart = FREUtil::padzeros( substart + subint );
        subend   = FREUtil::padzeros( subend + subint );
    }
    getlist = filelist;
    getlist =~ s/\monthf\tile/*/g;
    first_file = (split ' ', filelist)[0];

    #   print "filelist is filelist\n";
    #   print "getlist is getlist\n";
    tilestart = "set tile = ''";
    tileend   = '';
    if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {
        tilestart = "set tileN=1\nwhile (\tileN <= 6)\nset tile = .tile\tileN";
        tileend   = "@ tileN++\nend";
    }

    check_ncatted = errorstr("NCATTED (component src interval averages)");
    check_ncrcat  = errorstr("NCRCAT (component src interval averages)");
    check_timavg  = retryonerrorend("TIMAVG (component src interval averages)");
    retry_timavg  = retryonerrorstart("TIMAVG");
    check_dmget   = errorstr("DMGET (component src interval averages)");
    check_nccopy  = errorstr("NCCOPY (component src interval averages)");
    csh           = setcheckpt("monthlyAVfromav_interval");
    compress = compress_csh( "component.start-end.\monthf\tile.nc", check_nccopy );
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src interval averages from subint yr averages)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir

EOF
    if (opt_z) { csh .= begin_systime(); }
    csh .= <<EOF;

cd srcdir
time_dmget dmget "getlist"
set files = (`ls getlist`)
cd \work
foreach file (\files)
time_cp cp srcdir/\file .
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_cp cp srcdir/\file .
    checktransfer
endif
end

tilestart
#get static variables from first file (e.g. 1850.01.nc) from list_ncvars
#hard-code average vars and time_bnds which are skipped in list_ncvars but needed for timavg.csh
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 first_file`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
unset static_vars avg_vars
endif
set month = 1
while (\month <= 12)
set monthf = `echo \month | sed 's/.*/0&/;s/.\\(..\\)/\\1/'`
if ( -e month.nc ) rm -f month.nc
if ( "variables" != '' ) then
    time_ncrcat ncrcat \ncrcatopt -v \vars filelist month.nc
else
    time_ncrcat ncrcat \ncrcatopt filelist month.nc
endif
check_ncrcat
time_timavg \TIMAVG -o component.start-end.\monthf\tile.nc month.nc
retry_timavg
time_timavg \TIMAVG -o component.start-end.\monthf\tile.nc month.nc
check_timavg
time_ncatted ncatted -h -O -a filename,global,m,c,"component.start-end.\monthf\tile.nc" component.start-end.\monthf\tile.nc
check_ncatted
compress
time_mv mvfile component.start-end.\monthf\tile.nc \outdir/
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_mv mvfile component.start-end.\monthf\tile.nc \outdir/
    checktransfer
endif
time_rm rm component.start-end.\monthf\tile.nc
time_dmput dmput \outdir/component.start-end.\monthf\tile.nc
time_rm rm -f month.nc
@ month++
end
tileend

EOF

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub monthlyAVfromav


def annualAVfromav(taNode, sim0, subint):
"""TIMEAVERAGES - ANNUAL XYR"""
# frepp.pl l.6168
        #taNode = _[0] ;
    sim0   = _[1];
    subint = _[2];

    src         = 'annual';
    interval    = _[0]->findvalue('@interval');
    chunkLength = _[0]->findvalue('@chunkLength');
    sim0f       = FREUtil::graindate( sim0, src );
    t0f         = FREUtil::graindate( t0, src );
    tENDf       = FREUtil::graindate( tEND, src );
    outdir      = "ppRootDir/component/av/src" . "_interval";
    srcdir      = "ppRootDir/component/av/src" . "_subint" . "yr";
    tmp         = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar    = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int         = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }
    first = FREUtil::padzeros( t0f - int + 1 );

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    #check for missing files
    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = _[0]->parentNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode
            = _[0]->parentNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    csh = setcheckpt("annualAVfromav_interval");
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src interval averages from subint yr averages)'
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
EOF
    if (opt_z) { csh .= begin_systime(); }

    endinterval = FREUtil::padzeros( first + int - 1 );
    until ( endinterval > tENDf ) {

        #print "in tA, endinterval(endinterval) tENDf(tENDf)\n";
        substart = first;
        subend   = FREUtil::padzeros( first + subint - 1 );
        filelist = "";
        until ( subend > tENDf ) {
            filelist .= "component.substart-subend.ann\tile.nc ";
            substart = FREUtil::padzeros( substart + subint );
            subend   = FREUtil::padzeros( subend + subint );
        }

        tilestart = "set tile = ''";
        tileend   = '';
        if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {
            tilestart = "set tileN=1\nwhile (\tileN <= 6)\nset tile = .tile\tileN";
            tileend   = "@ tileN++\nend";
        }

        check_ncrcat  = errorstr("NCRCAT (component src interval averages)");
        check_timavg  = retryonerrorend("TIMAVG (component src interval averages)");
        retry_timavg  = retryonerrorstart("TIMAVG");
        check_ncatted = errorstr("NCATTED (component src interval averages)");
        check_dmget   = errorstr("DMGET (component src interval averages)");
        check_nccopy  = errorstr("NCCOPY (component src interval averages)");
        compress
            = compress_csh( "component.first-endinterval.ann\tile.nc", check_nccopy );
        csh .= <<EOF;
cd srcdir
tilestart

time_dmget dmget "filelist"

cd \work
foreach file (filelist)
time_cp cp srcdir/\file .
if ( \status ) then
    echo "WARNING: data transfer failure, retrying..."
    time_cp cp srcdir/\file .
    checktransfer
endif
end

if ( -e xyears.nc ) rm -f xyears.nc
if ( "variables" != '' ) then
set static_vars = (`\NCVARS -s0123 \file`)
set static_vars = `echo \static_vars |sed 's/ /,/g'`
set avg_vars = (average_T1,average_T2,average_DT,time_bnds)
set vars = ("variables","\static_vars","\avg_vars")
time_ncrcat ncrcat \ncrcatopt -v \vars filelist xyears.nc
unset static_vars avg_vars vars
else
time_ncrcat ncrcat \ncrcatopt filelist xyears.nc
endif
check_ncrcat
time_timavg \TIMAVG -o component.first-endinterval.ann\tile.nc xyears.nc
retry_timavg
time_timavg \TIMAVG -o component.first-endinterval.ann\tile.nc xyears.nc
check_timavg
time_ncatted ncatted -h -O -a filename,global,m,c,"component.first-endinterval.ann\tile.nc" component.first-endinterval.ann\tile.nc
check_ncatted
compress
time_mv mvfile component.first-endinterval.ann\tile.nc \outdir/component.first-endinterval.ann\tile.nc
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.first-endinterval.ann\tile.nc \outdir/component.first-endinterval.ann\tile.nc
checktransfer
endif
time_rm rm component.first-endinterval.ann\tile.nc
time_rm rm -f xyears.nc

tileend

EOF
        first       = FREUtil::padzeros( endinterval + 1 );
        endinterval = FREUtil::padzeros( first + int - 1 );
    } ## end until ( endinterval > tENDf)
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);
    return csh;
} ## end sub annualAVfromav

def staticvars(diag_source, ptmpDir, tmphistdir, refinedir):
"""Create static variables file."""
# frepp.pl 6302
    diag_source = _[0];
    ptmpDir     = _[1];
    tmphistdir  = _[2];
    refinedir   = _[3];

    #note: checking ncks: gives error messages when it shouldn't?
    check_ncks        = errorstr("NCKS (component static variables)");
    check_cpio        = errorstr("CPIO (component static variables)");
    check_ncatted     = errorstr("NCATTED (component static variables)");
    check_splitncvars = errorstr("SPLITNCVARS (component static variables)");
    check_fregrid     = errorstr("FREGRID (component static variables)");
    check_ncrename    = errorstr("NCRENAME (component static variables)");
    check_ncatted     = errorstr("NCATTED (component static variables)");
    check_nccopy      = errorstr("NCCOPY (component static variables)");
    compress = compress_csh( "ppRootDir/component/component.static.nc", check_nccopy );

    historyfiles .= "hDate" . ".nc.tar ";

    csh = setcheckpt('staticvars');
    csh .= <<EOF;
#####################################
if ( ! -e ppRootDir/component/component.static.nc ) then
echo 'static variables (component)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
mkdir -p ppRootDir/component
time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*diag_source\\*
if ( \status ) then
    echo "WARNING: hsmget reported failure, retrying..."
    time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*diag_source\\*
    checktransfer
endif
# Get files listed as associated_files
foreach file ( tmphistdir/hDate.nc/*diag_source* )
    # Get a list of all associated_files
    set assocFiles = `ncdump -h \file | grepAssocFiles`
    foreach assocFile ( \assocFiles )
        time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*\{assocFile:r}.\\*
    end
end

time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*diag_source\\*
if ( \status ) then
    echo "WARNING: hsmget reported failure, retrying..."
    time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*diag_source\\*
checktransfer
endif
# Get files listed as associated_files
foreach file ( tmphistdir/hDate.nc/*diag_source* )
    # Get a list of all associated_files
    set assocFiles = `ncdump -h \file | grepAssocFiles`
    foreach assocFile ( \assocFiles )
        time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*\{assocFile:r}.\\*
    end
end

foreach file ( `ls \histDir/*/*nc`)
    ln -s \file .
end
set output_files = ( )
EOF

    if ( "sourceGrid" eq 'cubedsphere' and "xyInterp" eq '' ) {

        #DATA LEFT ON CUBED SPHERE GRID
        csh = setcheckpt('staticvars');
        csh .= <<EOF;
#####################################
if ( ! -e ppRootDir/component/component.static.tile6.nc ) then
echo 'static variables (component)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
mkdir -p ppRootDir/component
time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*diag_source\\*
if ( \status ) then
    echo "WARNING: hsmget reported failure, retrying..."
    time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*diag_source\\*
checktransfer
endif
# Get files listed as associated_files
foreach file ( \\*diag_source\\* )
    # Get a list of all associated_files
    set assocFiles = `ncdump -h \file | grepAssocFiles`
    foreach assocFile ( \assocFiles )
        time_hsmget \hsmget -a opt_d -p ptmpDir/history -w tmphistdir hDate.nc/\\*\{assocFile:r}.\\*
    end
end

time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*diag_source\\*
if ( \status ) then
    echo "WARNING: hsmget reported failure, retrying..."
    time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*diag_source\\*
checktransfer
endif
# Get files listed as associated_files
foreach file ( \\*diag_source\\* )
    # Get a list of all associated_files
    set assocFiles = `ncdump -h \file | grepAssocFiles`
    foreach assocFile ( \assocFiles )
        time_hsmget \hsmget -a refinedir -p ptmpDir/history_refineDiag -w tmphistdir hDate.nc/\\*\{assocFile:r}.\\*
    end
end

foreach file ( `ls \histDir/*/*nc`)
    if ( ! -e `basename \file` ) ln -s \file .
end
EOF

        foreach i ( 1 .. 6 ) {
            csh .= <<EOF;
set files = (`ls -1 hDate.diag_source*tilei.nc | grep -v grid_spec | grep -v ocean_geometry`)
set static = (`\NCVARS -s012 \files`) # only support up to 2D static fields
if ( "\static" != "" ) then
    foreach file ( \files )
        set static = (`\NCVARS -s012 \file`)
        if ( "\static" != "" ) then
        set static = `echo \static | tr ' ' ','`
        time_splitncvars \SPLITNCVARS -s -v \static -f ppRootDir/component/component.static.tilei.nc \file
check_splitncvars
        endif
    end
endif
endif
EOF
        }
        return csh;
    } ## end if ( "sourceGrid" eq ...)
    elsif ( "sourceGrid" eq 'cubedsphere' and "xyInterp" ne '' ) {

        #DATA CONVERTED FROM CUBED SPHERE TO LATLON

        fregrid_wt = '';
        if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
            print
                "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                if opt_v;
            fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
        }
        call_and_check_fregrid = call_tile_fregrid;
        call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
        call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
        call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
        call_and_check_fregrid =~ s/set order1/if ( "\interpvars" != "" ) then\n set order1/;
        cubiccsh = <<EOF;

set output_files = ( )
set tiles = (`ls -1 hDate.diag_source*.tile1.nc | grep -v grid_spec | grep -v ocean_geometry`)
foreach tfile ( \tiles )
    if ( ! -w \tfile ) then
    set i = 1
    while ( \i <= 6 )
    set tf = \tfile:r:r.tile\i.nc
    time_cp cp \tf copy
    time_rm rm -f \tf
    time_mv mv copy \tf
    chmod 644 \tf
    @ i ++
    end
    endif
    set fregrid_wt = "fregrid_wt"
    set fregrid_in_date = hDate
    set fregrid_in = \tfile:r:r
    set nlat = nlat ; set nlon = nlon
    set interp_method = interpMethod
    set interp_options = "xyInterpOptions"
    set ncvars_arg = -s2
    set variables = ( )
    set fregrid_remap_file = xyInterpRegridFile
    set source_grid = sourceGrid

    set onedvars = `\NCVARS -s01 \fregrid_in.tile1.nc`
    if ( "\onedvars" != "" ) then
    set onedvarlist = `echo \onedvars | tr ' ' ','`
    time_splitncvars \SPLITNCVARS -s -v \onedvarlist -f ppRootDir/component/component.static.nc \fregrid_in.tile1.nc
    endif

# call and check fregrid start
call_and_check_fregrid
if (-e \fregrid_in.nc) set output_files = (\output_files \fregrid_in.nc)
endif
# call and check fregrid end

    end
EOF
        csh .= cubiccsh;
    } ## end elsif ( "sourceGrid" eq ...)
    else {
        #DATA ALREADY LATLON

        # convert latlon/tripolar to latlon
        if ( "xyInterp" ne '' ) {

            fregrid_wt = '';
            if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                print
                    "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                    if opt_v;
                fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
            }
            call_and_check_fregrid = call_fregrid;
            call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
            call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
            call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
            call_and_check_fregrid
                =~ s/set order1/if ( "\interpvars" != "" ) then\n set order1/;
            csh .= <<EOF;
        set output_files = ( )
        set files = (`ls -1 hDate.diag_source*.nc | grep -v grid_spec | grep -v ocean_geometry`)
        foreach file ( \files )
        set fregrid_wt = "fregrid_wt"
        set fregrid_in_date = hDate
        set fregrid_in = \file:r
        set nlat = nlat ; set nlon = nlon
        set interp_method = interpMethod
        set interp_options = "xyInterpOptions"
        set ncvars_arg = -s2
        set variables = ( )
        set fregrid_remap_file = xyInterpRegridFile
        set source_grid = sourceGrid

        call_and_check_fregrid

        if (-e \fregrid_in.nc ) set output_files = (\output_files \fregrid_in.nc)
        endif
        end

EOF
        } ## end if ( "xyInterp" ne '')
        else {

            csh .= <<EOF;
set output_files = (`ls -1 hDate.diag_source*.nc | grep -v grid_spec | grep -v ocean_geometry`)
EOF
        }
    } ## end else [ if ( "sourceGrid" eq ...)]

    # process latlon files for static variables
    csh .= <<EOF;
if ( \#output_files > 0 ) then
    set static = (`\NCVARS -s012 \output_files`) # only support up to 2D static fields
    if ( "\static" != "" ) then
        foreach file ( \output_files )
        set static = (`\NCVARS -s012 \file`)
        if ( "\static" != "" ) then
            set static = `echo \static | tr ' ' ','`
            time_splitncvars \SPLITNCVARS -s -v \static -f ppRootDir/component/component.static.nc \file
            check_splitncvars
        endif
        end
        time_dmput dmput ppRootDir/component/component.static.nc
    endif
endif
compress
endif
EOF
    return csh;
} ## end sub staticvars


def TSfromts(tsNode, sim0, subchunk):
"""TIMESERIES - from smaller timeSeries"""
# frepp.pl l.6562
        #tsNode = _[0] ;
    sim0     = _[1];
    subchunk = _[2];
    ppcNode  = _[0]->parentNode;

    freq        = _[0]->findvalue('@freq');
    chunkLength = _[0]->findvalue('@chunkLength');
    outdirpath  = "component/ts/freq/chunkLength";
    outdir      = "ppRootDir/outdirpath";
    if ( "chunkLength" eq "" ) {
        mailuser("Cannot create component freq timeSeries unless you set a chunkLength.");
        print STDERR
            "ERROR: Cannot create component freq timeSeries unless you set a chunkLength.\n";
        return "";
    }
    subchunkyr = "subchunk" . "yr";
    reqpath    = "ppRootDir/component/ts/freq/subchunk" . "yr";
    tmp        = FREUtil::modifydate( tEND, "+1 sec" );
    yrsSoFar   = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    cl         = chunkLength;
    cl =~ s/yr//;
    mod = yrsSoFar % cl;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( cl > maxyrs ) { maxyrs = cl; }

    #check that all files up to current time exist
    my @nodes       = ppcNode->findnodes("timeSeries[\@freq='freq']");
    diag_source = "";
    if ( scalar @nodes ) {
        node = ppcNode->findnodes("timeSeries[\@freq='freq']")->get_node(1);
        diag_source = node->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    tENDf = FREUtil::graindate( tEND, freq );
    start = FREUtil::modifydate( tEND, "-cl years +1 sec" );
    if ( "start" eq "0001010300:00:00" ) {
        start = "0001010100:00:00";
    }    #omg, hack, date::manip bugs
    end = FREUtil::modifydate( start, "+subchunk years -1 sec" );
    startf = FREUtil::graindate( start, freq );
    endf   = FREUtil::graindate( end,   freq );

#print "TSfromts tEND tEND cl cl subchunk subchunk start start startf startf end end endf endf\n";

    filelist   = "";
    getlist    = "";
    cpiolist   = "";
    periodlist = "";
    until ( endf > tENDf ) {
        filelist = "filelist component.startf-endf.\var";
        cpiolist
            = "cpiolist component.startf-endf." . FREUtil::timeabbrev(freq) . ".nc.cpio";
        getlist    = "getlist component.startf-endf.\*.nc";
        periodlist = "periodlist component.startf-endf";
        start      = FREUtil::modifydate( start, " + subchunk years" );
        end        = FREUtil::modifydate( end, " + subchunk years" );
        startf     = FREUtil::graindate( start, freq );
        endf       = FREUtil::graindate( end, freq );
    }

    #print "filelist is filelist\n";
    #print "getlist is getlist\n";
    #print "cpiolist is cpiolist\n";

    start = FREUtil::modifydate( tEND, "-cl years +1 sec" );
    startf = FREUtil::graindate( start, freq );

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    if ( "variables" ne "" ) {
        print STDERR "           from xml, vars are 'variables'\n" if opt_v;
    }
    variables =~ s/ /.nc /g;
    if ( "variables" ne "" ) { variables =~ s//.nc/g }
    varlist_from_xml = "";
    if ( "variables" ne "" ) {
        varlist_from_xml = "set varlist = ( variables )";
    }

    numtimelevels = gettimelevels( freq, chunkLength );
    check_ncatted
        = errorstr("NCATTED (component freq chunkLength ts from subchunk yr ts)");
    check_ncrcat
        = errorstr("NCRCAT (component freq chunkLength ts from subchunk yr ts)");

    #check_cpio = errorstr("CPIO (component freq chunkLength ts from subchunk yr ts)");
    check_cpio_msg = "CPIO (component freq chunkLength ts from subchunk yr ts)";
    check_dmget = errorstr("DMGET (component freq chunkLength ts from subchunk yr ts)");
    check_nccopy
        = errorstr("NCCOPY (component freq chunkLength ts from subchunk yr ts)");
    check_levels = '';
    if ( "caltype" eq "NOLEAP" or "caltype" eq "noleap" ) {
        check_levels
            = errorstr(
            "WRONG NUMBER OF TIME LEVELS (contains \length, should be numtimelevels) IN \outdir/component.startf-tENDf.\var"
            );
    }
    check_vars = errorstr(
        "NOT ALL VARIABLES EXIST FOR (component freq chunkLength ts from subchunk yr ts)");

    compress = compress_csh( "component.startf-tENDf.\var", check_nccopy );

    csh = setcheckpt( "TSfromts_freq" . "_chunkLength" );
    csh .= <<EOF;
#####################################
echo 'timeSeries (component freq chunkLength ts from subchunk yr ts)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath

EOF
    if (opt_z) { csh .= begin_systime(); }
    csh .= <<EOF;

cd reqpath
set cpiosexist = 1
foreach f ( cpiolist )
if ( ! -e \f ) set cpiosexist = 0
end
if ( \cpiosexist ) then
cd reqpath
time_dmget dmget cpiolist
set mylist = (`ls cpiolist`)
cd \work
foreach cpio ( \mylist )
    time_cp cp reqpath/\cpio .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp reqpath/\cpio .
        checktransfer
    endif
    time_uncpio uncpio -ivI \cpio || ( echo "check_cpio_msg"; echo "check_cpio_msg" > \work/.errors )
end
set varlist = `ls -1 getlist | cut -f3- -d'.' | sort -u`
varlist_from_xml
else
cd reqpath
set varlistprev = start
foreach f ( periodlist )
    set varlist = `ls -1 \f.*.nc | cut -f3- -d'.' | sort -u`
    if ( \#varlist == 0 ) break
    if ( "\varlistprev" != "start" && \#varlist != \varlistprev ) then
        echo "WARNING: different number of variables found for different time periods: \#varlist vs \varlistprev, extracting cpio files to retrieve all variables"
        set varlistprev = 'error'
        break
    else
        set varlistprev = \#varlist
    endif
end
test \#varlist != 0 -o "\varlistprev" != "error"
check_vars
varlist_from_xml
time_dmget dmget "getlist"
cd \work
foreach var (\varlist)
    foreach file ( filelist )
        if ( ! -f \tempCache/component/ts/freq/subchunkyr/\file ) then
        time_cp cp reqpath/\file \tempCache/component/ts/freq/subchunkyr/\file
        if ( \status ) then
            echo "WARNING: data transfer failure, retrying..."
            time_cp cp reqpath/\file \tempCache/component/ts/freq/subchunkyr/\file
            checktransfer
        endif
        endif
        ln -s \tempCache/component/ts/freq/subchunkyr/\file .
    end
end
endif


cd \work
foreach var (\varlist)
    if ( -e component.startf-tENDf.\var ) rm -f component.startf-tENDf.\var
    time_ncrcat ncrcat \ncrcatopt filelist component.startf-tENDf.\var
    check_ncrcat
    time_ncatted ncatted -h -O -a filename,global,m,c,"component.startf-tENDf.\var" component.startf-tENDf.\var
    check_ncatted
    set tmpstring = `ncdump -h component.startf-tENDf.\var | grep UNLIMITED`
    @ len = \#tmpstring - 1
    set length = `echo \tmpstring[\len] | cut -c2-`
    test \length = numtimelevels
    check_levels
    compress
    time_mv mvfile component.startf-tENDf.\var outdir/
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_mv mvfile component.startf-tENDf.\var outdir/
        checktransfer
    endif
    time_mv mv \work/component.startf-tENDf.\var \tempCache/outdirpath/
end

EOF

    if (aggregateTS) {
        csh .= createcpio( "\tempCache/outdirpath", outdir, "component.startf-tENDf",
            FREUtil::timeabbrev(freq), 1 );
    }
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub TSfromts

def seaTSfromts(tsNode, sim0, subchunk):
"""TIMESERIES - from smaller timeSeries"""
# frepp.pl l. 6774
    #tsNode = _[0] ;
    sim0     = _[1];
    subchunk = _[2];
    ppcNode  = _[0]->parentNode;

    freq        = _[0]->findvalue('@freq');
    chunkLength = _[0]->findvalue('@chunkLength');
    outdirpath  = "component/ts/freq/chunkLength";
    outdir      = "ppRootDir/outdirpath";
    if ( "chunkLength" eq "" ) {
        mailuser("Cannot create component freq timeSeries unless you set a chunkLength.");
        print STDERR
            "ERROR: Cannot create component freq timeSeries unless you set a chunkLength.\n";
        return "";
    }
    subchunkyr = "subchunk" . "yr";
    reqpath    = "ppRootDir/component/ts/freq/subchunk" . "yr";
    req        = "component/ts/freq/subchunk" . "yr";
    tmp        = FREUtil::modifydate( tEND, "+1 sec" );
    yrsSoFar   = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    cl         = chunkLength;
    cl =~ s/yr//;
    mod = yrsSoFar % cl;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( cl > maxyrs ) { maxyrs = cl; }

    #check that all files up to current time exist
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    diag_source = "";
    if ( scalar @monthnodes ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    tENDf = FREUtil::graindate( tEND, 'year' );
    start = FREUtil::modifydate( tEND,  "-cl years +1 sec" );
    end   = FREUtil::modifydate( start, "+subchunk years -1 sec" );
    startf = FREUtil::graindate( start, 'year' );
    endf   = FREUtil::graindate( end,   'year' );

    filelist = "";
    cpiolist = "";
    until ( endf > tENDf ) {
        filelist = "filelist component.startf-endf.\sea.\var";
        cpiolist
            = "cpiolist component.startf-endf." . FREUtil::timeabbrev(freq) . ".nc.cpio";
        start = FREUtil::modifydate( start, " + subchunk years" );
        end   = FREUtil::modifydate( end,   " + subchunk years" );
        startf = FREUtil::graindate( start, 'year' );
        endf   = FREUtil::graindate( end,   'year' );
    }

    #get variables
    variables  = FREUtil::cleanstr( _[0]->findvalue('variables') );
    setvarlist = "";
    if ( "variables" eq "" ) {
        setvarlist
            = "set varlist = (`ls -1 | grep -v '.nc.cpio' | cut -f4-5 -d'.' | sort -u`)\n";
        variables = 'varlist';
    }
    else {
        print STDERR "           from xml, vars are 'variables'\n" if opt_v;
        variables =~ s/ /.nc /g;
        if ( "variables" ne "" ) { variables =~ s//.nc/g }
        setvarlist = "set varlist = ( variables )\n";
    }

    start = FREUtil::modifydate( tEND, "-cl years +1 sec" );
    startf = FREUtil::graindate( start, 'year' );
    numtimelevels = gettimelevels( freq, chunkLength );

    check_ncatted
        = errorstr("NCATTED (component freq chunkLength ts from subchunk yr ts)");
    check_ncrcat
        = errorstr("NCRCAT (component freq chunkLength ts from subchunk yr ts)");
    check_cpio_msg = "CPIO (component freq chunkLength ts from subchunk yr ts)";
    check_dmget = errorstr("DMGET (component freq chunkLength ts from subchunk yr ts)");
    check_nccopy
        = errorstr("NCCOPY (component freq chunkLength ts from subchunk yr ts)");
    check_levels = '';

    if ( "caltype" eq "NOLEAP" or "caltype" eq "noleap" ) {
        check_levels
            = errorstr(
            "WRONG NUMBER OF TIME LEVELS (contains \length, should be numtimelevels) IN \outdir/component.startf-tENDf.\sea.\var"
            );
    }
    check_vars
        = errorstr(
        "MISSING FILE reqpath/\file, component.startf-tENDf.\sea.\var NOT CREATED (component freq chunkLength ts from subchunk yr ts)"
        );

    my @pieces = split ' ', filelist;
    dmgetcommand = "";
    foreach piece (@pieces) {
        piece =~ s/\sea.\var/*.nc/g;
        dmgetcommand .= "time_dmget dmget \"piece\"\n";
    }
    compress = compress_csh( "component.startf-tENDf.\sea.\var", check_nccopy );
    csh = setcheckpt("seaTSfromts_chunkLength");
    csh .= <<EOF;
#####################################
echo 'timeSeries (component freq chunkLength ts from subchunk yr ts)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
if ( ! -e \tempCache/outdirpath ) mkdir -p \tempCache/outdirpath

EOF
    if (opt_z) { csh .= begin_systime(); }
    csh .= <<EOF;

cd reqpath
set cpiosexist = 1
foreach f ( cpiolist )
if ( ! -e \f ) set cpiosexist = 0
end
if ( \cpiosexist ) then
time_dmget dmget cpiolist
set mylist = (`ls cpiolist`)
cd \work
foreach cpio ( \mylist )
    time_cp cp reqpath/\cpio .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp reqpath/\cpio .
        checktransfer
    endif
    time_uncpio uncpio -ivI \cpio || ( echo "check_cpio_msg"; echo "check_cpio_msg" > \work/.errors )
end
setvarlist
else
setvarlist
dmgetcommand
cd \work
foreach sea ( DJF MAM JJA SON )
    foreach var (\varlist)
        foreach file ( filelist )
        if ( ! -f \tempCache/req/\file ) then
            time_cp cp reqpath/\file \tempCache/req/\file
            if ( \status ) then
                echo "WARNING: data transfer failure, retrying..."
                time_cp cp reqpath/\file \tempCache/req/\file
                checktransfer
            endif
        endif
        ln -s \tempCache/req/\file .
        end
    end
end
endif

cd \work
foreach sea (DJF MAM JJA SON)
foreach var (\varlist)
    set missingfiles = 0
    foreach file (filelist)
        test -e \file
        check_vars
        if ( ! -e \file ) set missingfiles = 1
    end
    if ( \missingfiles == 0 ) then

    if ( -e component.startf-tENDf.\sea.\var ) rm -f component.startf-tENDf.\sea.\var
    time_ncrcat ncrcat \ncrcatopt filelist component.startf-tENDf.\sea.\var
    check_ncrcat
    time_ncatted ncatted -h -O -a filename,global,m,c,"component.startf-tENDf.\sea.\var" component.startf-tENDf.\sea.\var
    check_ncatted
    set tmpstring = `ncdump -h component.startf-tENDf.\sea.\var | grep UNLIMITED`
    @ len = \#tmpstring - 1
    set length = `echo \tmpstring[\len] | cut -c2-`
    test \length = numtimelevels
    compress
    time_mv mvfile component.startf-tENDf.\sea.\var \outdir/
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_mv mvfile component.startf-tENDf.\sea.\var \outdir/
        checktransfer
    endif
    time_mv mv component.startf-tENDf.\sea.\var \tempCache/outdirpath/
    check_levels

    endif
end
end

EOF
    if (aggregateTS) {
        csh .= createcpio( "\tempCache/outdirpath", outdir, "component.startf-tENDf",
            FREUtil::timeabbrev(freq), 1 );
    }
    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub seaTSfromts

def seasonalAVfromhist(taNode, sim0):
"""TIMEAVERAGES - SEASONAL"""
# frepp.pl l.6978
        #taNode = _[0] ;
    sim0    = _[1];
    ppcNode = _[0]->parentNode;

    src         = 'seasonal';
    interval    = _[0]->findvalue('@interval');
    outdir      = "ppRootDir/component/av/src" . "_interval";
    chunkLength = _[0]->findvalue('@chunkLength');
    tmp         = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar    = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int         = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }
    hDateyr = userstartyear;
    my @hDates  = ( FREUtil::padzeros( hDateyr - int + 1 ) .. "hDateyr" );
    tBEG    = FREUtil::modifydate( tEND, "- int years + 1 sec" );
    range   = FREUtil::graindate( tEND, 'year' );
    if ( scalar @hDates > 1 ) { range = FREUtil::padzeros( range - int + 1 ) . "-range"; }
    foreach d (@hDates) { historyfiles .= "d" . "0101.nc.tar "; }

    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    #get variables
    variables = FREUtil::cleanstr( _[0]->findvalue('variables') );
    variables =~ s/ /,/g;

    zInterp    = ppcNode->findvalue('@zInterp');
    do_zInterp = 0;
    if ( "zInterp" ne "" ) { do_zInterp = 1; }
    check_ncatted  = errorstr("NCATTED (component src interval averages)");
    check_cpio     = errorstr("CPIO/TAR (component src interval averages)");
    check_cpio_msg = "CPIO (component src interval averages)";
    check_ncrcat   = errorstr("NCRCAT (component src interval averages)");
    check_timavg   = retryonerrorend("TIMAVG (component src interval averages)");
    retry_timavg   = retryonerrorstart("TIMAVG");
    check_plevel   = errorstr("PLEVEL (component src interval averages)");
    check_ncdump   = errorstr("NCDUMP (component src interval averages)");
    check_dmget    = errorstr("DMGET (component src interval averages)");
    check_ncap     = errorstr("NCAP (component src interval averages)");
    check_nccopy   = errorstr("NCCOPY (component src interval averages)");
    check_numfiles = errorstr(
        "INCORRECT NUMBER OF SEASONS IN SEASONAL FILE (component src interval averages)");
    check_fregrid  = errorstr("FREGRID (component src interval averages)");
    check_ncrename = errorstr("NCRENAME (component src interval averages)");
    convertSeg     = convertSegments( segTime, segUnits, diag_source );
    convertDec     = convertSegments( segTime, segUnits, diag_source, 'dec' );
    decSeg         = ( split ' ', convertDec )[-4];
    decSeg =~ s/.*(\d\d01).*/1/;

    csh = setcheckpt("seasonalAVfromhist_interval");
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src interval averages)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir
mkdir out
EOF
    if (opt_z) { csh .= begin_systime(); }

    startseason  = 1;
    totalseasons = int * 4;

    if ( "sourceGrid" eq 'cubedsphere' ) {    #IF CUBIC

        tmpsrcstr = "diag_source.tile*.nc";
        csh .= <<EOF;
foreach hDate ( @hDates )
    set nhistfiles = 0
    foreach file ( `ls \histDir/\hDate*/*.tmpsrcstr`)
    ln -s \file .
    @ nhistfiles ++
    end
    foreach file ( `ls \histDir/\hDate*/*.grid_spec.tile*.nc`)
    ln -s \file .
    end
    if ( \nhistfiles == 0 ) then
        echo 'ERROR: No history files matching \hDate*/*.tmpsrcstr'
    endif
    convertSeg
end
EOF
        foreach season ( startseason .. totalseasons ) {
            startmo      = season * 3 - 4;
            tSEASON      = FREUtil::modifydate( tBEG, "+ startmo months" );
            seahist      = FREUtil::graindate( tSEASON, 'day' );
            tSEASONf     = FREUtil::graindate( tSEASON, 'seasonal' );
            my (year)       = tSEASONf =~ /(\d{4,})\./;
            my (prevyear)   = seahist =~ /(\d{4,})\d{4}/;
            prevhistcpio = "prevyear" . decSeg . ".nc.cpio";
            prevhisttar  = "prevyear" . decSeg . ".nc.tar";
            nextd        = FREUtil::modifydate( tSEASON, "+ 12 months" );
            nextdec      = FREUtil::graindate( nextd, 'day' ) . ".diag_source.nc";
            tile         = '.tilei';

            nextdec    = FREUtil::graindate( nextd, 'day' ) . ".diag_source";
            hfilelist1 = "";
            hfilelist2 = "";
            hfilelist3 = "";
            hfilelist4 = "";
            hfilelist5 = "";
            hfilelist6 = "";
            foreach s ( startmo .. ( startmo + 2 ) ) {
                t = FREUtil::modifydate( tBEG, "+ s months" );
                shist = FREUtil::graindate( t, 'day' );
                hfilelist1 = "hfilelist1 shist.diag_source.tile1.nc";
                hfilelist2 = "hfilelist2 shist.diag_source.tile2.nc";
                hfilelist3 = "hfilelist3 shist.diag_source.tile3.nc";
                hfilelist4 = "hfilelist4 shist.diag_source.tile4.nc";
                hfilelist5 = "hfilelist5 shist.diag_source.tile5.nc";
                hfilelist6 = "hfilelist6 shist.diag_source.tile6.nc";
            }
            csh .= <<EOF;
echo season season ==============================================================
EOF
            if ( season == 1 ) {
                csh .= <<EOF;
if ( ! -e seahist.diag_source.tile1.nc ) then
if ( -e ppRootDir/.dec/seahist.diag_source.tile1.nc ) then
    time_dmget dmget ppRootDir/.dec/seahist.diag_source.tile?.nc
    time_cp cp ppRootDir/.dec/seahist.diag_source.tile?.nc .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/.dec/seahist.diag_source.tile?.nc .
        checktransfer
    endif
    time_rm rm -f ppRootDir/.dec/seahist.diag_source.tile?.nc
else if ( -e opt_d/prevhistcpio ) then
    time_dmget dmget opt_d/prevhistcpio
    time_uncpio uncpio -ivI opt_d/prevhistcpio '*.diag_source.tile*.nc'
    check_cpio
    time_dmput dmput opt_d/prevhistcpio
    set prevyear = prevyear
    set i = 1
    while ( \i <= 6 )
    convertDec
    @ i ++
    end
else if ( -e opt_d/prevhisttar ) then
    time_dmget dmget opt_d/prevhisttar
    time_untar tar -xvf opt_d/prevhisttar --wildcards '*.diag_source.tile*.nc'
    check_cpio
    time_dmput dmput opt_d/prevhisttar
    set prevyear = prevyear
    set i = 1
    while ( \i <= 6 )
    convertDec
    @ i ++
    end
else
    set t = `ncdump -h nextdec.tile1.nc | grep -i '.*=.*unlimited.*currently' | awk '{print \1}'`
    set att_copy = (`ncdump -h nextdec.tile1.nc | sed -ne "s/.*\{t}:\\(.*\\) =.*/\t@\\1=\t@\\1;/gp"`)
    if ( `ncdump -h nextdec.tile1.nc | grep -c " average_T1("` == 1 ) then
        # A field with 'long_name =  "time axis boundary"' should be in
        # the file.  Be sure to use the same name in the following
        # commands.  We can rather safely assume the same field name
        # will be used in all the tile files.
        set tbnds_var = `ncdump -h nextdec.tile1.nc | grep 'long_name = "time axis boundaries"' | awk -F : '{ print 1 }'`

        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile1.nc seahist.diag_source.tile1.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile2.nc seahist.diag_source.tile2.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile3.nc seahist.diag_source.tile3.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile4.nc seahist.diag_source.tile4.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile5.nc seahist.diag_source.tile5.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec.tile6.nc seahist.diag_source.tile6.nc
        check_ncap
    else
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile1.nc seahist.diag_source.tile1.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile2.nc seahist.diag_source.tile2.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile3.nc seahist.diag_source.tile3.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile4.nc seahist.diag_source.tile4.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile5.nc seahist.diag_source.tile5.nc
        check_ncap
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec.tile6.nc seahist.diag_source.tile6.nc
        check_ncap
    endif
endif
endif
EOF
            } ## end if ( season == 1 )

            compress = compress_csh( "component.tSEASONf.nc", check_nccopy );

            csh .= <<EOF;
if ( -e sea1.nc ) time_rm rm -f sea?.nc
time_ncrcat ncrcat \ncrcatopt hfilelist1 sea1.nc
check_ncrcat
time_ncrcat ncrcat \ncrcatopt hfilelist2 sea2.nc
check_ncrcat
time_ncrcat ncrcat \ncrcatopt hfilelist3 sea3.nc
check_ncrcat
time_ncrcat ncrcat \ncrcatopt hfilelist4 sea4.nc
check_ncrcat
time_ncrcat ncrcat \ncrcatopt hfilelist5 sea5.nc
check_ncrcat
time_ncrcat ncrcat \ncrcatopt hfilelist6 sea6.nc
check_ncrcat
time_rm rm hfilelist1 hfilelist2 hfilelist3 hfilelist4 hfilelist5 hfilelist6
EOF

            #put code here to handle 1yr seasons case
            if ( int == 1 ) {
                if (do_zInterp) {
                    foreach t ( 1 .. 6 ) {
                        csh .= <<EOF;
time_timavg \TIMAVG -o modellevelst.nc seat.nc
retry_timavg
time_timavg \TIMAVG -o modellevelst.nc seat.nc
check_timavg
EOF
                        csh
                            .= zInterpolate( zInterp, "modellevelst.nc",
                            "component.tSEASONf.tilet.nc",
                            caltype, variables, component );
                    }
                    csh .= "time_rm rm modellevels?.nc\n";

                }
                else {
                    foreach t ( 1 .. 6 ) {
                        csh .= <<EOF;
time_timavg \TIMAVG -o component.tSEASONf.tilet.nc seat.nc
retry_timavg
time_timavg \TIMAVG -o component.tSEASONf.tilet.nc seat.nc
check_timavg
EOF
                    }
                }

                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_tile_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                csh .= <<EOF;
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = component.tSEASONf
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st23
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

time_ncatted ncatted -h -O -a filename,global,m,c,"component.tSEASONf.nc" component.tSEASONf.nc
check_ncatted

compress
time_mv mvfile component.tSEASONf.nc \outdir/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.tSEASONf.nc \outdir/
checktransfer
endif
time_rm rm component.tSEASONf.nc
if ( -e \outdir/component.tSEASONf.nc ) time_rm rm -f sea1.nc sea2.nc sea3.nc sea4.nc sea5.nc sea6.nc
EOF
            } ## end if ( int == 1 )
            else {    #int > 1
                foreach t ( 1 .. 6 ) {
                    csh .= <<EOF;
time_timavg \TIMAVG -o out/component.tSEASONf.tilet.nc seat.nc
retry_timavg
time_timavg \TIMAVG -o out/component.tSEASONf.tilet.nc seat.nc
check_timavg
time_rm rm -f seat.nc
EOF
                }
            }
        } ## end foreach season ( startseason...)

        startmo = ( totalseasons + 1 ) * 3 - 4;
        tSEASON = FREUtil::modifydate( tBEG, "+ startmo months" );
        nextdec = FREUtil::graindate( tSEASON, 'day' ) . ".diag_source";
        csh .= <<EOF;
time_mv mvfile nextdec.tile* ppRootDir/.dec/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile nextdec.tile* ppRootDir/.dec/
checktransfer
endif
time_rm rm nextdec.tile*
EOF

        if ( int > 1 ) {
            csh .= <<EOF;
time_mv mv out/* .
rmdir out
EOF
            foreach season ( 'DJF', 'MAM', 'JJA', 'SON' ) {
                dates = "range" . ".season";
                csh .= <<EOF;
echo season season =========================================================
test int = `ls -1 component.*.season.tile1.nc | wc -l`
check_numfiles
set i = 1
while ( \i <= 6 )
set list = `ls -1 component.*.season.tile\i.nc`
if ( -e sea\i.nc ) time_rm rm -f sea\i.nc
time_ncrcat ncrcat \ncrcatopt \list sea\i.nc
check_ncrcat
time_rm rm -f \list
EOF

                if (do_zInterp) {
                    csh .= <<EOF;
time_timavg \TIMAVG -o modellevels\i.nc sea\i.nc
retry_timavg
time_timavg \TIMAVG -o modellevels\i.nc sea\i.nc
check_timavg
EOF
                    csh
                        .= zInterpolate( zInterp, 'modellevelsi.nc',
                        "component.dates.tile\i.nc",
                        caltype, variables, component );
                }
                else {
                    csh .= <<EOF;
time_timavg \TIMAVG -o component.dates.tile\i.nc sea\i.nc
retry_timavg
time_timavg \TIMAVG -o component.dates.tile\i.nc sea\i.nc
check_timavg
EOF
                }

                #apparently we are assuming we want to convert to latlon...?
                fregrid_wt = '';
                if ( component =~ /land/ and dtvars{all_land_static} =~ /\bland_frac\b/ ) {
                    print
                        "       land_frac found, weighting exchange grid cell with hDate.land_static\n"
                        if opt_v;
                    fregrid_wt = "--weight_file hDate.land_static --weight_field land_frac";
                }
                call_and_check_fregrid = call_tile_fregrid;
                call_and_check_fregrid =~ s/#check_fregrid/check_fregrid/;
                call_and_check_fregrid =~ s/#check_ncrename/check_ncrename/g;
                call_and_check_fregrid =~ s/#check_ncatted/check_ncatted/g;
                compress = compress_csh( "component.dates.nc", check_nccopy );
                csh .= <<EOF;
@ i ++
end
set fregrid_wt = "fregrid_wt"
set fregrid_in_date = hDate
set fregrid_in = component.dates
set nlat = nlat ; set nlon = nlon
set interp_method = interpMethod
set interp_options = "xyInterpOptions"
set ncvars_arg = -st0123
set variables = ( variables )
set fregrid_remap_file = xyInterpRegridFile
set source_grid = sourceGrid
call_and_check_fregrid

time_ncatted ncatted -h -O -a filename,global,m,c,"component.dates.nc" component.dates.nc
check_ncatted

compress
time_mv mvfile component.dates.nc \outdir/component.dates.nc
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.dates.nc \outdir/component.dates.nc
checktransfer
endif
time_rm rm component.dates.nc
time_dmput dmput \outdir/component.dates.nc
time_rm rm -f sea1.nc sea2.nc sea3.nc sea4.nc sea5.nc sea6.nc
EOF
            } ## end foreach season ( 'DJF',...)
        } ## end if ( int > 1 )

    } ## end if ( "sourceGrid" eq ...)
    else {    #ELSE NOT CUBIC

        tmpsrcstr = "diag_source.nc";
        csh .= <<EOF;
foreach hDate ( @hDates )
    set nhistfiles = 0
    foreach file ( `ls \histDir/\hDate*/*.tmpsrcstr`)
    ln -s \file .
    @ nhistfiles ++
    end
    if ( \nhistfiles == 0 ) then
        echo 'ERROR: No history files matching \hDate*/*.tmpsrcstr'
    endif
    convertSeg
end
EOF

        foreach season ( startseason .. totalseasons ) {
            startmo = season * 3 - 4;
            plus    = "+";
            if ( "startmo" =~ /^-/ ) { plus = ''; }
            tSEASON = FREUtil::modifydate( tBEG, "plus startmo months" );
            seahist  = FREUtil::graindate( tSEASON, 'day' );
            tSEASONf = FREUtil::graindate( tSEASON, 'seasonal' );
            my (year)   = tSEASONf =~ /(\d{4,})\./;
            my (prevyear)   = seahist =~ /(\d{4,})\d{4}/;
            prevhistcpio = "prevyear" . decSeg . ".nc.cpio";
            prevhisttar  = "prevyear" . decSeg . ".nc.tar";
            nextd = FREUtil::modifydate( tSEASON, "+ 12 months" );
            nextdec = FREUtil::graindate( nextd, 'day' ) . ".diag_source.nc";
            hfilelist = "";

            foreach s ( startmo .. ( startmo + 2 ) ) {
                plus = "+";
                if ( "s" =~ /^-/ ) { plus = ''; }
                t = FREUtil::modifydate( tBEG, "plus s months" );
                shist = FREUtil::graindate( t, 'day' );
                hfilelist = "hfilelist shist.diag_source.nc";
            }

            if ( season == 1 ) {
                csh .= <<EOF;
echo season season ==============================================================
if ( ! -e seahist.diag_source.nc ) then
if ( -e ppRootDir/.dec/seahist.diag_source.nc ) then
    time_dmget dmget ppRootDir/.dec/seahist.diag_source.nc
    time_cp cp ppRootDir/.dec/seahist.diag_source.nc .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp ppRootDir/.dec/seahist.diag_source.nc .
        checktransfer
    endif
    time_rm rm -f ppRootDir/.dec/seahist.diag_source.nc
else if ( -e opt_d/prevhistcpio ) then
    time_dmget dmget opt_d/prevhistcpio
    time_uncpio uncpio -ivI opt_d/prevhistcpio '*.diag_source.nc'
    check_cpio
    time_dmput dmput opt_d/prevhistcpio
    set prevyear = prevyear
    convertDec
else if ( -e opt_d/prevhisttar ) then
    time_dmget dmget opt_d/prevhisttar
    time_untar tar -xvf opt_d/prevhisttar --wildcards '*.diag_source.nc'
    check_cpio
    time_dmput dmput opt_d/prevhisttar
    set prevyear = prevyear
    convertDec
else
    set t = `ncdump -h nextdec | grep -i '.*=.*unlimited.*currently' | awk '{print \1}'`
    set att_copy = (`ncdump -h nextdec | sed -ne "s/.*\{t}:\\(.*\\) =.*/\t@\\1=\t@\\1;/gp"`)
    #set hasAVT1 = `ncdump -v average_T1 nextdec | wc -l`
    if ( `ncdump -h nextdec | grep -c " average_T1("` == 1 ) then
        # A field with 'long_name =  "time axis boundary"' should be in
        # the file.  Be sure to use the same name in the following
        # commands.  We can rather safely assume the same field name
        # will be used in all the tile files.
        set tbnds_var = `ncdump -h nextdec.tile1.nc | grep 'long_name = "time axis boundaries"' | awk -F : '{ print 1 }'`

        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; average_T1=average_T1-365; average_T2=average_T2-365; \{tbnds_var}=\{tbnds_var}-365; \att_copy" nextdec seahist.diag_source.nc
        check_ncap
    else
        time_ncap ncap2 -h -O -s "\{t}[\{t}]=\{t}-365; \att_copy" nextdec seahist.diag_source.nc
        check_ncap
    endif
endif
endif
EOF
            } ## end if ( season == 1 )
            else {
                csh .= <<EOF;
echo season season ==============================================================
EOF
            }

            csh .= <<EOF;
if ( -e sea.nc ) time_rm rm -f sea.nc
time_ncrcat ncrcat \ncrcatopt hfilelist sea.nc
check_ncrcat
time_rm rm -f hfilelist
EOF

            #put code here to handle 1yr seasons case
            if ( int == 1 ) {
                compress = compress_csh( "component.tSEASONf.nc", check_nccopy );
                if (do_zInterp) {
                    csh .= <<EOF;
time_timavg \TIMAVG -o modellevels.nc sea.nc
retry_timavg
time_timavg \TIMAVG -o modellevels.nc sea.nc
check_timavg

EOF
                    csh .= zInterpolate( zInterp, 'modellevels.nc', "component.tSEASONf.nc",
                        caltype, variables, component );
                    csh .= <<EOF;
compress
time_mv mvfile component.tSEASONf.nc \outdir/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.tSEASONf.nc \outdir/
checktransfer
endif

EOF
                } ## end if (do_zInterp)
                else {
                    csh .= <<EOF;
time_timavg \TIMAVG -o component.tSEASONf.nc sea.nc
retry_timavg
time_timavg \TIMAVG -o component.tSEASONf.nc sea.nc
check_timavg
compress
time_mv mvfile component.tSEASONf.nc \outdir/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.tSEASONf.nc \outdir/
checktransfer
endif
time_rm rm component.tSEASONf.nc
if ( -e \outdir/component.tSEASONf.nc ) time_rm rm -f sea.nc
EOF
                }
            } ## end if ( int == 1 )
            else {
                csh .= <<EOF;
time_timavg \TIMAVG -o out/component.tSEASONf.nc sea.nc
retry_timavg
time_timavg \TIMAVG -o out/component.tSEASONf.nc sea.nc
check_timavg
if ( -e out/component.tSEASONf.nc ) time_rm rm -f sea.nc
EOF
            }
        } ## end foreach season ( startseason...)

        startmo = ( totalseasons + 1 ) * 3 - 4;
        tSEASON = FREUtil::modifydate( tBEG, "+ startmo months" );
        nextdec = FREUtil::graindate( tSEASON, 'day' ) . ".diag_source.nc";
        csh .= <<EOF;
time_mv mvfile nextdec ppRootDir/.dec/
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile nextdec ppRootDir/.dec/
checktransfer
endif
time_rm rm nextdec
cd out
EOF

        if ( int > 1 ) {
            foreach season ( 'DJF', 'MAM', 'JJA', 'SON' ) {
                dates = "range" . ".season";
                csh .= <<EOF;
echo season season =========================================================
test int = `ls -1 component.*.season.nc | wc -l`
check_numfiles
set list = `ls -1 component.*.season.nc`
if ( -e sea.nc ) time_rm rm -f sea.nc
time_ncrcat ncrcat \ncrcatopt \list sea.nc
check_ncrcat
time_rm rm -f \list
EOF

                if (do_zInterp) {
                    csh .= <<EOF;
time_timavg \TIMAVG -o modellevels.nc sea.nc
retry_timavg
time_timavg \TIMAVG -o modellevels.nc sea.nc
check_timavg

EOF
                    csh .= zInterpolate( zInterp, 'modellevels.nc', "component.dates.nc",
                        caltype, variables, component );
                }
                else {
                    csh .= <<EOF;
time_timavg \TIMAVG -o component.dates.nc sea.nc
retry_timavg
time_timavg \TIMAVG -o component.dates.nc sea.nc
check_timavg
EOF
                }

                compress = compress_csh( "component.dates.nc", check_nccopy );

                csh .= <<EOF;
time_ncatted ncatted -h -O -a filename,global,m,c,"\outdir/component.dates.nc" component.dates.nc
check_ncatted
compress
time_mv mvfile component.dates.nc \outdir/component.dates.nc
if ( \status ) then
echo "WARNING: data transfer failure, retrying..."
time_mv mvfile component.dates.nc \outdir/component.dates.nc
checktransfer
endif
time_rm rm component.dates.nc
time_dmput dmput \outdir/component.dates.nc
time_rm rm sea.nc
EOF
            } ## end foreach season ( 'DJF',...)
        } ## end if ( int > 1 )

        #END NOT CUBIC
    } ## end else [ if ( "sourceGrid" eq ...)]

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub seasonalAVfromhist

def seasonalAVfromav(taNode, sim0, subint):
"""TIMEAVERAGES - SEASONAL (doesn't support cubedsphere)"""
# frepp.pl l.7611
    #taNode = _[0] ;
    sim0    = _[1];
    subint  = _[2];
    ppcNode = _[0]->parentNode;

    src         = 'seasonal';
    interval    = _[0]->findvalue('@interval');
    outdir      = "ppRootDir/component/av/src" . "_interval";
    srcdir      = "ppRootDir/component/av/src" . "_subint" . "yr";
    chunkLength = _[0]->findvalue('@chunkLength');
    tmp         = FREUtil::modifydate( tEND, "+ 1 sec" );
    yrsSoFar    = &Delta_Format( FREUtil::dateCalc( sim0, tmp ), 0, "%yd" );
    int         = interval;
    int =~ s/yr//;
    mod = yrsSoFar % int;
    unless ( mod == 0 ) { return ""; }
    mkdircommand .= "outdir ";
    if ( int > maxyrs ) { maxyrs = int; }

    #check for missing files
    diag_source = _[0]->findvalue('@diagSource');
    my @monthnodes  = ppcNode->findnodes('timeSeries[@freq="monthly"]');
    if ( scalar @monthnodes and "diag_source" eq "" ) {
        monthnode = ppcNode->findnodes('timeSeries[@freq="monthly"]')->get_node(1);
        diag_source = monthnode->getAttribute('@source');
    }
    if ( "diag_source" eq "" ) { diag_source = _[0]->findvalue('../@source'); }
    if ( "diag_source" eq "" ) { diag_source = component . "_month"; }

    end = FREUtil::graindate( tEND, 'year' );
    start = FREUtil::padzeros( end - int + 1 );

    substart = start;
    subend   = FREUtil::padzeros( start + subint - 1 );
    filelist = "";
    getlist  = "";
    until ( subend > end ) {
        if   ( substart == subend ) { filelist .= "component.substart.\sea.nc "; }
        else                          { filelist .= "component.substart-subend.\sea.nc "; }
        substart = FREUtil::padzeros( substart + subint );
        subend   = FREUtil::padzeros( subend + subint );
    }
    getlist = filelist;
    getlist =~ s/\sea/*/g;

    #   print "filelist is filelist\n";
    #   print "getlist is getlist\n";

    check_ncatted = errorstr("NCATTED (component src interval averages)");
    check_ncrcat  = errorstr("NCRCAT (component src interval averages)");
    check_timavg  = retryonerrorend("TIMAVG (component src interval averages)");
    retry_timavg  = retryonerrorstart("TIMAVG");
    check_dmget   = errorstr("DMGET (component src interval averages)");
    check_nccopy  = errorstr("NCCOPY (component src interval averages)");
    csh           = setcheckpt("seasonalAVfromav_interval");
    csh .= <<EOF;
#####################################
echo 'timeAverage (component src interval averages from subint yr averages)'
cd \work
find \work/* -maxdepth 1 -exec rm -rf {} \\;
set outdir = outdir
if ( ! -e \outdir ) mkdir -p \outdir

EOF
    if (opt_z) { csh .= begin_systime(); }
    compress = compress_csh( "component.start-end.\sea.nc", check_nccopy );
    csh .= <<EOF;

cd srcdir
time_dmget dmget "getlist"

foreach sea (DJF MAM JJA SON)
    cd \work
    foreach file (filelist)
    time_cp cp srcdir/\file .
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_cp cp srcdir/\file .
        checktransfer
    endif
    end
    if ( -e \sea.nc ) rm -f \sea.nc
    time_ncrcat ncrcat \ncrcatopt filelist \sea.nc
    check_ncrcat
    time_timavg \TIMAVG -o component.start-end.\sea.nc \sea.nc
    retry_timavg
    time_timavg \TIMAVG -o component.start-end.\sea.nc \sea.nc
    check_timavg
    time_ncatted ncatted -h -O -a filename,global,m,c,"component.start-end.\sea.nc" component.start-end.\sea.nc
    check_ncatted
    compress
    time_mv mvfile component.start-end.\sea.nc \outdir/component.start-end.\sea.nc
    if ( \status ) then
        echo "WARNING: data transfer failure, retrying..."
        time_mv mvfile component.start-end.\sea.nc \outdir/component.start-end.\sea.nc
        checktransfer
    endif
    time_rm rm component.start-end.\sea.nc
    time_dmput dmput \outdir/component.start-end.\sea.nc
    time_rm rm -f \sea.nc
end

EOF

    if (opt_z) { csh .= end_systime(); }
    csh .= mailerrors(outdir);

    return csh;
} ## end sub seasonalAVfromav

