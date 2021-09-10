# FREpp package

This package contains translations of the code in `FRE/bin/frepp.pl` from perl to 
python. To simplify migration, variable names and code organization follow 
frepp.pl as closely as possible.

Functions correspond to code in the bodies of loops, as outlined below. Each 
function is intended to be wrapped and executed as a separate task in a 
python-based workflow manager.

- Cli.py handoff to initialization of FREpp object
- `setup_fre`: Initialize lib/FRE object; parse XML
- Loop over experiments in cli args:
  - `setup_expt`: Configure lib/FREExperiment object.
  - `expt_loop_pre_component`: Template shared portion of .csh runscript.
  - Loop over components in experiment:
    - `component_loop_setup`: Add component-specific setup to runscript.
    - `timeseries_static`: Add commands to postprocess static variables.
    - Loop over time average intervals:
      - `timesaverages_setup`: Set up loop for time averages at this interval, if any
      - Loop over requested time averages:
        - `add_timeaverage`: Add commands to generate one time average.
    - Loop over time series frequencies:
      - `timeseries_setup`: Set up loop for time series at this frequency, if any
      - Loop over requested time series:
        - `add_timeseries`: Add commands to generate one time series.
    - `component_loop_dependencies`: construct and submit jobs for dependent years.
  - `expt_loop_post_component`: run `--plus` commands for following year
