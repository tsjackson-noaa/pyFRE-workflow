# FREpp package

This package contains translations of the code in `FRE/bin/frepp.pl`. Functions 
correspond to code in the bodies of loops, as outlined below. Each function is
intended to be wrapped and executed as a separate task in a python-based workflow
manager.

- Cli.py handoff to initialization of FREpp object
- `setup_fre`: Initialize lib/FRE object; parse XML
- Loop over experiments in cli args:
  - `setup_expt`: Configure lib/FREExperiment object.
  - `expt_loop_pre_component`: Template shared portion of .csh runscript.
  - Loop over components in experiment:
    - `component_loop_setup`: Add component-specific setup to runscript.
    - `timeseries_static`: Add commands to postprocess static variables.
    - `timeaverages_monthly_setup`: Set up loop for monthly time averages, if any
    - Loop over requested time averages:
      - `timeaverages_monthly`: Add commands to generate one monthly time average.
    - `timeaverages_annual_setup`: Set up loop for annual time averages, if any
    - Loop over requested time averages:
      - `timeaverages_annual`: Add commands to generate one annual time average.
    - `timeaverages_seasonal_setup`: Set up loop for seasonal time averages, if any
    - Loop over requested time averages:
      - `timeaverages_seasonal`: Add commands to generate one seasonal time average.
    - Loop over sub-daily time series frequencies:
        - `timeseries_hourly_setup`: Set up loop for sub-daily time series of the requested frequency, if any
        - Loop over requested time series:
          - `timeseries_hourly`: Add commands to generate one sub-daily time series.
    - `timeseries_daily_setup`: Set up loop for daily time series, if any
    - Loop over requested time series:
      - `timeseries_daily`: Add commands to generate one daily time series.
    - `timeseries_monthly_setup`: Set up loop for monthly time series, if any
    - Loop over requested time series:
      - `timeseries_monthly`: Add commands to generate one monthly time series.
    - `timeseries_annual_setup`: Set up loop for annual time series, if any
    - Loop over requested time series:
      - `timeseries_annual`: Add commands to generate one annual time series.
    - `timeseries_seasonal_setup`: Set up loop for seasonal time series, if any
    - Loop over requested time series:
      - `timeseries_seasonal`: Add commands to generate one seasonal time series.
    - `component_loop_dependencies`: construct and submit jobs for dependent years.
  - `expt_loop_post_component`: run `--plus` commands for following year