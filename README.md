# pyFRE-workflow

Proof-of-concept for implementing GFDL's [FMS Runtime Environment](https://github.com/NOAA-GFDL/FRE) in terms of a python-based workflow engine. 

`/FRE` contains the current FRE repo as a git subtree. For the purposes of the proof-of-concept, the perl code in FRE will be transliterated into python in `\src`. We also reuse utility classes from [MDTF-diagnostics](https://github.com/NOAA-GFDL/MDTF-diagnostics).

## Installation and Use

Code is not currently executable; see [mdtf-dagster-demo](https://github.com/tsjackson-noaa/mdtf-dagster-demo) for a working demo.

Dependencies are provided through a conda environment defined in conda_env_dev.yml. To install it, run

> conda env create -f conda_env_dev.yml
> conda activate FRE-dagster-dev

## Disclaimer

This repository is a scientific product and is not an official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.
