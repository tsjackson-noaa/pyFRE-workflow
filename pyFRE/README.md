# pyFRE package

This is work in progress to port FRE's Perl code to python, starting with /bin/frepp and the library modules used by that script. This would be necessary for this approach because frepp calls itself recursively, both to generate the runscript for processing the following years and for analysis and refineDaig scripts for the current year. 

All code in this directory is based on FRE commit [11815a0ccd7c90c80d12044208fb5b3c1999a488](https://github.com/NOAA-GFDL/FRE/commit/11815a0ccd7c90c80d12044208fb5b3c1999a488).
