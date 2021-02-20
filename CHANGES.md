
## Marshall Engine Release Notes

**v1.0.7 - February 20, 2021**

**ENHANCEMENT**: Added cleanup function at end of ingests so objects appear in inbox quicker and akas are updated more frequently
**REFACTORING**: Reduced the crossmatch radius from 7 to 4 arcsec (it is easier to merge than split transients later on)

**v1.0.6 - January 29, 2021**

**FEATURE**: match transients to astronotes (not yet visualised in the webapp)  
**ENHANCEMENT**: HTM indexing added the transientBucketSummaries table so we can spatially crossmatch   

**v1.0.5 - January 13, 2021**

**REFACTORING**: atel parsing and matching within the marshall database now upgraded

**v1.0.4 - January 11, 2021**

**REFACTORING**: some database schema changes for latest version of Sherlock to run.  
**FIXED**: added a function to recalculate sherlock original radii for merged sources. Fixes webapp visualisation.  
**FIXED**: reduced number of ATLAS LC to be generated in a single batch.  

**v1.0.3 - December 10, 2020**

**REFACTORING**: reading settings from marshall config folder instead of marshallEngine  
**FIXED**: the save location of lightcurve files was resulting in files not being found in webapp

**v1.0.2 - November 14, 2020**

* **feature** PS2 survey added as a new import (discoveries, lightcurves and stamps)

**v1.0.1 - October 20, 2020**

**REFACTORING**: added time filtering on ATLAS summary CSV file (thanks Ken)
**FIXED**: small fix to panstamp location map downloader

**v1.0.0 - May 28, 2020**

* Now compatible with Python 3.*
