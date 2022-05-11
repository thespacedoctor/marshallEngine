
## Marshall Engine Release Notes  

**v1.0.10 - May 7, 2021** 

* **ENHANCEMENT**: adding tns-marker into the TNS search requests
* **FIXED**: doc fixes
* **FIXED**: fixed atlas and panstarrs image stamp to download from new URLs

**v1.0.9 - May 7, 2021**  

* **REFACTOR**: resurrection code optimised to resurface a few more objects
* **REFACTOR**: some MySQL queries rewritten and table indexes added to optimise common queries 
* **REFACTOR**: ATLAS forced photometry plotting code refactored to bring plots in-line with those produced by ATLAS forced photometry server. Biggest improvement is the more robust clipping of outlying data points.

**v1.0.8 - March 17, 2021**  
  
* **REFACTOR**: Occasional check to make sure all akas are set (i.e. more than just for transients discovered in the last 3 weeks).
* **FIXED**: Links to the ASASSN Sky Patrol now added to all TNS reported ASASSN transient names (credentials discoverable in hover-over tool-tip)
* **FIXED**: ATel comments where getting added correctly to associated object but ticket "ATel" drop-up menu was missing some ATel links occasionally.  
  
**v1.0.7 - February 20, 2021**  
  
* **ENHANCEMENT**: Added cleanup function at end of ingests so objects appear in inbox quicker and akas are updated more frequently  
* **REFACTORING**: Reduced the crossmatch radius from 7 to 4 arcsec (it is easier to merge than split transients later on)  
  
**v1.0.6 - January 29, 2021**  
  
* **FEATURE**: match transients to astronotes (not yet visualised in the webapp)  
* **ENHANCEMENT**: HTM indexing added the transientBucketSummaries table so we can spatially crossmatch   
  
**v1.0.5 - January 13, 2021**  
  
* **REFACTORING**: atel parsing and matching within the marshall database now upgraded  
  
**v1.0.4 - January 11, 2021**  
  
* **REFACTORING**: some database schema changes for latest version of Sherlock to run.  
* **FIXED**: added a function to recalculate sherlock original radii for merged sources. Fixes webapp visualisation.  
* **FIXED**: reduced number of ATLAS LC to be generated in a single batch.  

**v1.0.3 - December 10, 2020**  

* **REFACTORING**: reading settings from marshall config folder instead of marshallEngine  
* **FIXED**: the save location of lightcurve files was resulting in files not being found in webapp  

**v1.0.2 - November 14, 2020**  

* **feature** PS2 survey added as a new import (discoveries, lightcurves and stamps)  

**v1.0.1 - October 20, 2020**  

* **REFACTORING**: added time filtering on ATLAS summary CSV file (thanks Ken)  
* **FIXED**: small fix to panstamp location map downloader  

**v1.0.0 - May 28, 2020**  

* Now compatible with Python 3.*    * 
