

```bash 
    
    Documentation for marshallEngine can be found here: http://marshallEngine.readthedocs.org
    
    Usage:
        marshall init
        marshall clean [-s <pathToSettingsFile>]
        marshall import <survey> [<withInLastDay>] [-s <pathToSettingsFile>]
        marshall lightcurve <transientBucketId> [-s <pathToSettingsFile>]
    
    Options:
        init                  setup the marshallEngine settings file for the first time
        clean                 preform cleanup tasks like updating transient summaries table
        import                import data, images, lightcurves from a feeder survey
        lightcurve            generate a lightcurve for a transient in the marshall database
        transientBucketId     the transient ID from the database
        survey                name of survey to import [panstarrs|atlas|useradded]
        withInLastDay         import transient detections from the last N days (Default 30)
    
        -h, --help                              show this help message
        -v, --version                           show version
        -s, --settings <pathToSettingsFile>     the settings file
    

```
