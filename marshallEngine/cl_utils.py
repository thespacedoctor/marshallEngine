#!/usr/bin/env python
# encoding: utf-8
"""
Documentation for marshallEngine can be found here: http://marshallEngine.readthedocs.org

Usage:
    marshall init
    marshall clean [-s <pathToSettingsFile>]
    marshall import <survey> [<withInLastDay>] [-s <pathToSettingsFile>]
    marshall lightcurve <transientBucketId> [-s <pathToSettingsFile>]
    marshall refresh <transientBucketId>  [-s <pathToSettingsFile>]

Options:
    init                  setup the marshallEngine settings file for the first time
    clean                 preform cleanup tasks like updating transient summaries table
    import                import data, images, lightcurves from a feeder survey
    refresh               update the cached metadata for a given transient
    lightcurve            generate a lightcurve for a transient in the marshall database
    transientBucketId     the transient ID from the database
    survey                name of survey to import [panstarrs|atlas|useradded]
    withInLastDay         import transient detections from the last N days (Default 30)

    -h, --help                              show this help message
    -v, --version                           show version
    -s, --settings <pathToSettingsFile>     the settings file
"""
from __future__ import print_function
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from docopt import docopt
from fundamentals import tools, times
from subprocess import Popen, PIPE, STDOUT


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    *The main function used when `cl_utils.py` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING",
        options_first=False,
        projectName="marshall",
        defaultSettingsFile=True
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
    a = {}
    for arg, val in list(arguments.items()):
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        a[varname] = val
        if arg == "--dbConn":
            dbConn = val
            a["dbConn"] = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = times.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE cl_utils.py AT %s' %
        (startTime,))

    init = a["init"]
    clean = a["clean"]
    iimport = a["import"]
    lightcurve = a["lightcurve"]
    transientBucketId = a["transientBucketId"]
    survey = a["survey"]
    withInLastDay = a["withInLastDay"]
    settingsFlag = a["settingsFlag"]

    # set options interactively if user requests
    if "interactiveFlag" in a and a["interactiveFlag"]:

        # load previous settings
        moduleDirectory = os.path.dirname(__file__) + "/resources"
        pathToPickleFile = "%(moduleDirectory)s/previousSettings.p" % locals()
        try:
            with open(pathToPickleFile):
                pass
            previousSettingsExist = True
        except:
            previousSettingsExist = False
        previousSettings = {}
        if previousSettingsExist:
            previousSettings = pickle.load(open(pathToPickleFile, "rb"))

        # x-raw-input
        # x-boolean-raw-input
        # x-raw-input-with-default-value-from-previous-settings

        # save the most recently used requests
        pickleMeObjects = []
        pickleMe = {}
        theseLocals = locals()
        for k in pickleMeObjects:
            pickleMe[k] = theseLocals[k]
        pickle.dump(pickleMe, open(pathToPickleFile, "wb"))

    if a["init"]:
        from os.path import expanduser
        home = expanduser("~")
        filepath = home + "/.config/marshallEngine/marshallEngine.yaml"
        try:
            cmd = """open %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass
        try:
            cmd = """start %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass
        return

    # CALL FUNCTIONS/OBJECTS
    # DEFAULT VALUES
    if not withInLastDay:
        withInLastDay = 30

    # CALL FUNCTIONS/OBJECTS
    if clean:
        # RESCUE ORPHANED TRANSIENTS - NO MASTER ID FLAG
        print("rescuing orphaned transients")
        from fundamentals.mysql import writequery

        procedureNames = [
            "update_transients_with_no_masteridflag()",
            "insert_new_transients_into_transientbucketsummaries()",
            "resurrect_objects()",
            "update_sherlock_xmatch_counts()",
            "update_inbox_auto_archiver()",
            "update_transient_akas(0)"
        ]

        # CALL EACH PROCEDURE
        for p in procedureNames:
            sqlQuery = "CALL %(p)s;" % locals()
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )

        # UPDATE THE TRANSIENT BUCKET SUMMARY TABLE IN THE MARSHALL DATABASE
        from marshallEngine.housekeeping import update_transient_summaries
        updater = update_transient_summaries(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).update()

    if iimport:
        if survey.lower() == "panstarrs":
            from marshallEngine.feeders.panstarrs.data import data
            from marshallEngine.feeders.panstarrs import images
        if survey.lower() == "atlas":
            from marshallEngine.feeders.atlas.data import data
            from marshallEngine.feeders.atlas import images
        if survey.lower() == "useradded":
            from marshallEngine.feeders.useradded.data import data
            from marshallEngine.feeders.useradded import images
        if survey.lower() == "tns":
            from marshallEngine.feeders.tns.data import data
            from marshallEngine.feeders.tns import images
        if survey.lower() == "ztf":
            from marshallEngine.feeders.ztf.data import data
            from marshallEngine.feeders.ztf import images
        if survey.lower() == "atels" or survey.lower() == "atel":
            from marshallEngine.feeders.atels.data import data
            from marshallEngine.feeders.atels import images
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=withInLastDay)
        cacher = images(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).cache(limit=3000)

        from marshallEngine.services import panstarrs_location_stamps
        ps_stamp = panstarrs_location_stamps(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).get()

    if lightcurve:
        from marshallEngine.lightcurves import marshall_lightcurves
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=transientBucketId
        )
        filepath = lc.plot()
        print("The lightcurve plot for transient %(transientBucketId)s can be found here: %(filepath)s" % locals())

    if a["refresh"]:
        from marshallEngine.housekeeping import update_transient_summaries
        updater = update_transient_summaries(
            log=log,
            settings=settings,
            dbConn=dbConn,
            transientBucketId=transientBucketId
        ).update()

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = times.get_now_sql_datetime()
    runningTime = times.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE cl_utils.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return

if __name__ == '__main__':
    main()
