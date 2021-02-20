#!/usr/local/bin/python
# encoding: utf-8
"""
*import the useradded stream into the marshall*

:Author:
    David Young
"""
from __future__ import print_function
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from ..data import data as basedata
from astrocalc.times import now
from fundamentals.mysql import readquery
from marshallEngine.housekeeping import update_transient_summaries


class data(basedata):
    """
    *Import the useradded transient data into the marshall database*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the marshall database connection
    - ``settings`` -- the settings dictionary


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a data object, use the following:

    ```python
    from marshallEngine.feeders.useradded.data import data
    ingester = data(
        log=log,
        settings=settings,
        dbConn=dbConn
    ).ingest(withinLastDays=withInLastDay)
    ```

    """
    # Initialisation

    def __init__(
            self,
            log,
            dbConn,
            settings=False,
    ):
        self.log = log
        log.debug("instansiating a new 'data' object")
        self.settings = settings
        self.dbConn = dbConn

        self.fsTableName = "fs_user_added"
        self.survey = "useradded"

        # xt-self-arg-tmpx

        return None

    def ingest(
            self,
            withinLastDays):
        """*Ingest the data into the marshall feeder survey table*

        **Key Arguments**

        - ``withinLastDays`` -- within the last number of days. *Default: 50*

        """
        self.log.info('starting the ``ingest`` method')

        allLists = []

        self.dictList = allLists
        self._import_to_feeder_survey_table()

        self.insert_into_transientBucket(
            updateTransientSummaries=False)

        sqlQuery = u"""
            select transientBucketId from fs_user_added where transientBucketId is not null order by dateCreated desc limit 1
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        if len(rows):
            transientBucketId = rows[0]["transientBucketId"]
            print(transientBucketId)
        else:
            transientBucketId = False

        # UPDATE THE TRANSIENT BUCKET SUMMARY TABLE IN THE MARSHALL DATABASE
        updater = update_transient_summaries(
            log=self.log,
            settings=self.settings,
            dbConn=self.dbConn,
            transientBucketId=transientBucketId
        )
        updater.update()

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.info('completed the ``ingest`` method')
        return None

    def _clean_data_pre_ingest(
            self,
            surveyName,
            withinLastDays=False):
        """*clean up the list of dictionaries containing the useradded data, pre-ingest*

        **Key Arguments**

        - ``surveyName`` -- the useradded survey name
        -  ``withinLastDays`` -- the lower limit of observations to include (within the last N days from now). Default *False*, i.e. no limit


        **Return**

        - ``dictList`` -- the cleaned list of dictionaries ready for ingest


        **Usage**

        To clean the data from the useradded survey:

        ```python
        dictList = ingesters._clean_data_pre_ingest(surveyName="useradded")
        ```

        Note you will also be able to access the data via ``ingester.dictList``

        """
        self.log.info('starting the ``_clean_data_pre_ingest`` method')

        self.dictList = []

        # CALC MJD LIMIT
        if withinLastDays:
            mjdLimit = now(
                log=self.log
            ).get_mjd() - float(withinLastDays)

        for row in self.csvDicts:
            # IF NOW IN THE LAST N DAYS - SKIP
            if withinLastDays and float(row["mjd_obs"]) < mjdLimit:
                continue

            # MASSAGE THE DATA IN THE INPT FORMAT TO WHAT IS NEEDED IN THE
            # FEEDER SURVEY TABLE IN THE DATABASE
            thisDictionary = {}
            # thisDictionary["candidateID"] = row["ps1_designation"]
            # ...

            self.dictList.append(thisDictionary)

        self.log.info('completed the ``_clean_data_pre_ingest`` method')
        return self.dictList

    # use the tab-trigger below for new method
    # xt-class-method
