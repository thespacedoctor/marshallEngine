#!/usr/local/bin/python
# encoding: utf-8
"""
*import the tns stream into the marshall*

:Author:
    David Young
"""
from builtins import zip
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from ..data import data as basedata
from astrocalc.times import now
from transientNamer import search
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables


class data(basedata):
    """
    *Import the tns transient data into the marshall database*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the marshall database connection
    - ``settings`` -- the settings dictionary


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a data object, use the following:

    ```python
    from marshallEngine.feeders.tns.data import data
    ingester = data(
        log=log,
        settings=settings,
        dbConn=dbConn
    ).ingest()   
    ```

    """

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
        self.fsTableName = "tns_sources"
        self.survey = "tns"

        # xt-self-arg-tmpx

        return None

    def ingest(
            self,
            withinLastDays=False):
        """*Ingest the data into the marshall feeder survey table*

        **Key Arguments**

        - ``withinLastDays`` -- note this will be handle by the transientNamer import to the database

        """
        self.log.debug('starting the ``ingest`` method')

        # UPDATE THE TNS SPECTRA TABLE WITH EXTRA INFOS
        from fundamentals.mysql import writequery
        sqlQuery = """CALL `update_tns_tables`();""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # PARSE TNS
        tns = search(
            log=self.log,
            discInLastDays=withinLastDays
        )

        lists = [tns.sources, tns.photometry, tns.files, tns.spectra]
        tableNames = ["tns_sources", "tns_photometry",
                      "tns_files", "tns_spectra"]

        for l, t in zip(lists, tableNames):
            # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
            # DICTIONARIES INTO DATABASE
            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=l,
                dbTableName=t,
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )

        # INSERT THE SOURCES TABLE
        self.insert_into_transientBucket()

        # NOW THE SPECTRA TABLE
        self.fsTableName = "tns_spectra"
        self.survey = "tns"
        self.insert_into_transientBucket(importUnmatched=False)

        # NOW THE PHOTOMETRY TABLE
        self.fsTableName = "tns_photometry"
        self.survey = "tns"
        self.insert_into_transientBucket(importUnmatched=False)

        # ALSO MATCH NEW ASTRONOTES
        sqlQuery = """CALL sync_marshall_feeder_survey_transientBucketId('astronotes_transients');""" % locals(
        )
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.debug('completed the ``ingest`` method')
        return None
