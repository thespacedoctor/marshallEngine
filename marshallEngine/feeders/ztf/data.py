#!/usr/local/bin/python
# encoding: utf-8
"""
*import the ZTF stream into the marshall*

:Author:
    David Young
"""
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from ..data import data as basedata
from astrocalc.times import now
from fundamentals.mysql import writequery


class data(basedata):
    """
    *Import the ZTF transient data into the marshall database*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the marshall database connection
    - ``settings`` -- the settings dictionary


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a data object, use the following:

    ```python
    from marshallEngine.feeders.ztf.data import data
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

        self.fsTableName = "fs_ztf"
        self.survey = "ZTF"

        # xt-self-arg-tmpx

        return None

    def ingest(
            self,
            withinLastDays):
        """*Ingest the data into the marshall feeder survey table*

        **Key Arguments**

        - ``withinLastDays`` -- within the last number of days. *Default: 50*

        """
        self.log.debug('starting the ``ingest`` method')

        allLists = []

        sqlQuery = """call update_fs_ztf()""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # self._import_to_feeder_survey_table()
        self.insert_into_transientBucket()

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.debug('completed the ``ingest`` method')
        return None
