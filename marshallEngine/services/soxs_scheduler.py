#!/usr/bin/env python
# encoding: utf-8
"""
*request, update and read status of observation blocks with the SOXS Scheduler*

:Author:
    Marco Landoni & David Young

:Date Created:
    November 17, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery, writequery
import requests
import json


class soxs_scheduler(object):
    """
    *request, update and read status of observation blocks with the SOXS Scheduler *

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- marshall DB connection
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initialise the scheduler object, use the following:

    ```python
    from marshallEngine.services import soxs_scheduler
    schr = soxs_scheduler(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'soxs_scheduler' object")
        self.settings = settings
        self.dbConn = dbConn
        self.baseurl = self.settings["scheduler_baseurl"]

        # Initial Actions
        self.update_scheduler_ob_table()

        return None

    def request_all_required_auto_obs(
            self):
        """*request OBs for all new transients from scheduler*

        **Return:**
            - ``passList, failedIds`` -- 2 lists, one of transientBucketId that have been assigned an OB and the other of those that have failed to be assigned an OB

        **Usage:**

        ```python
        schr.request_all_required_auto_obs()
        ```
        """
        self.log.debug('starting the ``request_all_required_auto_obs`` method')

        # GENERATE THE LIST OF TRANSIENTS NEEDING AN OB
        sqlQuery = f"""
            SELECT 
            t.transientBucketId, s.targetName, t.raDeg, t.decDeg, s.latestMag, s.latestMagFilter
            FROM
            scheduler_obs s,
            transientbucketsummaries t
            WHERE
            t.transientBucketId = s.transientBucketId AND s.OB_ID is null
        """

        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn)
        passList = []
        failedIds = []

        for r in rows:
            # GET FIRST CHARACTER OF FILTER
            ffilter = r['latestMagFilter']
            if not ffilter:
                ffilter = ""
            else:
                ffilter = ffilter[0]
            try:
                transientBucketId = r['transientBucketId']
                obid = self._create_single_auto_ob(
                    transientBucketId=transientBucketId,
                    target_name=r['targetName'],
                    raDeg=r['raDeg'],
                    decDeg=r['decDeg'],
                    magnitude_list=[
                        [ffilter, float(r['latestMag'])]],
                    existenceCheck=False
                )
                if -1 == obid:
                    failedIds.append(transientBucketId)
                else:
                    passList.append(transientBucketId)
            except Exception as e:
                print(e)
                failedIds.append(transientBucketId)
                pass

        print(f"{len(passList)} OBs added to the scheduler, {len(failedIds)} failed to be added.")

        self.log.debug(
            'completed the ``request_all_required_auto_obs`` method')
        return (passList, failedIds)

    def update_scheduler_ob_table(
            self):
        """*sync the scheduler ob tables with the core marshall tables to bring it up-to-date*

        **Usage:**

        ```python
        schr.update_scheduler_ob_table()
        ```
        """
        self.log.debug('starting the ``update_scheduler_ob_table`` method')

        sqlQuery = f"""
            call update_scheduler_obs();
        """
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug('completed the ``update_scheduler_ob_table`` method')
        return

    def _create_single_auto_ob(
            self,
            transientBucketId,
            target_name,
            raDeg,
            decDeg,
            magnitude_list,
            existenceCheck=True):
        """*request to generate a single auto ob from the soxs scheduler*

        **Key Arguments:**
            - ``transientBucketId`` -- the transients ID from the marshall.
            - ``target_name`` -- the master name of the target.
            - ``raDeg`` -- the target RA.
            - ``decDeg`` -- the target declination.
            - ``magnitude_list`` -- the list of lists of magnitudes. [['g':19.06],['r':19.39]]
            - ``existenceCheck`` -- check local database to see if OB already exists for this transient. Default *True*.

        **Return:**
            - ``obid`` -- the ID of the OB generated by the scheduler
        """
        self.log.debug('starting the ``_create_single_auto_ob`` method')

        if existenceCheck:
            # CHECK FOR EXISTENCE OF OB IN LOCAL DATABASE
            sqlQuery = f"""
                select ob_id from scheduler_obs where transientBucketId = {transientBucketId}
            """
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
            if len(rows) and rows[0]['ob_id']:
                obid = rows[0]['ob_id']
                self.log.warning(
                    f'You are trying to create an OB for transient {transientBucketId}, but it is already assigned OBID = {obid}')
                return obid

        try:
            schd_status_code = 0
            http_status_code = 500
            response = requests.post(
                url="https://soxs-scheduler-pwoxq.ondigitalocean.app/createAutoOB",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                },
                data=json.dumps({
                    "magnitude_list": magnitude_list,
                    "declination": float(decDeg),
                    "target_name": target_name,
                    "transientBucketID": transientBucketId,
                    "right_ascension": float(raDeg)
                })
            )

            response = response.json()
            schd_status_code = response["status"]
            content = response["data"]
            http_status_code = content["status_code"]

        except requests.exceptions.RequestException:
            self.log.error(
                'HTTP Request failed to scheduler `createAutoOB` resource failed')
        if http_status_code != 201 or schd_status_code != 1:
            error = content["payload"]
            print(f"createAutoOB failed with error: '{error}'")
            return -1

        obid = content["payload"][0]['OB_ID']

        # UPDATE THE SCHEDULER TABLE
        sqlQuery = f"""
            update scheduler_obs set ob_id = {obid} where transientBucketId = {transientBucketId};
        """
        rows = writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug('completed the ``_create_single_auto_ob`` method')
        return obid

    # use the tab-trigger below for new method
    # xt-class-method
