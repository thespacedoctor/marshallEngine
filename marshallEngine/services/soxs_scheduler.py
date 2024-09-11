#!/usr/bin/env python
# encoding: utf-8
"""
*request, update and read status of observation blocks with the SOXS Scheduler*

:Author:
    Marco Landoni & David Young

:Date Created:
    November 17, 2021
"""
import json
import requests
from fundamentals.mysql import readquery, writequery
from fundamentals import tools
from builtins import object
import sys
import os
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
os.environ['TERM'] = 'vt100'


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
            t.transientBucketId = s.transientBucketId AND s.OB_ID is null and s.latestMag < 19.0
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
                url=f"{self.baseurl}/createAutoOB",
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

    def collect_schedule_obs_statuses(
            self):
        """*collect schedule obs statuses from the scheduler*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**

        ```python
        usage code 
        ```

        ---

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - write a command-line tool for this method
            - update package tutorial with command-line tool info if needed
        ```
        """
        self.log.debug('starting the ``collect_schedule_obs_statuses`` method')

        try:
            response = requests.get(
                url=f"{self.baseurl}/obMarshallShort",
            )
            data = response.json()

        except requests.exceptions.RequestException:
            self.log.debug('HTTP Request failed on obMarshallShort')

        try:
            dictList = [{**d, 'ESO_OB_Status': d['ESO_OB_Status'] if d['ESO_OB_Status'] is not None else 'Not Available'} for d in data['data']['payload']]

            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=dictList,
                dbTableName="scheduler_obs",
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )
        except Exception as e:
            print(e)
            self.log.debug('Update failed. Exception caught: ' + str(e))

        self.log.debug('completed the ``collect_schedule_obs_statuses`` method')
        return None

    def remove_classified_obs(self):
        sqlQuery = 'SELECT t.transientBucketId , so.OB_ID FROM  pesstoobjects AS t , scheduler_obs AS so  WHERE t.classifiedFlag = 1 AND so.transientBucketId = t.transientBucketId AND so.autoOB = 1 and so.OB_ID is not null'
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        for r in rows:
            #print(r)

            # SEND TO SCHEDULER A DELETE COMMAND

            response = requests.delete(
                url=f"{self.baseurl}/deleteOB",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                },
                data=json.dumps({
                    "OB_ID": r['OB_ID']
                })
            )

            sqlQuery = "UPDATE scheduler_obs SET scheduler_obs.autoOB = -1, scheduler_obs.ESO_OB_Status = 'Deleted' WHERE OB_ID = " + str(r['OB_ID']) + ";"
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
    #This method should be used ONLY in debug
    def removeOlderOBs(self):
        sqlQuery = "SELECT OB_ID FROM  scheduler_obs  WHERE `dateCreated` < date('2024-09-04') AND OB_ID is not NULL;"
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        #print(rows)


        for r in rows:
            #print(r)

            # SEND TO SCHEDULER A DELETE COMMAND

            response = requests.delete(
                url=f"{self.baseurl}/deleteOB",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                },
                data=json.dumps({
                    "OB_ID": r['OB_ID']
                })
            )

            print('OB ' + str(r['OB_ID']) + 'Deletet with response: ' + str(response))


    # use the tab-trigger below for new method
    # xt-class-method
