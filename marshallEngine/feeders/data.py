#!/usr/local/bin/python
# encoding: utf-8
"""
*Baseclass for survey data ingesters*

:Author:
    David Young
"""
from __future__ import print_function
from __future__ import division
from builtins import zip
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
import sys
import os
os.environ['TERM'] = 'vt100'
import csv
import requests
from requests.auth import HTTPBasicAuth
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables, readquery, writequery
from fundamentals import tools


class data(object):
    """
    *This baseclass for the feeder survey data imports*

    **Usage**

    .. todo::

        - create a frankenstein template for importer

    To create a new survey data ingester create a new class using this class as the baseclass:

    ```python
    from ..data import data as basedata
    class data(basedata):
        ....
    ```

    """

    def get_csv_data(
            self,
            url,
            user=False,
            pwd=False):
        """*collect the CSV data from a URL with option to supply basic auth credentials*

        **Key Arguments**

        - ``url`` -- the url to the csv file
        - ``user`` -- basic auth username
        - ``pwd`` -- basic auth password


        **Return**

        - ``csvData`` -- a list of dictionaries from the csv file


        **Usage**

        To get the CSV data for a suvery from a given URL in the marshall settings file run something similar to:

        ```python
        from marshallEngine.feeders.panstarrs.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        )
        csvDicts = ingester.get_csv_data(
            url=settings["panstarrs urls"]["3pi"]["summary csv"],
            user=settings["credentials"]["ps1-3pi"]["username"],
            pwd=settings["credentials"]["ps1-3pi"]["password"]
        )
        ```

        Note you will also be able to access the data via ``ingester.csvDicts`` 

        """
        self.log.debug('starting the ``get_csv_data`` method')

        # DOWNLOAD THE CSV FILE DATA
        try:
            if user:
                response = requests.get(
                    url=url,
                    auth=HTTPBasicAuth(user, pwd)
                )
            else:
                response = requests.get(
                    url=url
                )
            status_code = response.status_code
        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            sys.exit(0)

        if status_code == 502:
            print('HTTP Request failed - status %(status_code)s' % locals())
            print(url)
            self.csvDicts = []
            return self.csvDicts

        if status_code != 200:
            print('HTTP Request failed - status %(status_code)s' % locals())
            sys.exit(0)

        # CONVERT THE RESPONSE TO CSV LIST OF DICTIONARIES
        self.csvDicts = csv.DictReader(
            response.iter_lines(decode_unicode='utf-8'), dialect='excel', delimiter='|', quotechar='"')

        self.log.debug('completed the ``get_csv_data`` method')
        return self.csvDicts

    def _import_to_feeder_survey_table(
            self):
        """*import the list of dictionaries (self.dictList) into the marshall feeder survey table*

        **Return**

        - None


        **Usage**

        ```python
        self._import_to_feeder_survey_table()
        ```

        """
        self.log.debug(
            'starting the ``_import_to_feeder_survey_table`` method')

        if not len(self.dictList):
            return

        # USE dbSettings TO ACTIVATE MULTIPROCESSING
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=self.dictList,
            dbTableName=self.fsTableName,
            dateModified=True,
            dateCreated=True,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        self.log.debug(
            'completed the ``_import_to_feeder_survey_table`` method')
        return None

    def insert_into_transientBucket(
            self,
            importUnmatched=True,
            updateTransientSummaries=True):
        """*insert objects/detections from the feeder survey table into the transientbucket*

        **Key Arguments**

        - ``importUnmatched`` -- import unmatched (new) transients into the marshall (not wanted in some circumstances)
        - ``updateTransientSummaries`` -- update the transient summaries and lightcurves? Can be True or False, or alternatively a specific transientBucketId


        This method aims to reduce crossmatching and load on the database by:

        1. automatically assign the transientbucket id to feeder survey detections where the object name is found in the transientbukcet (no spatial crossmatch required). Copy matched feeder survey rows to the transientbucket.
        2. crossmatch remaining unique, unmatched sources in feeder survey with sources in the transientbucket. Add associated transientBucketIds to matched feeder survey sources. Copy matched feeder survey rows to the transientbucket.
        3. assign a new transientbucketid to any feeder survey source not matched in steps 1 & 2. Copy these unmatched feeder survey rows to the transientbucket as new transient detections.

        **Return**

        - None


        **Usage**

        ```python
        ingester.insert_into_transientBucket()
        ```

        """
        self.log.debug(
            'starting the ``crossmatch_with_transientBucket`` method')

        fsTableName = self.fsTableName

        # 1. automatically assign the transientbucket id to feeder survey
        # detections where the object name is found in the transientbukcet (no
        # spatial crossmatch required). Copy matched feeder survey rows to the
        # transientbucket.
        self._feeder_survey_transientbucket_name_match_and_import()

        # 2. crossmatch remaining unique, unmatched sources in feeder survey
        # with sources in the transientbucket. Add associated
        # transientBucketIds to matched feeder survey sources. Copy matched
        # feeder survey rows to the transientbucket.
        from HMpTy.mysql import add_htm_ids_to_mysql_database_table
        add_htm_ids_to_mysql_database_table(
            raColName="raDeg",
            declColName="decDeg",
            tableName="transientBucket",
            dbConn=self.dbConn,
            log=self.log,
            primaryIdColumnName="primaryKeyId",
            dbSettings=self.settings["database settings"]
        )
        unmatched = self._feeder_survey_transientbucket_crossmatch()

        # 3. assign a new transientbucketid to any feeder survey source not
        # matched in steps 1 & 2. Copy these unmatched feeder survey rows to
        # the transientbucket as new transient detections.
        if importUnmatched:
            self._import_unmatched_feeder_survey_sources_to_transientbucket(
                unmatched)

        # UPDATE OBSERVATION DATES FROM MJDs
        sqlQuery = "call update_transientbucket_observation_dates()"
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # UPDATE THE TRANSIENT BUCKET SUMMARY TABLE IN THE MARSHALL DATABASE
        if updateTransientSummaries:
            if isinstance(updateTransientSummaries, int) and not isinstance(updateTransientSummaries, bool):
                transientBucketId = updateTransientSummaries
            else:
                transientBucketId = False
            from marshallEngine.housekeeping import update_transient_summaries
            updater = update_transient_summaries(
                log=self.log,
                settings=self.settings,
                dbConn=self.dbConn,
                transientBucketId=transientBucketId
            )
            updater.update()

        self.log.debug(
            'completed the ``crossmatch_with_transientBucket`` method')
        return None

    def _feeder_survey_transientbucket_name_match_and_import(
            self):
        """*automatically assign the transientbucket id to feeder survey detections where the object name is found in the transientbukcet (no spatial crossmatch required). Copy feeder survey rows to the transientbucket.*

        **Return**

        - None


        **Usage**

        ```python
        self._feeder_survey_transientbucket_name_match_and_import()
        ```

        """
        self.log.debug(
            'starting the ``_feeder_survey_transientbucket_name_match_and_import`` method')

        fsTableName = self.fsTableName

        # MATCH TRANSIENT BUCKET IDS WITH NAMES FOUND IN FEEDER TABLE, THEN
        # COPY ROWS TO TRANSIENTBUCKET USING COLUMN MATCH TABLE IN DATABASE
        sqlQuery = """CALL `sync_marshall_feeder_survey_transientBucketId`('%(fsTableName)s');""" % locals(
        )

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug(
            'completed the ``_feeder_survey_transientbucket_name_match_and_import`` method')
        return None

    def _feeder_survey_transientbucket_crossmatch(
            self):
        """*crossmatch remaining unique, unmatched sources in feeder survey with sources in the transientbucket & copy matched feeder survey rows to the transientbucket*

        **Return**

        - ``unmatched`` -- a list of the unmatched (i.e. new to the marshall) feeder survey surveys

        """
        self.log.debug(
            'starting the ``_feeder_survey_transientbucket_crossmatch`` method')

        fsTableName = self.fsTableName

        # GET THE COLUMN MAP FOR THE FEEDER SURVEY TABLE
        sqlQuery = u"""
            SELECT * FROM marshall_fs_column_map where fs_table_name = '%(fsTableName)s' and transientBucket_column in ('name','raDeg','decDeg','limitingMag')
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            quiet=False
        )

        columns = {}
        for row in rows:
            columns[row["transientBucket_column"]] = row["fs_table_column"]

        if "raDeg" not in columns:
            print(f"No coordinates to match in the {fsTableName} table")
            return []

        # BUILD QUERY TO GET UNIQUE UN-MATCHED SOURCES
        fs_name = columns["name"]
        self.fs_name = fs_name
        fs_ra = columns["raDeg"]
        fs_dec = columns["decDeg"]
        if 'limitingMag' in columns:
            fs_lim = columns["limitingMag"]
            limitClause = " and %(fs_lim)s = 0 " % locals()
        else:
            limitClause = ""
        sqlQuery = u"""
            select %(fs_name)s, avg(%(fs_ra)s) as %(fs_ra)s, avg(%(fs_dec)s) as %(fs_dec)s from %(fsTableName)s where ingested = 0 %(limitClause)s and %(fs_ra)s is not null and %(fs_dec)s is not null group by %(fs_name)s 
        """ % locals()

        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            quiet=False
        )

        # STOP IF NO MATCHES
        if not len(rows):
            return []

        # SPLIT INTO BATCHES SO NOT TO OVERWHELM MEMORY
        batchSize = 200
        total = len(rows)
        batches = int(old_div(total, batchSize))
        start = 0
        end = 0
        theseBatches = []
        for i in range(batches + 1):
            end = end + batchSize
            start = i * batchSize
            thisBatch = rows[start:end]
            theseBatches.append(thisBatch)

        unmatched = []
        ticker = 0
        for batch in theseBatches:

            fs_name_list = []
            fs_ra_list = []
            fs_dec_list = []
            fs_name_list = [row[fs_name] for row in batch if row[fs_ra]]
            fs_ra_list = [row[fs_ra] for row in batch if row[fs_ra]]
            fs_dec_list = [row[fs_dec] for row in batch if row[fs_ra]]

            ticker += len(fs_name_list)
            print("Matching %(ticker)s/%(total)s sources in the %(fsTableName)s against the transientBucket table" % locals())

            # CONESEARCH TRANSIENT BUCKET FOR PRE-KNOWN SOURCES FROM OTHER
            # SURVEYS
            from HMpTy.mysql import conesearch
            cs = conesearch(
                log=self.log,
                dbConn=self.dbConn,
                tableName="transientBucket",
                columns="transientBucketId, name",
                ra=fs_ra_list,
                dec=fs_dec_list,
                radiusArcsec=3.5,
                separations=True,
                distinct=True,
                sqlWhere="masterIDFlag=1",
                closest=True
            )
            matchIndies, matches = cs.search()

            # CREATE SQL QUERY TO UPDATE MATCHES IN FS TABLE WITH MATCHED
            # TRANSIENTBUCKET IDs
            updates = []
            originalList = matches.list
            originalTotal = len(originalList)

            print("Adding %(originalTotal)s new %(fsTableName)s transient detections to the transientBucket table" % locals())
            if originalTotal:
                updates = []
                updates[:] = ["update " + fsTableName + " set transientBucketId = " + str(o['transientBucketId']) +
                              " where " + fs_name + " = '" + str(fs_name_list[m]) + "' and transientBucketId is null;" for m, o in zip(matchIndies, originalList)]
                updates = ("\n").join(updates)
                writequery(
                    log=self.log,
                    sqlQuery=updates,
                    dbConn=self.dbConn
                )

            # RETURN UNMATCHED TRANSIENTS
            for i, v in enumerate(fs_name_list):
                if i not in matchIndies:
                    unmatched.append(v)

        # COPY MATCHED ROWS TO TRANSIENTBUCKET
        self._feeder_survey_transientbucket_name_match_and_import()

        self.log.debug(
            'completed the ``_feeder_survey_transientbucket_crossmatch`` method')
        return unmatched

    def _import_unmatched_feeder_survey_sources_to_transientbucket(
            self,
            unmatched):
        """*assign a new transientbucketid to any feeder survey source not yet matched in steps. Copy these unmatched feeder survey rows to the transientbucket as new transient detections.*

        **Key Arguments**

        - ``unmatched`` -- the remaining unmatched feeder survey object names.

        """
        self.log.debug(
            'starting the ``_import_unmatched_feeder_survey_sources_to_transientbucket`` method')

        if not len(unmatched):
            return None

        fsTableName = self.fsTableName
        fs_name = self.fs_name

        # READ MAX TRANSIENTBUCKET ID FROM TRANSIENTBUCKET
        sqlQuery = u"""
            select max(transientBucketId) as maxId from transientBucket
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        if not len(rows) or not rows[0]["maxId"]:
            maxId = 1
        else:
            maxId = rows[0]["maxId"] + 1

        # ADD NEW TRANSIENTBUCKETIDS TO FEEDER SURVEY TABLE
        updates = []
        newTransientBucketIds = []
        for u in unmatched:
            update = "update " + fsTableName + " set transientBucketId = " + \
                str(maxId) + " where " + fs_name + " = '" + str(u) + "';"
            updates.append(update)
            newTransientBucketIds.append(str(maxId))
            maxId += 1
        updates = ("\n").join(updates)
        writequery(
            log=self.log,
            sqlQuery=updates,
            dbConn=self.dbConn
        )

        # COPY FEEDER SURVEY ROWS TO TRANSIENTBUCKET
        self._feeder_survey_transientbucket_name_match_and_import()

        # SET THE MASTER ID FLAG FOR ALL NEW TRANSIENTS IN THE TRANSIENTBUCKET
        newTransientBucketIds = (",").join(newTransientBucketIds)
        sqlQuery = """update transientBucket set masterIDFlag = 1 where primaryKeyId in (select * from (
SELECT 
    primaryKeyId
FROM
    transientBucket
WHERE
    transientBucketId IN (%(newTransientBucketIds)s)
GROUP BY transientBucketId) as a )""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug(
            'completed the ``_import_unmatched_feeder_survey_sources_to_transientbucket`` method')
        return None

    # use the tab-trigger below for new method
    def clean_up(
            self):
        """*A few tasks to finish off the ingest*

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
        self.log.debug('starting the ``clean_up`` method')

        sqlQueries = [
            "insert into sherlock_classifications (transient_object_id) select distinct transientBucketId from transientBucketSummaries ON DUPLICATE KEY UPDATE  transient_object_id = transientBucketId;",
            "CALL update_transient_akas(); "
        ]

        for sqlQuery in sqlQueries:
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

        self.log.debug('completed the ``clean_up`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
