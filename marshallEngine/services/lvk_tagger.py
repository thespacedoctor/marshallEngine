#!/usr/bin/env python
# encoding: utf-8
"""
*match marshall transients against LVK skymap and add metadata into database*

:Author:
    David Young

:Date Created:
    September  7, 2023
"""
from fundamentals import tools
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'


class lvk_tagger(object):
    """
    *match marshall transients against LVK skymap and add metadata into database*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``dbConn`` -- marshall DB connection

    **Usage:**

    To set up your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate and run a `lvk_tagger` object, use the following:

    ```python
    from marshallEngine.services import lvk_tagger
    lvk = lvk_tagger(
        log=log,
        settings=settings,
        dbConn=dbConn
    )
    lvk.tag()
    ```
    """

    def __init__(
            self,
            log,
            settings=False,
            dbConn=False
    ):
        self.log = log
        log.debug("instantiating a new 'lvk_tagger' object")
        self.settings = settings
        self.dbConn = dbConn

        return None

    def tag(self):
        """
        *tag the transients with their locations on the LVK skymaps*
        """
        self.log.debug('starting the ``tag`` method')

        mapTransDF = self.collect_recent_event_maps()
        if mapTransDF is not None:
            matchedTransientsDF = self.match_transients_against_maps(mapTransDF)
            if len(matchedTransientsDF.index):
                self.add_tagged_transients_to_database(matchedTransientsDF)

        self.log.debug('completed the ``tag`` method')
        return None

    def collect_recent_event_maps(
            self):
        """*collect the maps needed to tag transients against (recent maps and those never previously matched against)*

        **Return:**
            - ``mapTransDF`` -- a grouped dataframe (transients group by map they are in temporal coincidence with)

        **Usage:**

        ```python
        mapTransDF = lvk.collect_recent_event_maps()
        ```
        """
        self.log.debug('starting the ``collect_recent_event_maps`` method')

        from fundamentals.mysql import readquery
        import pandas as pd

        sqlQuery = f"""
            SELECT
                l.primaryId as mapId, t.transientBucketId, t.masterName, t.raDeg, t.decDeg, TO_SECONDS(t.earliestDetection)/(3600*24)-678941 as tMJD, TO_SECONDS(t.earliestDetection)/(3600*24)-678941-l.mjd_obs as daysSinceEvent, l.superevent_id, l.map
            FROM
                lvk_alerts l,
                transientbucketsummaries t
            WHERE
                t.earliestDetection BETWEEN DATE_ADD(l.date_obs, INTERVAL - 12 HOUR) AND DATE_ADD(l.date_obs, INTERVAL 7 DAY) and (t.dateAdded > l.dateLastMatched or t.dateAdded is null or l.dateLastMatched  is null);
        """

        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        mapTransDF = pd.DataFrame(rows)

        if not (len(mapTransDF.index)):
            print("No new transients to annotate with skytag")
            return None

        # GROUP RESULTS BY MAP
        mapTransDF = mapTransDF.groupby(['map'])

        self.log.debug('completed the ``collect_recent_event_maps`` method')
        return mapTransDF

    def match_transients_against_maps(
            self,
            mapTransDF):
        """*match transients against maps and return matching data*

        **Key Arguments:**
            - ``mapTransDF`` -- a grouped dataframe (transients group by map they are in temporal coincidence with)

        **Return:**
            - ``matchedTransientsDF`` -- a dataframe of transients matched within the 90% likelihood contour of a skymap

        **Usage:**

        ```python
        matchedTransientsDF = self.match_transients_against_maps(mapTransDF)
        ```
        """
        self.log.debug('starting the ``match_transients_against_maps`` method')

        from skytag.commonutils import prob_at_location
        import numpy as np
        import pandas as pd

        # ITERATE OVER MAPS & MATCH TRANSIENTS
        matchedTransientsDF = []
        for mapPath, df in mapTransDF:

            prob, deltas = prob_at_location(
                log=self.log,
                ra=list(df["raDeg"].values),
                dec=list(df["decDeg"].values),
                mjd=list(df["tMJD"].values.astype(float)),
                mapPath=mapPath[0]
            )

            # CONTOUR TO HIGHEST 10%
            df["contour"] = np.ceil(np.array(prob) / 10) * 10
            matchedTransientsDF.append(df)

        matchedTransientsDF = pd.concat(matchedTransientsDF, ignore_index=True)

        # DROP TRANSIENTS OUTSIDE 90% CONTOURS
        mask = (matchedTransientsDF['contour'] < 100)
        matchedTransientsDF = matchedTransientsDF.loc[mask]

        self.log.debug('completed the ``match_transients_against_maps`` method')
        return matchedTransientsDF

    def add_tagged_transients_to_database(
            self,
            matchedTransientsDF):
        """*add the tagged transients back to the marshall database*

        **Key Arguments:**
            - ``matchedTransientsDF`` -- a dataframe of transients matched within the 90% likelihood contour of a skymap

        **Usage:**

        ```python
        lvk.add_tagged_transients_to_database(matchedTransientsDF)
        ```
        """
        self.log.debug('starting the ``add_tagged_transients_to_database`` method')

        from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
        from fundamentals.mysql import writequery

        sqlQuery = f"""CREATE TABLE IF NOT EXISTS `lvk_skytag` (
          `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
          `dateCreated` datetime NOT NULL DEFAULT current_timestamp(),
          `dateLastModified` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          `contour` double NOT NULL,
          `daysSinceEvent` double NOT NULL,
          `mapId` tinyint(4) NOT NULL,
          `superevent_id` varchar(100) NOT NULL,
          `transientBucketId` int(11) NOT NULL,
          PRIMARY KEY (`primaryId`),
          UNIQUE KEY `mapid_transientbucketid` (`mapId`,`transientBucketId`),
          KEY `idx_transientBucketId` (`transientBucketId`)
        ) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
        """
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        matchedTransientsDF = matchedTransientsDF[["mapId", "transientBucketId", "superevent_id", "contour", "daysSinceEvent"]]

        # CONVERT TO A LIST OF DICTIONARIES
        matchedTransients = matchedTransientsDF.to_dict('records')

        # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF DICTIONARIES INTO DATABASE
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=matchedTransients,
            dbTableName="lvk_skytag",
            uniqueKeyList=["mapId", "transientBucketId"],
            dateModified=True,
            dateCreated=True,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        mapIds = (" ,").join(list(matchedTransientsDF["mapId"].values.astype("str")))

        # UPDATE THE ALERT TABLE WITH DATELASTMATCHED
        sqlQuery = f"""update lvk_alerts set dateLastMatched = NOW() where primaryId in ({mapIds})
        """
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug('completed the ``add_tagged_transients_to_database`` method')
        return None
