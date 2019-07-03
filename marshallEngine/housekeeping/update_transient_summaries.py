#!/usr/local/bin/python
# encoding: utf-8
"""
*Update the transient summaries table in the marshall database with the top-level metadata*

:Author:
    David Young

:Date Created:
    June 24, 2019
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import writequery, readquery
from astropy import units as u
from astropy.coordinates import SkyCoord
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
from astrocalc.distances import converter


class update_transient_summaries():
    """
    *Update the transient summaries table in the marshall database*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``dbConn`` -- the marshall database connection

    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

        To initiate a update_transient_summaries object, use the following:

        .. todo::

            - add a tutorial about ``update_transient_summaries`` to documentation

        .. code-block:: python 

            from marshallEngine.housekeeping import update_transient_summaries
            updater = update_transient_summaries(
                log=log,
                settings=settings,
                dbConn=dbConn
            ).update()  
    """
    # Initialisation

    def __init__(
            self,
            log,
            dbConn,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'update_transient_summaries' object")
        self.settings = settings
        self.dbConn = dbConn
        self.transientBucketIds = []
        # xt-self-arg-tmpx

        # UPDATE OBSERVATION DATES FROM MJDs
        sqlQuery = "call update_transientbucket_observation_dates()"
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        print "Updating the transientbucket summary table"
        # UPDATE TRANSIENT BUCKET SUMMARIES (IN MYSQL)
        sqlQuery = "call update_transientbucketsummaries()"
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        return None

    def update(self):
        """
        *Update the transient summaries table in the marshall database*

        **Return:**
            - ``update_transient_summaries``

        **Usage:**

        .. code-block:: python 

            from marshallEngine.housekeeping import update_transient_summaries
            updater = update_transient_summaries(
                log=log,
                settings=settings,
                dbConn=dbConn
            ).update()
        """
        self.log.debug('starting the ``get`` method')

        self._add_galactic_coords()
        self._add_distances()

        # UPDATE LIGHTCURVE PLOTS

        from marshallEngine.lightcurves import marshall_lightcurves
        lc = marshall_lightcurves(
            log=self.log,
            dbConn=self.dbConn,
            settings=self.settings,
            transientBucketIds=self.transientBucketIds
        )
        lc.plot()

        # RESET UPDATE-NEEDED FLAG
        sqlQuery = """update transientBucketSummaries set updateNeeded = 0 where updateNeeded = 2""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug('completed the ``get`` method')
        return None

    def _add_galactic_coords(
            self):
        """*add galactic coordinates to the summary table*

        **Usage:**

            .. code-block:: python 

                from marshallEngine.housekeeping import update_transient_summaries
                updater = update_transient_summaries(
                    log=log,
                    settings=settings,
                    dbConn=dbConn
                )
                updater._add_galactic_coords()
        """
        self.log.debug('starting the ``_add_galactic_coords`` method')

        # SELECT THE TRANSIENTS NEEDING UPDATED
        sqlQuery = u"""
            select raDeg, decDeg, transientBucketId from transientBucketSummaries where updateNeeded = 2
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            quiet=False
        )

        # STOP IF NOTHING TO UPDATE
        if not len(rows):
            return

        # CREATE 3 LISTS - RA, DEC, ID
        raDeg = []
        raDeg[:] = [r["raDeg"] * u.degree for r in rows]
        decDeg = []
        decDeg[:] = [r["decDeg"] * u.degree for r in rows]
        ids = []
        ids[:] = [r["transientBucketId"] for r in rows]
        self.transientBucketIds = ids

        # DETERMINE GALACTIC COORDINATES
        c = SkyCoord(ra=raDeg, dec=decDeg, frame='icrs')
        l = c.galactic.l.deg
        b = c.galactic.b.deg

        # CREATE A LIST OF DICTIONARIES
        dictList = []
        for gl, gb, tid in zip(l, b, ids):
            dictList.append({
                "glat": gb,
                "glon": gl,
                "transientBucketId": tid
            })

        if not len(dictList):
            return

        # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
        # DICTIONARIES INTO DATABASE
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=dictList,
            dbTableName="transientBucketSummaries",
            dateModified=False,
            dateCreated=False,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        self.log.debug('completed the ``_add_galactic_coords`` method')
        return None

    def _add_distances(
            self):
        """*Add a distance measurement from the best redshift if the transient does not already have a distance measurement*

        **Usage:**

            .. code-block:: python 

                from marshallEngine.housekeeping import update_transient_summaries
                updater = update_transient_summaries(
                    log=log,
                    settings=settings,
                    dbConn=dbConn
                )
                updater._add_distances()
        """
        self.log.debug('starting the ``_add_distances`` method')

        # SELECT THE TRANSIENTS NEEDING UPDATED
        sqlQuery = u"""
            select best_redshift, transientBucketId from transientBucketSummaries where best_redshift is not null and distanceMpc is null and best_redshift > 0.001
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            quiet=False
        )

        # CONVERT REDSHIFT TO DISTANCE
        c = converter(log=self.log)
        dictList = []
        for r in rows:
            dists = c.redshift_to_distance(
                z=r["best_redshift"],
                WM=0.3,
                WV=0.7,
                H0=70.0
            )
            dmod = dists["dmod"]
            dl_mpc = dists["dl_mpc"]
            da_scale = dists["da_scale"]
            da_mpc = dists["da_mpc"]
            dcmr_mpc = dists["dcmr_mpc"]
            dictList.append({
                "transientBucketId": r["transientBucketId"],
                "distanceMpc": dl_mpc
            })

        if not len(dictList):
            return

        # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
        # DICTIONARIES INTO DATABASE
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=dictList,
            dbTableName="transientBucketSummaries",
            dateModified=False,
            dateCreated=False,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        self.log.debug('completed the ``_add_distances`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method