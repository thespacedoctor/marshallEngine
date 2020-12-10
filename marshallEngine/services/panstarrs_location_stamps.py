#!/usr/local/bin/python
# encoding: utf-8
"""
*generate colour panstarrs location stamps for regions of transients*

:Author:
    David Young
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery, writequery
from panstamps.downloader import downloader
from panstamps.image import image


class panstarrs_location_stamps(object):
    """
    *The worker class for the panstarrs_location_stamps module*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``dbConn`` -- dbConn
    - ``transientId`` -- will download for one transient if single ID given. Default *None*


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a panstarrs_location_stamps object, use the following:

    ```python
    from marshallEngine.services import panstarrs_location_stamps
    ps_stamp = panstarrs_location_stamps(
        log=log,
        settings=settings,
        dbConn=dbConn,
        transientId=None
    ).get()
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            transientId=None,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'panstarrs_location_stamps' object")
        self.settings = settings
        self.dbConn = dbConn
        self.transientId = transientId

        # xt-self-arg-tmpx

        return None

    def get(self):
        """
        *get the panstarrs_location_stamps object*
        """
        self.log.debug('starting the ``get`` method')

        # FOR A SINGLE TRANSIENT
        if self.transientId:
            transientId = self.transientId
            sqlQuery = u"""
                select t.transientBucketId, t.raDeg,t.decDeg from pesstoObjects p, transientBucketSummaries t where p.transientBucketId = t.transientBucketId and t.transientBucketId = %(transientId)s;
            """ % locals()
        # OR THE NEXT 200 TRANSIENTS NEEDING STAMPS
        else:
            # GET NEXT 200 TRANSIENTS NEEDING PANSTARRS STAMPS
            sqlQuery = u"""
                select * from pesstoObjects p, transientBucketSummaries t where (ps1_map is null or ps1_map not in (0,1)) and p.transientBucketId = t.transientBucketId order by t.transientBucketId desc limit 200;
            """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # FOR EACH TRANSIENT DOWNLOAD STAMP TO CACHE DIRECTORY
        downloadDirectoryPath = self.settings[
            "cache-directory"] + "/transients/"

        for row in rows:
            transientBucketId = row["transientBucketId"]
            downloadPath = f"{downloadDirectoryPath}/{transientBucketId}"
            ra = row["raDeg"]
            dec = row["decDeg"]

            fitsPaths, jpegPaths, colorPath = downloader(
                log=self.log,
                settings=self.settings,
                downloadDirectory=downloadPath,
                fits=False,
                jpeg=False,
                arcsecSize=60,
                filterSet='gri',
                color=True,
                singleFilters=False,
                ra=ra,
                dec=dec,
                imageType="stack"  # warp | stack
            ).get()

            # CHECK FOR FAILED IMAGES AND FLAG IN DATABASE
            if len(colorPath) == 0 or not colorPath[0]:
                sqlQuery = u"""
                    update pesstoObjects p, transientBucketSummaries t set p.ps1_map = 0 where p.transientBucketId=t.transientBucketId and (ps1_map is null or ps1_map != 0) and t.decDeg < -40;
                    update pesstoObjects set ps1_map = 2 where transientBucketId = %(transientBucketId)s and ps1_map is null;
                    update pesstoObjects set ps1_map = 2+ps1_map where transientBucketId = %(transientBucketId)s and ps1_map is not null;
                    update pesstoObjects set ps1_map = 0 where transientBucketId = %(transientBucketId)s and ps1_map > 10;
                """ % locals()
                writequery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=self.dbConn
                )
                continue

            source = colorPath[0]
            destination = downloadPath + "/ps1_map_color.jpeg"
            try:
                os.rename(source, destination)

                # DOWNLOAD THE COLOR IMAGE
                myimage = image(
                    log=self.log,
                    settings=self.settings,
                    imagePath=destination,
                    arcsecSize=60,
                    crosshairs=True,
                    transient=False,
                    scale=True,
                    invert=False,
                    greyscale=False
                ).get()

                # UPDATE DATABASE FLAG
                sqlQuery = u"""
                    update pesstoObjects set ps1_map = 1 where transientBucketId = %(transientBucketId)s
                """ % locals()

                writequery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=self.dbConn
                )
            except:
                self.log.warning(
                    "Could not process the image %(destination)s" % locals())

        self.log.debug('completed the ``get`` method')
        return None
