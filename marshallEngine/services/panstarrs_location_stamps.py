#!/usr/local/bin/python
# encoding: utf-8
"""
*generate colour panstarrs location stamps for regions of transients*

:Author:
    David Young

:Date Created:
    August 18, 2019
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery, writequery
from panstamps.downloader import downloader
from panstamps.image import image


class panstarrs_location_stamps():
    """
    *The worker class for the panstarrs_location_stamps module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``dbConn`` -- dbConn
        - ``transientId`` -- will download for one transient if single ID given. Default *None*

    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

        To initiate a panstarrs_location_stamps object, use the following:

        .. todo::

            - add a tutorial about ``panstarrs_location_stamps`` to documentation
            - create a blog post about what ``panstarrs_location_stamps`` does

        .. code-block:: python 

            from marshallEngine.services import panstarrs_location_stamps
            ps_stamp = panstarrs_location_stamps(
                log=log,
                settings=settings,
                dbConn=dbConn,
                transientId=None
            ).get()
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

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

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the panstarrs_location_stamps object*

        **Return:**
            - ``panstarrs_location_stamps``

        **Usage:**
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - create cl-util for this method
            - update the package tutorial if needed

        .. code-block:: python 

            usage code 
        """
        self.log.debug('starting the ``get`` method')

        if self.transientId:
            transientId = self.transientId
            sqlQuery = u"""
                select t.transientBucketId, t.raDeg,t.decDeg from pesstoObjects p, transientBucketSummaries t where p.transientBucketId = t.transientBucketId and t.transientBucketId = %(transientId)s;
            """ % locals()
        else:
            # GET NEXT 200 TRANSIENTS NEEDING PANSTARRS STAMPS
            sqlQuery = u"""
                select t.transientBucketId, t.raDeg,t.decDeg from pesstoObjects p, transientBucketSummaries t where ps1_map is null and p.transientBucketId = t.transientBucketId order by t.transientBucketId desc limit 200;
            """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # FOR EACH TRANSIENT DOWNLOAD STAMP TO CACHE DIRECTORY
        downloadDirectoryPath = self.settings[
            "downloads"]["transient cache directory"]

        for row in rows:
            transientBucketId = row["transientBucketId"]
            downloadPath = "%s/%s" % (downloadDirectoryPath, transientBucketId)
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
                    update pesstoObjects set ps1_map = 0 where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=self.dbConn
                )
                continue

            source = colorPath[0]
            destination = downloadPath + "/ps1_map_color.jpeg"
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

        self.log.debug('completed the ``get`` method')
        return panstarrs_location_stamps

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
