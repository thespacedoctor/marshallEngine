#!/usr/local/bin/python
# encoding: utf-8
"""
*cache the panstarrs image stamps*

:Author:
    David Young
"""
from __future__ import print_function
from __future__ import division
from builtins import str
from builtins import zip
from builtins import object
from past.utils import old_div
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery
import requests
from requests.auth import HTTPBasicAuth
import codecs
from fundamentals import fmultiprocess
from fundamentals.mysql import writequery


class images(object):
    """
    *The base class for the feeder image cachers*

    **Usage**

    To create a new survey image cacher create a new class using this class as the baseclass:

    ```python
    from ..images import images as baseimages
    class images(baseimages):
        ....
    ```

    """

    def cache(
            self,
            limit=1000):
        """*cache the image for the requested survey*

        **Key Arguments**

        - ``limit`` -- limit the number of transients in the list so not to piss-off survey owners by downloading everything in one go.


        **Usage**

        ```python
        from marshallEngine.feeders.panstarrs import images
        cacher = images(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).cache(limit=1000)
        ```

        """
        self.log.debug('starting the ``cache`` method')

        # THESE SURVEY DON'T HAVE IMAGES - PASS
        if self.survey in ["tns", "atel", "atels"]:
            return

        transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls = self._list_images_needing_cached()
        leng = len(transientBucketIds)
        survey = self.survey

        if not leng:
            print("All _new_ images are cached for the %(survey)s survey" % locals())

        else:

            if leng > limit:
                print(
                    "Downloading image stamps for the next %(limit)s transients of %(leng)s remaining for %(survey)s" % locals())
            else:
                print(
                    "Downloading image stamps for the remaining %(leng)s transients for %(survey)s" % locals())
            subtractedStatus, targetStatus, referenceStatus, tripletStatus = self._download(
                transientBucketIds=transientBucketIds[:limit],
                subtractedUrls=subtractedUrls[:limit],
                targetUrls=targetUrls[:limit],
                referenceUrls=referenceUrls[:limit],
                tripletUrls=tripletUrls[:limit]
            )
            self._update_database()

        transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls = self._list_images_needing_cached(
            failedImage=True)
        leng = len(transientBucketIds)

        if not leng:
            print("All images are cached for the %(survey)s survey" % locals())

        else:
            print("Downloading image stamps for the next %(limit)s transients of %(leng)s remaining for %(survey)s - previously failed" % locals())
            subtractedStatus, targetStatus, referenceStatus, tripletStatus = self._download(
                transientBucketIds=transientBucketIds[:limit],
                subtractedUrls=subtractedUrls[:limit],
                targetUrls=targetUrls[:limit],
                referenceUrls=referenceUrls[:limit],
                tripletUrls=tripletUrls[:limit]
            )
            self._update_database()

        self.log.debug('completed the ``cache`` method')
        return None

    def _list_images_needing_cached(
            self,
            failedImage=False):
        """*get lists of the transientBucketIds and images needing cached for those transients*

        **Key Arguments**

        - ``failedImage`` -- second pass attempt to download alternative image for transients


        **Return**

        - ``transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls`` -- synced lists of transientBucketIds, subtracted-, target-, reference- and triplet-image urls. All lists are the same size.

        """
        self.log.debug('starting the ``_list_images_needing_cached`` method')

        subtractedUrls, targetUrls, referenceUrls, tripletUrls = [], [], [], []
        for imageType, v in list(self.stampFlagColumns.items()):
            if not v:
                continue
            imageUrl = imageType + "ImageUrl"
            # CREATE THE STAMP WHERE CLAUSE
            if not failedImage:
                stampWhere = v + " IS NULL "
            else:
                stampWhere = v + " = 2 "

            # CREATE THE SURVEY WHERE CLAUSE
            dbSurveyNames = "survey LIKE '%%" + \
                ("%%' OR survey LIKE '%%").join(self.dbSurveyNames) + "%%'"
            dbSurveyNames2 = dbSurveyNames.replace("survey L", "a.survey L")

            # NOW GENERATE SQL TO GET THE URLS OF STAMPS NEEDING DOWNLOADED
            if self.survey == "useradded":
                sqlQuery = u"""
                SELECT 
        a.transientBucketId, a.%(imageUrl)s 
    FROM
        transientBucket a
            JOIN
        (SELECT 
            MIN(magnitude) AS mag, transientBucketId
        FROM
            transientBucket
        WHERE
            magnitude IS NOT NULL
                AND %(imageUrl)s IS NOT NULL
                AND transientBucketId in (select transientBucketId from fs_user_added)
                AND transientBucketId IN (SELECT 
                    transientBucketId
                FROM
                    pesstoObjects
                WHERE
                    %(stampWhere)s and limitingMag = 0)
        GROUP BY transientBucketId
        ORDER BY transientBucketId) AS b ON a.transientBucketId = b.transientBucketId
            AND a.magnitude = b.mag
            WHERE limitingMag = 0
        GROUP BY transientBucketId;
            """ % locals()
            else:
                sqlQuery = u"""
                    SELECT 
            distinct a.transientBucketId, a.%(imageUrl)s 
        FROM
            transientBucket a
                JOIN
            (SELECT 
                MIN(magnitude) AS mag, transientBucketId
            FROM
                transientBucket
            WHERE
                magnitude IS NOT NULL
                    AND %(imageUrl)s IS NOT NULL
                    AND transientBucketId IN (SELECT 
                        transientBucketId
                    FROM
                        pesstoObjects
                    WHERE
                        %(stampWhere)s)
                    AND (%(dbSurveyNames)s) and limitingMag = 0 
            GROUP BY transientBucketId
            ORDER BY transientBucketId) AS b ON a.transientBucketId = b.transientBucketId
                AND a.magnitude = b.mag
        WHERE
            (%(dbSurveyNames2)s) and limitingMag = 0 and magnitude IS NOT NULL AND %(imageUrl)s IS NOT NULL GROUP BY a.transientBucketId;
                """ % locals()

            if failedImage:
                sqlQuery = sqlQuery.replace("AND a.magnitude = b.mag", "").replace(
                    "GROUP BY a.transientBucketId;", "")

            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn,
            )

            # SPLIT URLS INTO STAMP TYPES AND ORDER ALONGSIDE
            # TRANSIENTBUKCETIDs
            transientBucketIds = []
            for row in rows:
                transientBucketIds.append(row["transientBucketId"])
                if imageType == "subtracted":
                    subtractedUrls.append(row["subtractedImageUrl"])
                if imageType == "target":
                    targetUrls.append(row["targetImageUrl"])
                if imageType == "reference":
                    referenceUrls.append(row["referenceImageUrl"])
                if imageType == "triplet":
                    tripletUrls.append(row["tripletImageUrl"])

        for imageType, v in list(self.stampFlagColumns.items()):
            if not v:
                if imageType == "subtracted":
                    subtractedUrls = [None] * len(transientBucketIds)
                if imageType == "target":
                    targetUrls = [None] * len(transientBucketIds)
                if imageType == "reference":
                    referenceUrls = [None] * len(transientBucketIds)
                if imageType == "triplet":
                    tripletUrls = [None] * len(transientBucketIds)

        self.log.debug('completed the ``_list_images_needing_cached`` method')
        self.transientBucketIds = transientBucketIds
        return transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls

    def _download(
            self,
            transientBucketIds,
            subtractedUrls,
            targetUrls,
            referenceUrls,
            tripletUrls):
        """*cache the images for the survey under their transientBucketId folders in the web-server cache*

        **Key Arguments**

        - ``transientBucketIds`` -- the list of transientBucketId for the transients needing images downloaded.
        - ``subtractedUrls`` -- the list of subtracted image urls (same length as transientBucketIds list).
        - ``targetUrls`` -- the list of target image urls (same length as transientBucketIds list).
        - ``referenceUrls`` -- the list of reference image urls (same length as transientBucketIds list).
        - ``tripletUrls`` -- the list of triplet image urls (same length as transientBucketIds list).


        **Return**

        - ``subtractedStatus`` -- status of the subtracted image download (0 = fail, 1 = success, 2 = does not exist)
        - ``targetStatus`` -- status of the target image download (0 = fail, 1 = success, 2 = does not exist)
        - ``referenceStatus`` -- status of the reference image download (0 = fail, 1 = success, 2 = does not exist)
        - ``tripletStatus`` -- status of the triplet image download (0 = fail, 1 = success, 2 = does not exist)

        """
        self.log.debug('starting the ``_download`` method')

        downloadDirectoryPath = self.downloadDirectoryPath
        self.subtractedStatus = []
        self.targetStatus = []
        self.referenceStatus = []
        self.tripletStatus = []
        index = 1
        survey = self.survey.lower()

        # TOTAL TO DOWNLOAD
        count = len(transientBucketIds)

        # DOWNLOAD THE IMAGE SETS FOR EACH TRANSIENT AND ADD STATUS TO OUTPUT
        # ARRAYS. (0 = fail, 1 = success, 2 = does not exist)
        for tid, surl, turl, rurl, purl in zip(transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls):
            if index > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")

            percent = (old_div(float(index), float(count))) * 100.
            print('%(index)s/%(count)s (%(percent)1.1f%% done): downloading %(survey)s stamps for transientBucketId: %(tid)s' % locals())

            statusArray = download_image_array(imageArray=[
                                               tid, surl, turl, rurl, purl], log=self.log, survey=self.survey, downloadPath=downloadDirectoryPath)
            self.subtractedStatus.append(statusArray[1])
            self.targetStatus.append(statusArray[2])
            self.referenceStatus.append(statusArray[3])
            self.tripletStatus.append(statusArray[4])
            index += 1

        self.log.debug('completed the ``_download`` method')
        return self.subtractedStatus, self.targetStatus, self.referenceStatus, self.tripletStatus

    def _update_database(
            self):
        """*update the database to show which images have been cached on the server*
        """
        self.log.debug('starting the ``_update_database`` method')

        if not len(self.tripletStatus):
            self.log.debug('completed the ``_update_database`` method')
            return None

        # ITERATE OVER 4 STAMP COLUMNS
        for imageType, column in list(self.stampFlagColumns.items()):
            if column:
                if imageType == "subtracted":
                    status = self.subtractedStatus
                if imageType == "target":
                    status = self.targetStatus
                if imageType == "reference":
                    status = self.referenceStatus
                if imageType == "triplet":
                    status = self.tripletStatus

                nonexist = []
                exist = []
                # NON-EXISTANT == STATUS 2
                nonexist[:] = [str(t) for t, s in zip(
                    self.transientBucketIds, status) if s == 2]
                nonexist = (",").join(nonexist)
                # EXISTANT == STATUS 1 (i.e. these are downloaded)
                exist[:] = [str(t) for t, s in zip(
                    self.transientBucketIds, status) if s == 1]
                exist = (",").join(exist)
                # GENERATE THE SQL TO UPDATE DATABASE
                sqlQuery = ""
                if len(nonexist):
                    sqlQuery += """update pesstoObjects set %(column)s = 3 where transientBucketId in (%(nonexist)s) and %(column)s = 2;""" % locals(
                    )
                    sqlQuery += """update pesstoObjects set %(column)s = 2 where transientBucketId in (%(nonexist)s) and (%(column)s is null or %(column)s = 0);""" % locals(
                    )

                    writequery(
                        log=self.log,
                        sqlQuery=sqlQuery,
                        dbConn=self.dbConn
                    )
                if len(exist):
                    sqlQuery = """update pesstoObjects set %(column)s = 1 where transientBucketId in (%(exist)s) and (%(column)s != 1 or %(column)s is null);""" % locals(
                    )

                    writequery(
                        log=self.log,
                        sqlQuery=sqlQuery,
                        dbConn=self.dbConn
                    )

        self.log.debug('completed the ``_update_database`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method


def download_image_array(
        imageArray,
        log,
        survey,
        downloadPath):
    """*download an array of transient image stamps*

    **Key Arguments**

    - ``log`` -- logger
    - ``imageArray`` -- [transientBucketId, subtractedUrl, targetUrl, referenceUrl, tripletUrl]
    - ``survey`` -- name of the survey to name stamps with
    - ``downloadPath`` -- directory to download the images into


    **Return**

    - statusArray -- [subtractedStatus, targetStatus, referenceStatus, tripletStatus]

    """
    tid = imageArray[0]
    statusArray = [imageArray[0]]

    # RECURSIVELY CREATE MISSING TRANSIENT DIRECTORIES
    downloadPath = "%(downloadPath)s/%(tid)s/" % locals()
    if not os.path.exists(downloadPath):
        os.makedirs(downloadPath)
    filepath = downloadPath + survey.lower()

    for url, stamp in zip(imageArray[1:], ["subtracted", "target", "reference", "triplet"]):
        if url:
            pathToWriteFile = "%(filepath)s_%(stamp)s_stamp.jpeg" % locals(
            )
        else:
            # NOTHING TO DOWNLOAD
            statusArray.append(0)
            continue

        try:
            response = requests.get(
                url=url,
                timeout=1.0
                # params={},
                # auth=HTTPBasicAuth('user', 'pwd')
            )
            content = response.content
            status_code = response.status_code
        except requests.exceptions.RequestException as e:
            if 'timed out' in str(e):
                print('timed out - try again next time' % locals())
                statusArray.append(0)
            else:
                print('HTTP Request failed - %(e)s' % locals())
                print("")
                statusArray.append(2)
            continue

        if status_code == 404:
            print('image not found' % locals())
            statusArray.append(2)
            continue

        # WRITE STAMP TO FILE
        try:
            writeFile = codecs.open(
                pathToWriteFile, mode='wb')
        except IOError as e:
            message = 'could not open the file %s' % (pathToWriteFile,)
            raise IOError(message)
        writeFile.write(content)
        writeFile.close()
        statusArray.append(1)

    return statusArray
