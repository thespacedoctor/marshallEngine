#!/usr/local/bin/python
# encoding: utf-8
"""
*cache the ATLAS image stamps*

:Author:
    David Young
"""
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
from ..images import images as baseimages


class images(baseimages):
    """
    *cacher for the ATLAS image stamps*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``dbConn`` -- the marshall database connection.


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a images object, use the following:

    ```python
    from marshallEngine.feeders.atlas import images
    cacher = images(
        log=log,
        settings=settings,
        dbConn=dbConn
    ).cache(limit=1000)  
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'images' object")
        self.settings = settings
        self.dbConn = dbConn
        self.downloadDirectoryPath = settings[
            "cache-directory"] + "/transients/"

        self.dbSurveyNames = [
            "atlas", "ATLAS"]

        # SET THESE IMAGE FLAG COLUMNS FOR THE SURVEY
        self.stampFlagColumns = {
            "subtracted": None,
            "target": "atlas_target_stamp",
            "reference": None,
            "triplet": None
        }
        self.survey = "ATLAS"

        return None
