#!/usr/bin/env python
# encoding: utf-8
"""
*import the atels stream into the marshall*

:Author:
    David Young

:Date Created:
    January 12, 2021
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from ..data import data as basedata
from astrocalc.times import now
import git
from fundamentals.mysql import writequery


class data(basedata):
    """
    *Import the atels transient data into the marshall database*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- the marshall database connection
        - ``settings`` -- the settings dictionary

    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

        To initiate a data object, use the following:

        .. code-block:: python 

            from marshallEngine.feeders.atels.data import data
            ingester = data(
                log=log,
                settings=settings,
                dbConn=dbConn
            ).ingest(withinLastDays=withInLastDay)   
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

        self.fsTableName = "atel_coordinates"
        self.survey = "atel"
        self.dictList = []

        # xt-self-arg-tmpx

        return None

    def ingest(
            self,
            withinLastDays):
        """*Ingest the data into the marshall feeder survey table*

        **Key Arguments:**
            - ``withinLastDays`` -- within the last number of days. *Default: 50*
        """
        self.log.debug('starting the ``ingest`` method')

        allLists = []

        self.download_new_atels()
        self.parse_atels_to_database()

        self.insert_into_transientBucket(
            importUnmatched=False,
            updateTransientSummaries=True)

        self.fsTableName = "atel_names"
        self.insert_into_transientBucket(
            importUnmatched=False,
            updateTransientSummaries=True)

        sqlQuery = f"""CALL insert_atel_titles_to_comments();"""
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.debug('completed the ``ingest`` method')
        return None

    def update_git_repo(
            self):
        """*update the atel data git repo (if it exists)*
        ```
        """
        self.log.debug('starting the ``update_git_repo`` method')

        # TEST IF atel-directory IS A GIT REPO
        atelDir = self.settings["atel-directory"]

        # FIRST TRY RAW PATH
        try:
            repo = git.Repo(atelDir)
        except:
            repo = False

        # IF THIS FAILS TRY PARENT DIRECTORY
        if repo == False:
            try:
                repo = os.path.dirname(atelDir)
                repo = git.Repo(repo)
            except:
                repo = False

        # PUSH/PULL CHAGES
        if repo != False:
            repo.git.add(update=True)
            repo.index.commit("adding atels")
            o = repo.remotes.origin
            o.pull()
            o.push()
            o.pull()

        self.log.debug('completed the ``update_git_repo`` method')
        return None

    def download_new_atels(
            self):
        """*download new atel html files*
        """
        self.log.debug('starting the ``download_new_atels`` method')

        self.update_git_repo()
        from atelParser import download
        atels = download(
            log=self.log,
            settings=self.settings
        )
        atelsToDownload = atels.get_list_of_atels_still_to_download()
        if "test" in self.settings and self.settings["test"] == True:
            atelsToDownload = [atelsToDownload[-1]]
            atels.maxsleep = 1
        else:
            atels.maxsleep = 20
        atels.download_list_of_atels(atelsToDownload)
        self.update_git_repo()

        self.log.debug('completed the ``download_new_atels`` method')
        return None

    def parse_atels_to_database(
            self):
        """*parse content of atels to the marshall database.*

        This populates the `atel_coordinates`, `atel_fullcontent` and `atel_names` database tables.
        """
        self.log.debug('starting the ``parse_atels_to_database`` method')

        from atelParser import mysql
        parser = mysql(
            log=self.log,
            settings=self.settings
        )
        parser.atels_to_database()
        parser.parse_atels()
        parser.populate_htm_columns()

        self.log.debug('completed the ``parse_atels_to_database`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
