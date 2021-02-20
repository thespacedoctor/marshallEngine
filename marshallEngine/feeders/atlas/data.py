#!/usr/local/bin/python
# encoding: utf-8
"""
*import the ATLAS stream into the marshall*

:Author:
    David Young
"""
from builtins import str
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from ..data import data as basedata
from astrocalc.times import now
from astrocalc.times import conversions
from fundamentals.mysql import writequery
from marshallEngine.feeders.atlas.lightcurve import generate_atlas_lightcurves
from datetime import datetime, date, time, timedelta


class data(basedata):
    """
    *Import the ATLAS transient data into the marshall database*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the marshall database connection
    - ``settings`` -- the settings dictionary


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a data object, use the following:

    ```python
    from marshallEngine.feeders.atlas.data import data
    ingester = data(
        log=log,
        settings=settings,
        dbConn=dbConn
    ).ingest(withinLastDays=withInLastDay)   
    ```

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

        self.fsTableName = "fs_atlas"
        self.survey = "ATLAS"

        # xt-self-arg-tmpx

        return None

    def ingest(
            self,
            withinLastDays):
        """*Ingest the data into the marshall feeder survey table*

        **Key Arguments**

        - ``withinLastDays`` -- within the last number of days. *Default: 50*

        """
        self.log.debug('starting the ``ingest`` method')

        timelimit = datetime.now() - timedelta(days=int(withinLastDays))
        timelimit = timelimit.strftime("%Y-%m-%d")

        csvDicts = self.get_csv_data(
            url=self.settings["atlas urls"]["summary csv"] + f"?followup_flag_date__gte={timelimit}"
        )

        self._clean_data_pre_ingest(
            surveyName="ATLAS", withinLastDays=withinLastDays)

        self._import_to_feeder_survey_table()
        self.insert_into_transientBucket(updateTransientSummaries=False)

        sqlQuery = """call update_fs_atlas_forced_phot()""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.fsTableName = "fs_atlas_forced_phot"
        self.survey = "ATLAS FP"

        sqlQuery = """CALL update_transientBucket_atlas_sources()""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.insert_into_transientBucket(importUnmatched=False)

        # UPDATE THE ATLAS SPECIFIC FLUX SPACE LIGHTCURVES
        generate_atlas_lightcurves(
            log=self.log,
            dbConn=self.dbConn,
            settings=self.settings
        )

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.debug('completed the ``ingest`` method')
        return None

    def _clean_data_pre_ingest(
            self,
            surveyName,
            withinLastDays=False):
        """*clean up the list of dictionaries containing the ATLAS data, pre-ingest*

        **Key Arguments**

        - ``surveyName`` -- the ATLAS survey name
        -  ``withinLastDays`` -- the lower limit of observations to include (within the last N days from now). Default *False*, i.e. no limit


        **Return**

        - ``dictList`` -- the cleaned list of dictionaries ready for ingest


        **Usage**

        To clean the data from the ATLAS survey:

        ```python
        dictList = ingesters._clean_data_pre_ingest(surveyName="ATLAS")
        ```

        Note you will also be able to access the data via ``ingester.dictList``

        """
        self.log.debug('starting the ``_clean_data_pre_ingest`` method')

        self.dictList = []

        # CALC MJD LIMIT
        if withinLastDays:
            mjdLimit = now(
                log=self.log
            ).get_mjd() - float(withinLastDays)

        # CONVERTER TO CONVERT MJD TO DATE
        converter = conversions(
            log=self.log
        )

        for row in self.csvDicts:
            # IF NOW IN THE LAST N DAYS - SKIP
            flagMjd = converter.ut_datetime_to_mjd(
                utDatetime=row["followup_flag_date"])

            if withinLastDays and (float(row["earliest_mjd"]) < mjdLimit and float(flagMjd) < mjdLimit):
                continue

            # MASSAGE THE DATA IN THE INPUT FORMAT TO WHAT IS NEEDED IN THE
            # FEEDER SURVEY TABLE IN THE DATABASE
            target = row["target"]
            diff = row["diff"]
            ref = row["ref"]
            targetImageURL = None
            refImageURL = None
            diffImageURL = None

            if target:
                mjdStr = str(int(float(target.split("_")[1])))
                if target:
                    iid, mjdString, diffId, ippIdet, type = target.split('_')
                    targetImageURL = "https://star.pst.qub.ac.uk/sne/atlas4/site_media/images/data/atlas4/" % locals() + '/' + \
                        mjdStr + '/' + target + '.jpeg'
                    objectURL = "https://star.pst.qub.ac.uk/sne/atlas4/candidate/" + iid

            if ref:
                mjdStr = str(int(float(ref.split("_")[1])))
                if ref:
                    iid, mjdString, diffId, ippIdet, type = ref.split('_')
                    refImageURL = "https://star.pst.qub.ac.uk/sne/atlas4/site_media/images/data/atlas4/" % locals() + '/' + \
                        mjdStr + '/' + ref + '.jpeg'
                    objectURL = "https://star.pst.qub.ac.uk/sne/atlas4/candidate/" + iid

            if diff:
                mjdStr = str(int(float(diff.split("_")[1])))
                if diff:
                    iid, mjdString, diffId, ippIdet, type = diff.split('_')
                    diffImageURL = "https://star.pst.qub.ac.uk/sne/atlas4/site_media/images/data/atlas4/" % locals() + '/' + \
                        mjdStr + '/' + diff + '.jpeg'
                    objectURL = "https://star.pst.qub.ac.uk/sne/atlas4/candidate/" + iid

            discDate = converter.mjd_to_ut_datetime(
                mjd=row["earliest_mjd"], sqlDate=True)

            thisDictionary = {}
            thisDictionary["candidateID"] = row["name"]
            thisDictionary["ra_deg"] = row["ra"]
            thisDictionary["dec_deg"] = row["dec"]
            thisDictionary["mag"] = row["earliest_mag"]
            thisDictionary["observationMJD"] = row["earliest_mjd"]
            thisDictionary["filter"] = row["earliest_filter"]
            thisDictionary["discDate"] = discDate
            thisDictionary["discMag"] = row["earliest_mag"]
            thisDictionary["suggestedType"] = row["object_classification"]
            thisDictionary["targetImageURL"] = targetImageURL
            thisDictionary["refImageURL"] = refImageURL
            thisDictionary["diffImageURL"] = diffImageURL
            thisDictionary["objectURL"] = objectURL

            self.dictList.append(thisDictionary)

        self.log.debug('completed the ``_clean_data_pre_ingest`` method')
        return self.dictList

    # use the tab-trigger below for new method
    # xt-class-method
