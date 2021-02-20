#!/usr/local/bin/python
# encoding: utf-8
"""
*import the panstarrs stream into the marshall*

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
from fundamentals.mysql import writequery


class data(basedata):
    """
    *Import the PanSTARRS transient data into the marshall database*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the marshall database connection
    - ``settings`` -- the settings dictionary


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a data object, use the following:

    ```python
    from marshallEngine.feeders.panstarrs.data import data
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

        self.fsTableName = "fs_panstarrs"
        self.survey = "Pan-STARRS"

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

        allLists = []
        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["ps13pi"]["summary csv"],
            user=self.settings["credentials"]["ps13pi"]["username"],
            pwd=self.settings["credentials"]["ps13pi"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="ps13pi", withinLastDays=withinLastDays))

        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["ps13pi"]["recurrence csv"],
            user=self.settings["credentials"]["ps13pi"]["username"],
            pwd=self.settings["credentials"]["ps13pi"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="ps13pi", withinLastDays=withinLastDays))

        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["ps23pi"]["summary csv"],
            user=self.settings["credentials"]["ps23pi"]["username"],
            pwd=self.settings["credentials"]["ps23pi"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="ps23pi", withinLastDays=withinLastDays))

        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["ps23pi"]["recurrence csv"],
            user=self.settings["credentials"]["ps23pi"]["username"],
            pwd=self.settings["credentials"]["ps23pi"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="ps23pi", withinLastDays=withinLastDays))

        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["pso3"]["summary csv"],
            user=self.settings["credentials"]["pso3"]["username"],
            pwd=self.settings["credentials"]["pso3"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="pso3", withinLastDays=withinLastDays))
        csvDicts = self.get_csv_data(
            url=self.settings["panstarrs urls"]["pso3"]["recurrence csv"],
            user=self.settings["credentials"]["pso3"]["username"],
            pwd=self.settings["credentials"]["pso3"]["password"]
        )
        allLists.extend(self._clean_data_pre_ingest(
            surveyName="pso3", withinLastDays=withinLastDays))

        self.dictList = allLists
        self._import_to_feeder_survey_table()

        self.insert_into_transientBucket()

        # FIX ODD PANSTARRS COORDINATES
        sqlQuery = """update transientBucket set raDeg = raDeg+360.0 where raDeg  < 0;""" % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # CLEAN UP TASKS TO MAKE THE TICKET UPDATE
        self.clean_up()

        self.log.debug('completed the ``ingest`` method')
        return None

    def _clean_data_pre_ingest(
            self,
            surveyName,
            withinLastDays=False):
        """*clean up the list of dictionaries containing the PS data, pre-ingest*

        **Key Arguments**

        - ``surveyName`` -- the PS survey name
        -  ``withinLastDays`` -- the lower limit of observations to include (within the last N days from now). Default *False*, i.e. no limit


        **Return**

        - ``dictList`` -- the cleaned list of dictionaries ready for ingest


        **Usage**

        To clean the data from the PS 3pi survey:

        ```python
        dictList = ingesters._clean_data_pre_ingest(surveyName="3pi")
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

        for row in self.csvDicts:
            # IF NOW IN THE LAST N DAYS - SKIP
            if withinLastDays and float(row["mjd_obs"]) < mjdLimit:
                continue
            if float(row["ra_psf"]) < 0:
                row["ra_psf"] = 360. + float(row["ra_psf"])
            thisDictionary = {}

            thisDictionary["candidateID"] = row["ps1_designation"]
            thisDictionary["ra_deg"] = row["ra_psf"]
            thisDictionary["dec_deg"] = row["dec_psf"]
            thisDictionary["mag"] = row["cal_psf_mag"]
            thisDictionary["magerr"] = row["psf_inst_mag_sig"]
            thisDictionary["observationMJD"] = row["mjd_obs"]
            thisDictionary["filter"] = row["filter"]

            try:
                thisDictionary["discDate"] = row["followup_flag_date"]
            except:
                pass
            thisDictionary["discMag"] = row["cal_psf_mag"]

            if "transient_object_id" in list(row.keys()):
                thisDictionary[
                    "objectURL"] = "http://star.pst.qub.ac.uk/sne/%(surveyName)s/psdb/candidate/" % locals() + row["transient_object_id"]
            else:
                thisDictionary[
                    "objectURL"] = "http://star.pst.qub.ac.uk/sne/%(surveyName)s/psdb/candidate/" % locals() + row["id"]

            # CLEAN UP IMAGE URLS
            target = row["target"]
            if target:
                id, mjdString, diffId, ippIdet, type = target.split('_')
                thisDictionary["targetImageURL"] = "http://star.pst.qub.ac.uk/sne/%(surveyName)s/site_media/images/data/%(surveyName)s" % locals() + '/' + \
                    str(int(float(mjdString))) + '/' + target + '.jpeg'

            ref = row["ref"]
            if ref:
                id, mjdString, diffId, ippIdet, type = ref.split('_')
                thisDictionary["refImageURL"]  = "http://star.pst.qub.ac.uk/sne/%(surveyName)s/site_media/images/data/%(surveyName)s" % locals() + '/' + \
                    str(int(float(mjdString))) + '/' + ref + '.jpeg'

            diff = row["diff"]
            if diff:
                id, mjdString, diffId, ippIdet, type = diff.split('_')
                thisDictionary["diffImageURL"] = "http://star.pst.qub.ac.uk/sne/%(surveyName)s/site_media/images/data/%(surveyName)s" % locals() + '/' + \
                    str(int(float(mjdString))) + '/' + diff + '.jpeg'

            self.dictList.append(thisDictionary)

        self.log.debug('completed the ``_clean_data_pre_ingest`` method')
        return self.dictList

    # use the tab-trigger below for new method
    # xt-class-method
