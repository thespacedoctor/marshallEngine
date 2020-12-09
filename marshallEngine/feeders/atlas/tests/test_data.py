from __future__ import print_function
from builtins import str
import os
import unittest
import shutil
import yaml
from marshallEngine.utKit import utKit
from fundamentals import tools
from os.path import expanduser
home = expanduser("~")

packageDirectory = utKit("").get_project_root()
# settingsFile = packageDirectory + "/test_settings.yaml"
settingsFile = home + "/git_repos/_misc_/settings/marshall/test_settings.yaml"

su = tools(
    arguments={"settingsFile": settingsFile},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName=None,
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# SETUP PATHS TO COMMON DIRECTORIES FOR TEST DATA
moduleDirectory = os.path.dirname(__file__)
pathToInputDir = moduleDirectory + "/input/"
pathToOutputDir = moduleDirectory + "/output/"

try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)

import shutil
try:
    shutil.rmtree(settings[
        "cache-directory"] + "/transients/")
    shutil.rmtree(settings[
        "cache-directory"] + "/stats/")
except:
    pass


class test_data(unittest.TestCase):

    def test_data_function(self):

        allLists = []
        from marshallEngine.feeders.atlas.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        )

        from datetime import datetime, date, time, timedelta
        timelimit = datetime.now() - timedelta(days=1)
        timelimit = timelimit.strftime("%Y-%m-%d")

        csvDicts = ingester.get_csv_data(
            url=settings["atlas urls"]["summary csv"] + f"?followup_flag_date__gte={timelimit}"
        )
        ingester._clean_data_pre_ingest(
            surveyName="ATLAS", withinLastDays=1)

        # ADD DATA IMPORTING CODE HERE
        ingester._import_to_feeder_survey_table()
        ingester.insert_into_transientBucket()

    def test_data_function2(self):

        from marshallEngine.feeders.atlas.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=1)

    def test_data_function_exception(self):

        from marshallEngine.feeders.atlas.data import data
        try:
            this = data(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception as e:
            assert True
            print(str(e))

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
