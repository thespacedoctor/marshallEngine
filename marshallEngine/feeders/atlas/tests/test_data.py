import os
import nose2
import shutil
import unittest
import yaml
from marshallEngine.utKit import utKit

from fundamentals import tools

packageDirectory = utKit("").get_project_root()
su = tools(
    arguments={"settingsFile": packageDirectory +
               "/test_settings.yaml"},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="marshall_webapp",
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log2, dbConn2, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

import shutil
try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)

# xt-setup-unit-testing-files-and-folders


class test_data(unittest.TestCase):

    def test_data_function(self):

        allLists = []
        from marshallEngine.feeders.atlas.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        )

        csvDicts = ingester.get_csv_data(
            url=settings["atlas urls"]["summary csv"]
        )
        ingester._clean_data_pre_ingest(
            surveyName="ATLAS", withinLastDays=50)

        # ADD DATA IMPORTING CODE HERE
        ingester._import_to_feeder_survey_table()
        ingester.insert_into_transientBucket()

    def test_data_function2(self):

        from marshallEngine.feeders.atlas.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=300)

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
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
