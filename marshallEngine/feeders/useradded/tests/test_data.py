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
# EMPTY OUT THE UNIT TEST DATABASE BEFORE TESTING
utKit.refresh_database()
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

        # ADD A ROW TO BE IMPORTED
        utKit.refresh_database()
        from fundamentals.mysql import writequery
        sqlQuery = """INSERT IGNORE INTO `fs_user_added` (`id`,`candidateID`,`ra_deg`,`dec_deg`,`mag`,`magErr`,`filter`,`observationMJD`,`discDate`,`discMag`,`suggestedType`,`catalogType`,`hostZ`,`targetImageURL`,`objectURL`,`summaryRow`,`ingested`,`htm16ID`,`survey`,`author`,`dateCreated`,`dateLastModified`,`suggestedClassification`,`htm13ID`,`htm10ID`,`transientBucketId`) VALUES (856,'TestSource',155.125958333,-15.1787369444,20.3,NULL,NULL,57627.5,'2016-08-27',20.3,'SN',NULL,0.34,'http://thespacedoctor.co.uk/images/thespacedoctor_icon_white_circle.png','http://thespacedoctor.co.uk',1,0,NULL,'testSurvey','None','2019-07-30 14:25:39','2019-07-30 14:25:39',NULL,NULL,NULL,NULL);""" % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        from marshallEngine.feeders.useradded.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=300)

    def test_data_function2(self):

        from marshallEngine.feeders.useradded.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=300)

    def test_data_function_exception(self):

        from marshallEngine.feeders.useradded.data import data
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
