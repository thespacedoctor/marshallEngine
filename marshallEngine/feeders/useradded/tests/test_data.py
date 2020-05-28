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


class test_data(unittest.TestCase):

    def test_data_function(self):

        # ADD A ROW TO BE IMPORTED
        # utKit.refresh_database()
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
        ).ingest(withinLastDays=3)

    def test_data_function2(self):

        from marshallEngine.feeders.useradded.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).ingest(withinLastDays=3)

    def test_update_summaries_function(self):

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
        ).insert_into_transientBucket(updateTransientSummaries=False)

        from fundamentals.mysql import readquery
        sqlQuery = u"""
            SELECT transientBucketId FROM fs_user_added order by dateLastModified desc limit 1;
        """ % locals()
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )

        transientBucketId = rows[0]["transientBucketId"]

        from marshallEngine.feeders.useradded.data import data
        ingester = data(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).insert_into_transientBucket(updateTransientSummaries=transientBucketId)

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
        except Exception as e:
            assert True
            print(str(e))

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
