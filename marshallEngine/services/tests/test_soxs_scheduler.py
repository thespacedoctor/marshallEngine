from __future__ import print_function
from builtins import str
import os
import unittest
import shutil
import unittest
import yaml
from marshallEngine.utKit import utKit
from fundamentals import tools
from os.path import expanduser
from docopt import docopt
from marshallEngine import cl_utils
doc = cl_utils.__doc__
home = expanduser("~")

packageDirectory = utKit("").get_project_root()
#settingsFile = packageDirectory + "/test_settings.yaml"
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


# xt-setup-unit-testing-files-and-folders
# xt-utkit-refresh-database

class test_soxs_scheduler(unittest.TestCase):

    def test_soxs_scheduler_create_single_auto_ob(self):

        from marshallEngine.services import soxs_scheduler
        schr = soxs_scheduler(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        obid = schr._create_single_auto_ob(
            transientBucketId=1,
            target_name="joe",
            raDeg=23.3445,
            decDeg=-30.034,
            magnitude_list=[["g", 19.06]]
        )
        print(obid)

    def test_soxs_scheduler_request_all_required_auto_obs(self):

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

        from marshallEngine.services import soxs_scheduler
        schr = soxs_scheduler(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        passedOBID, failedOBIDs = schr.request_all_required_auto_obs()
        print(failedOBIDs)

    def test_soxs_scheduler_function_exception(self):

        from marshallEngine.services import soxs_scheduler
        try:
            this = soxs_scheduler(
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
