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

class test_panstarrs_location_stamps(unittest.TestCase):

    def test_panstarrs_location_stamps_function(self):

        from fundamentals.mysql import writequery
        sqlQuery = """update pesstoObjects set ps1_map = 1;
        update pesstoObjects set ps1_map = null limit 10;""" % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )

        from marshallEngine.services import panstarrs_location_stamps
        ps_stamp = panstarrs_location_stamps(
            log=log,
            settings=settings,
            dbConn=dbConn,
            transientId=False
        )
        ps_stamp.get()

    def test_panstarrs_location_stamps_function_exception(self):

        from marshallEngine.services import panstarrs_location_stamps
        try:
            this = panstarrs_location_stamps(
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
