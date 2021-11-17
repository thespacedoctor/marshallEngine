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

    def test_soxs_scheduler_function(self):

        from marshallEngine.services import soxs_scheduler
        schr = soxs_scheduler(
            log=log,
            settings=settings
        )
        obid = schr.create_single_auto_ob(
            transientBucketID=1,
            target_name="joe",
            raDeg=23.3445,
            decDeg=-30.034,
            magnitude_list=[["g", 19.06]]
        )
        print(obid)

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
