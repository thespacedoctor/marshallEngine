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

class test_lightcurve(unittest.TestCase):

    def test_lightcurve_function(self):

        # utKit.refresh_database() # reset database to database found in
        # marshallEngine/test/input
        from marshallEngine.feeders.atlas.lightcurve import generate_atlas_lightcurves
        generate_atlas_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings
        )

    def test_lightcurve_function_exception(self):

        from marshallEngine.feeders.atlas.lightcurve import generate_atlas_lightcurves
        try:
            this = generate_atlas_lightcurves(
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
