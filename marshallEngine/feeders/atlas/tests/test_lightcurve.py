import os
import nose2
import shutil
import unittest
import yaml
from marshallEngine.utKit import utKit
from fundamentals import tools
from os.path import expanduser
home = expanduser("~")

packageDirectory = utKit("").get_project_root()
settingsFile = packageDirectory + "/test_settings.yaml"
settingsFile = home + "/.config/marshallEngine/marshallEngine.yaml"
su = tools(
    arguments={"settingsFile": settingsFile},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName=None,
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

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

        from marshallEngine import lightcurve
        try:
            this = lightcurve(
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
