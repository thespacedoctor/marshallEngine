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


class test_marshall_lightcurves(unittest.TestCase):

    def test_marshall_lightcurves_function(self):

        from marshallEngine.lightcurves import marshall_lightcurves, _plot_one
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=28121353
        )
        filepath, currentMag, gradient = _plot_one(28121353, log, settings)

    def test_marshall_lightcurves_function2(self):

        from marshallEngine.lightcurves import marshall_lightcurves
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=[17, 21, 26, 35, 43, 48, 57, 73, 81, 85, 26787201, 26787202, 26787203, 28173350, 28173371, 28173379, 28173390, 28173405,
                                28173408, 28173409, 28173428, 28173431, 28174061, 28174288, 28174289, 28174292, 28174296, 28174298, 28177073, 28177093, 28177094]
        )
        this = lc.plot()
        print(this)

    def test_marshall_lightcurves_function_exception(self):

        from marshallEngine.lightcurves import marshall_lightcurves
        try:
            this = marshall_lightcurves(
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
