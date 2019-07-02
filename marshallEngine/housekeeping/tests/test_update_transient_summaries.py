import os
import nose2
import shutil
import unittest
import yaml
from marshallEngine.utKit import utKit

from fundamentals import tools

su = tools(
    arguments={"settingsFile": None},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="marshallEngine",
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# load settings
from os.path import expanduser
home = expanduser("~")
stream = file(
    home + "/.config/marshallEngine/marshallEngine.yaml", 'r')
settings = yaml.load(stream)
stream.close()

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


class test_update_transient_summaries(unittest.TestCase):

    def test_update_transient_summaries_function(self):

        from marshallEngine.housekeeping import update_transient_summaries
        updater = update_transient_summaries(
            log=log,
            settings=settings,
            dbConn=dbConn
        )
        updater._add_galactic_coords()

    def test_update_transient_summaries_function2(self):

        from marshallEngine.housekeeping import update_transient_summaries
        updater = update_transient_summaries(
            log=log,
            settings=settings,
            dbConn=dbConn
        )
        updater._add_distances()

    def test_update_transient_summaries_function3(self):

        from marshallEngine.housekeeping import update_transient_summaries
        updater = update_transient_summaries(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).update()

    def test_update_transient_summaries_function_exception(self):

        from marshallEngine import update_transient_summaries
        try:
            this = update_transient_summaries(
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
