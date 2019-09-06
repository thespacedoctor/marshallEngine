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

# # load settings
# stream = file(
#     "/Users/Dave/.config/marshallEngine/marshallEngine.yaml", 'r')
# settings = yaml.load(stream)
# stream.close()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# load settings
from os.path import expanduser
home = expanduser("~")
stream = file(
    home + "/.config/marshallEngine/marshallEngine.yaml", 'r')
settings = yaml.load(stream)
stream.close()

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


class test_images(unittest.TestCase):

    def test_images_function(self):

        from marshallEngine import images
        this = images(
            log=log,
            settings=settings
        )
        this.get()

    def test_images_function_exception(self):

        from marshallEngine import images
        try:
            this = images(
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
