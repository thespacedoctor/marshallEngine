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

class test_images(unittest.TestCase):

    def test_images_function(self):

        from marshallEngine.feeders.atlas import images
        cacher = images(
            log=log,
            settings=settings,
            dbConn=dbConn
        )
        transientBucketIds, subtractedUrls, targetUrls, referenceUrls, tripletUrls = cacher._list_images_needing_cached()
        subtractedStatus, targetStatus, referenceStatus, tripletStatus = cacher._download(
            transientBucketIds=transientBucketIds[:10],
            subtractedUrls=subtractedUrls[:10],
            targetUrls=targetUrls[:10],
            referenceUrls=referenceUrls[:10],
            tripletUrls=tripletUrls[:10]
        )
        cacher._update_database()

    def test_images_function2(self):

        from marshallEngine.feeders.atlas import images
        cacher = images(
            log=log,
            settings=settings,
            dbConn=dbConn
        ).cache(limit=1000)

    def test_images_function_exception(self):

        from marshallEngine.feeders.atlas import images
        try:
            this = images(
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
