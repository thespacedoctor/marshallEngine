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
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
