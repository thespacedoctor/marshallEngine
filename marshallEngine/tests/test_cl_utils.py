from __future__ import print_function
from builtins import str
import os
import unittest
import shutil
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

    # marshall lightcurve < transientBucketId > [-s < pathToSettingsFile > ]


class test_cl_utils(unittest.TestCase):

    # def test_01_cl(self):

    #     pathToSettingsFile = settingsFile
    #     # TEST CL-OPTIONS
    #     command = "marshall init"
    #     args = docopt(doc, command.split(" ")[1:])
    #     cl_utils.main(args)

    #     command = "marshall clean -s %(pathToSettingsFile)s" % locals()
    #     args = docopt(doc, command.split(" ")[1:])
    #     cl_utils.main(args)

    # def test_02_cl(self):

    #     pathToSettingsFile = settingsFile
    #     command = "marshall import atlas 1 -s %(pathToSettingsFile)s" % locals()
    #     args = docopt(doc, command.split(" ")[1:])
    #     cl_utils.main(args)

    # def test_03_cl(self):

    #     pathToSettingsFile = settingsFile
    #     command = "marshall import panstarrs 1 -s %(pathToSettingsFile)s" % locals(
    #     )
    #     args = docopt(doc, command.split(" ")[1:])
    #     cl_utils.main(args)

    def test_04_cl(self):

        pathToSettingsFile = settingsFile
        command = "marshall import tns 1 -s %(pathToSettingsFile)s" % locals()
        args = docopt(doc, command.split(" ")[1:])
        cl_utils.main(args)

    def test_05_cl(self):

        pathToSettingsFile = settingsFile
        command = "marshall import useradded 1 -s %(pathToSettingsFile)s" % locals(
        )
        args = docopt(doc, command.split(" ")[1:])
        cl_utils.main(args)

    def test_06_cl(self):

        pathToSettingsFile = settingsFile
        command = "marshall import ztf 1 -s %(pathToSettingsFile)s" % locals()
        args = docopt(doc, command.split(" ")[1:])
        cl_utils.main(args)

    def test_07_cl(self):

        pathToSettingsFile = settingsFile
        command = "marshall lightcurve 1 -s %(pathToSettingsFile)s" % locals()
        args = docopt(doc, command.split(" ")[1:])
        cl_utils.main(args)

        return

    # x-class-to-test-named-worker-function
