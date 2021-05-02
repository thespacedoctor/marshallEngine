#!/usr/local/bin/python
# encoding: utf-8
"""
*Generate the standard multi-survey lightcurve plots for the marshall transients*

:Author:
    David Young
"""
from __future__ import print_function
from builtins import str
from builtins import zip
from builtins import range
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals import fmultiprocess
from fundamentals.mysql import readquery, writequery
from fundamentals.mysql import database
from astrocalc.times import conversions, now
import numpy as np
# SUPPRESS MATPLOTLIB WARNINGS
import warnings
warnings.filterwarnings("ignore")
import matplotlib as mpl
from matplotlib import dates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.ticker as mtick
from matplotlib.backends.backend_pdf import PdfPages
import math

from numpy.polynomial.chebyshev import chebfit, chebval


class marshall_lightcurves(object):
    """
    *The worker class for the marshall_lightcurves module*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``dbConn`` -- the database connection for the mrshall
    - ``transientBucketIds`` -- the transientBucketId(s) requiring lightcurves to be regenerated. (int or list)


    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a marshall_lightcurves object, use the following:

    ```python
    from marshallEngine.lightcurves import marshall_lightcurves
    lc = marshall_lightcurves(
        log=log,
        dbConn=dbConn,
        settings=settings,
        transientBucketIds=[28421489, 28121353, 4637952, 27409808]
    )
    lc.plot()
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            settings=False,
            transientBucketIds=[]
    ):
        self.log = log
        log.debug("instansiating a new 'marshall_lightcurves' object")
        self.settings = settings
        self.dbConn = dbConn
        self.transientBucketIds = transientBucketIds
        # xt-self-arg-tmpx

        # CONVERT TRANSIENTBUCKETIDS TO LIST
        if not isinstance(self.transientBucketIds, list):
            self.transientBucketIds = [self.transientBucketIds]

        return None

    def _select_data_for_transient(
            self,
            transientBucketId):
        """*get the transient lightcurve data from the marshall database*

        **Key Arguments**

        - ``transientBucketId`` -- the transientBucketId of source to get data for

        """
        self.log.debug('starting the ``_select_data_for_transient`` method')

        # SELECT THE DATA REQUIRED FOR THIS LIGHTCURVE PLOT
        sqlQuery = """
            SELECT
                transientBucketId,
                survey,
                magnitude,
                magnitudeError,
                observationDate,
                observationMJD,
                IFNULL(filter, '?') as filter,
                name,
                limitingMag,
                concat(ROUND(observationMJD, 2), SUBSTRING(filter, 1, 1)) as uniqueConstraint,
                concat(ROUND(observationMJD, 0), SUBSTRING(filter, 1, 1)) as limitConstraint
            FROM
                transientBucket
            WHERE
                replacedByRowId = 0
                    AND transientBucketId = %(transientBucketId)s
                    AND magnitude IS NOT NULL
                    -- AND filter is not null
            ORDER BY uniqueConstraint ASC, magnitudeError desc, magnitude asc
        """ % locals()
        transientData = readquery(
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            log=self.log
        )

        theseMags = []
        theseMags[:] = [a
                        for a in transientData if a["limitingMag"] == 0]

        # CLIP OUTLIERS
        if len(theseMags) > 4:
            allmags = []
            allmags[:] = [a["magnitude"] for a in theseMags]
            mean = np.mean(allmags)
            std = np.std(allmags)

            if std > 0.1:
                allmags[:] = [a for a in transientData if (abs(
                    a["magnitude"] - mean) < 2 * std) or a["limitingMag"] == 1]
                transientData = allmags

        # NO DATA?
        if len(transientData) == 0:
            return False, False, False

        # SPLIT DATA BY FILTER - MAGNITUDES AND LIMITS
        filterList = []
        filterList[:] = set([r["filter"][0] for r in transientData])

        dataset = {}
        flatdata = {"mag": [], "mjd": []}
        flatLimits = {"mag": [], "mjd": []}
        for f in filterList:
            mag = []
            magErr = []
            magMjd = []
            limit = []
            limitMjd = []
            magNoErr = []
            magNoErrMjd = []
            magNoErrFudge = []
            catch = []
            limitCatcher = {}
            for r in transientData:
                if r["filter"][0] == f and r["uniqueConstraint"] not in catch:
                    if r["limitingMag"] == 0 and r["magnitudeError"]:
                        mag.append(r["magnitude"])
                        magErr.append(r["magnitudeError"])
                        magMjd.append(r["observationMJD"])
                        flatdata["mag"].append(r["magnitude"])
                        flatdata["mjd"].append(r["observationMJD"])
                        catch.append(r["uniqueConstraint"])
                    elif r["limitingMag"] == 0 and not r["magnitudeError"]:
                        magNoErr.append(r["magnitude"])
                        magNoErrFudge.append(0.3)
                        magNoErrMjd.append(r["observationMJD"])
                        flatdata["mag"].append(r["magnitude"])
                        flatdata["mjd"].append(r["observationMJD"])
                        catch.append(r["uniqueConstraint"])
                    elif r["limitConstraint"] not in limitCatcher:
                        limitCatcher[r["limitConstraint"]] = [
                            r["magnitude"], r["observationMJD"]]
                    elif limitCatcher[r["limitConstraint"]][0] < r["magnitude"]:
                        limitCatcher[r["limitConstraint"]] = [
                            r["magnitude"], r["observationMJD"]]

            for k, v in list(limitCatcher.items()):
                limit.append(v[0])
                limitMjd.append(v[1])
                flatLimits["mag"].append(v[0])
                flatLimits["mjd"].append(v[1])

            dataset[f] = {
                "limit": limit,
                "limitMjd": limitMjd,
                "mag": mag,
                "magErr": magErr,
                "magMjd": magMjd,
                "magNoErr": magNoErr,
                "magNoErrFudge": magNoErrFudge,
                "magNoErrMjd": magNoErrMjd
            }

        if len(flatdata["mag"]) == 0:
            return False, False, False

        self.log.debug('completed the ``_select_data_for_transient`` method')
        return dataset, flatdata, flatLimits

    def _create_lightcurve_plot_file(
            self,
            dataset,
            flatdata,
            flatLimits,
            objectNames,
            saveLocation,
            saveFileName):
        """*Generate the lightcurve and save to file*

        **Key Arguments**

        - ``log`` -- logger
        - ``dataset`` -- the observational dataset split into filters (and then mags, limits etc)
        - ``flatdata`` -- a flattened dataset to determine current magnitude
        - ``flatLimits`` -- a flattened dataset of non-detection limits
        - ``objectNames`` -- a single name or a list of names
        - ``saveLocation`` -- the folder to save the plot file to
        - ``saveFileName`` -- the filename to give the plot file (without extension)


        **Return**

        - ``filepath`` -- path to the lightcurve file
        - ``currentMag`` -- a prediction of the current magnitude if there is enough recent data
        - ``gradient`` -- a prediction of the gradient of recent data (on rise or decline?)

        """
        self.log.debug('starting the ``_create_lightcurve_plot_file`` method')

        # CONVERTER TO CONVERT MJD TO DATE
        converter = conversions(
            log=self.log
        )

        # INITIATE THE PLOT FIGURE - SQUARE
        fig = plt.figure(
            num=None,
            figsize=(10, 10),
            dpi=100,
            facecolor=None,
            edgecolor=None,
            frameon=True)
        ax = fig.add_subplot(1, 1, 1)

        # TICK LABEL SIZE
        mpl.rc('ytick', labelsize=25)
        mpl.rc('xtick', labelsize=25)
        mpl.rcParams.update({'font.size': 25})

        # INITIAL RESTRICTIONS
        currentMag = -9999
        gradient = -9999

        # WHAT IS TODAY MJD (FIR CURRENT MAG ESTIMATE)
        todayMjd = now(
            log=self.log
        ).get_mjd()

        # MAKE ARRAYS OF TIME AND MAG FOR PLOTS
        bigTimeArray, bigMagArray = np.array(
            flatdata["mjd"]), np.array(flatdata["mag"])
        # SORT TWO LIST BASED ON FIRST
        bigTimeArray, bigMagArray = zip(
            *[(x, y) for x, y in sorted(zip(bigTimeArray, bigMagArray))])

        # BIN DATA FOR POLYNOMIALS
        binData = True
        if binData is True:
            distinctMjds = {}
            for mjd, mag in zip(bigTimeArray, bigMagArray):
                # DICT KEY IS THE UNIQUE INTEGER MJD
                key = str(int(math.floor(mjd / 1.0)))
                # FIRST DATA POINT OF THE NIGHTS? CREATE NEW DATA SET
                if key not in distinctMjds:
                    distinctMjds[key] = {
                        "mjds": [mjd],
                        "mags": [mag]
                    }
                # OR NOT THE FIRST? APPEND TO ALREADY CREATED LIST
                else:
                    distinctMjds[key]["mjds"].append(mjd)
                    distinctMjds[key]["mags"].append(mag)

            # ALL DATA NOW IN MJD SUBSETS. SO FOR EACH SUBSET (I.E. INDIVIDUAL
            # NIGHTS) ...
            summedMagnitudes = {
                'mjds': [],
                'mags': []
            }
            for k, v in list(distinctMjds.items()):
                # GIVE ME THE MEAN MJD
                meanMjd = sum(v["mjds"]) / len(v["mjds"])
                summedMagnitudes["mjds"].append(meanMjd)
                # GIVE ME THE MEAN MAG
                meanMag = sum(v["mags"]) / len(v["mags"])
                summedMagnitudes["mags"].append(meanMag)

            bigTimeArray = summedMagnitudes["mjds"]
            bigMagArray = summedMagnitudes["mags"]

        bigTimeArray = np.array(bigTimeArray)
        bigMagArray = np.array(bigMagArray)

        # DETERMINE SENSIBLE AXIS LIMITS FROM FLATTENED DATA
        # LIMITS HERE ARE LOWER AND UPPER MJDS FOR X-AXIS
        xLowerLimit = bigTimeArray.min()
        xUpperLimit = bigTimeArray.max()
        latestTime = xUpperLimit
        xBorder = math.fabs((xUpperLimit - xLowerLimit)) * 0.1
        if xBorder < 5:
            xBorder = 5.
        xLowerLimit -= xBorder
        xUpperLimit += xBorder
        fixedXUpperLimit = xUpperLimit
        timeRange = xUpperLimit - xLowerLimit

        # POLYNOMIAL CONSTAINTS USING COMBINED DATASETS
        # POLYNOMIAL/LINEAR SETTINGS
        # SETTINGS FILE
        polyOrder = 5
        # EITHER USE DATA IN THESE LAST NUMBER OF DAYS OR ...
        lastNumDays = 10.
        # ... IF NOT ENOUGH DATA USE THE LAST NUMBER OF DATA POINTS
        predictCurrentMag = True
        lastNumDataPoints = 3
        numAnchors = 3
        anchorSeparation = 70
        latestMag = bigMagArray[0]
        anchorPointMag = latestMag + 0.5
        polyTimeArray, polyMagArray = [], []

        # QUIT IF NOT ENOUGH DATA FOR POLYNOMIAL
        if len(bigTimeArray) <= lastNumDataPoints or timeRange < 3.:
            predictCurrentMag = False

        if max(bigTimeArray) < todayMjd - 120:
            predictCurrentMag = False

        if predictCurrentMag:
            # USE ONLY THE LAST N DAYS OF DATA FOR LINEAR FIT
            mask = np.where(bigTimeArray -
                            bigTimeArray.max() < -lastNumDays, False, True)

            # DETERMINE GRADIENT OF SLOPE FROM LAST `LASTNUMDAYS` DAYS
            linearTimeArray = bigTimeArray[mask]
            linearMagArray = bigMagArray[mask]
            # FIT AND PLOT THE POLYNOMIAL ASSOCSIATED WITH ALL DATA SETS
            thisLinear = chebfit(linearTimeArray, linearMagArray, 1)
            gradient = thisLinear[1]

            firstAnchorPointTime = anchorSeparation + latestTime

            # CREATE THE ARRAY OF DATA USED TO GERNERATE THE POLYNOMIAL
            polyTimeArray = bigTimeArray
            polyMagArray = bigMagArray

            # ANCHOR THE POLYNOMIAL IN THE FUTURE SO THAT ALL PREDICTED LIGHTCURVES
            # EVENTUALLY FADE TO NOTHING
            extraTimes = np.arange(0, numAnchors) * \
                anchorSeparation + firstAnchorPointTime
            extraMags = np.ones(numAnchors) * anchorPointMag
            polyTimeArray = np.append(polyTimeArray, extraTimes)
            polyMagArray = np.append(polyMagArray, extraMags)

            # POLYNOMIAL LIMTIS
            xPolyLowerLimit = min(polyTimeArray) - 2.0
            xPolyUpperLimit = max(polyTimeArray) + 2.0

        # SET AXIS LIMITS
        xUpperLimit = 5
        yLowerLimit = min(bigMagArray) - 0.3
        yUpperLimit = max(bigMagArray) + 0.5
        yBorder = math.fabs((yUpperLimit - yLowerLimit)) * 0.1
        yLowerLimit -= yBorder
        yUpperLimit += yBorder

        # EXTEND LOWER X-LIMIT FOR NON-DETECTIONS
        xLowerTmp = xLowerLimit
        for t, m in zip(flatLimits["mjd"], flatLimits["mag"]):
            if m > yLowerLimit and t < xLowerTmp + 2 and t > xLowerLimit - 40:
                xLowerTmp = t - 2
        xLowerLimit = xLowerTmp

        if predictCurrentMag:
            thisPoly = chebfit(polyTimeArray, polyMagArray, polyOrder)
            # FLATTEN INTO A FUNCTION TO MAKE PLOTTING EASIER
            xData = np.arange(xPolyLowerLimit, todayMjd + 50, 1)
            flatLinear = chebval(xData, thisLinear)
            flatPoly = chebval(xData, thisPoly)
            plt.plot(xData, flatPoly, label="poly")
            plt.plot(xData, flatLinear, label="linear")

            # PREDICT A CURRENT MAGNITUDE FROM THE PLOT

            currentMag = chebval(todayMjd, thisPoly)
            self.log.debug(
                'currentMag: %(currentMag)0.2f, m=%(gradient)s' % locals())

            ls = "*g" % locals()
            currentMagArray = np.array([currentMag])
            nowArray = np.array([todayMjd])
            line = ax.plot(nowArray, currentMagArray,
                           ls, label="current estimate")

            lineExtras = ax.plot(extraTimes, extraMags, "+")

            # SET THE AXES / VIEWPORT FOR THE PLOT
            # if currentMag < yLowerLimit:
            #     yLowerLimit = currentMag - 0.4

        if currentMag > 23:
            currentMag = -9999.

        plt.clf()
        plt.cla()
        ax = fig.add_subplot(1, 1, 1)
        # print(currentMag)
        # print(bigTimeArray)
        # print(bigMagArray)

        # PLOT DATA VIA FILTER. MAGS AND LIMITS
        filterColor = {
            "r": "#29a329",
            "g": "#268bd2",
            "G": "#859900",
            "o": "#cb4b16",
            "c": "#2aa198",
            "U": "#6c71c4",
            "B": "blue",
            "V": "#008000",
            "R": "#e67300",
            "I": "#dc322f",
            "w": "#cc2900",
            "y": "#ff6666",
            "z": "#990000",
        }
        i = 0
        handles = []
        handlesAdded = []
        for k, v in list(dataset.items()):
            mag = v["mag"]
            magErr = v["magErr"]
            magMjd = v["magMjd"]
            limit = v["limit"]
            limitMjd = v["limitMjd"]
            magNoErr = v["magNoErr"]
            magNoErrMjd = v["magNoErrMjd"]
            magNoErrFudge = v["magNoErrFudge"]

            if k in filterColor:
                color = filterColor[k]
            else:
                color = "black"

            if len(limit):
                for l, m in zip(limit, limitMjd):
                    plt.text(m, l, u"\u21A7", fontname='STIXGeneral',
                             size=30, va='top', ha='center', clip_on=True, color=color, zorder=1)
            if len(magNoErr):
                theseMags = ax.errorbar(magNoErrMjd, magNoErr, yerr=magNoErrFudge, color=color, fmt='o', mfc=color,
                                        mec=color, zorder=2, ms=12., alpha=0.8, linewidth=1.2,  label=k, capsize=0)
                theseMags[-1][0].set_linestyle('--')

            if len(mag):
                theseMags = ax.errorbar(magMjd, mag, yerr=magErr, color=color, fmt='o', mfc=color,
                                        mec=color, zorder=3, ms=12., alpha=0.8, linewidth=1.2,  label=k, capsize=10)

            if not len(mag):
                theseMags = ax.errorbar([-500], [20], yerr=[0.2], color=color, fmt='o', mfc=color,
                                        mec=color, zorder=3, ms=12., alpha=0.8, linewidth=1.2,  label=k, capsize=10)

            if k not in handlesAdded:
                handles.append(theseMags)
                handlesAdded.append(k)

        # ADD LEGEND
        plt.legend(handles=handles, prop={
                   'size': 13.5}, bbox_to_anchor=(1., 1.25), loc=0, borderaxespad=0., ncol=18, scatterpoints=1)

        # RHS AXIS TICKS
        plt.setp(ax.xaxis.get_majorticklabels(),
                 rotation=45, horizontalalignment='right')
        ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%5.0f'))

        # CHANGE PLOT TO FIXED TIME
        # SETUP THE AXES
        xUpperLimit = fixedXUpperLimit
        ax.set_xlabel('MJD',  labelpad=20, fontsize=30)
        ax.set_ylabel('Magnitude',  labelpad=20, fontsize=30)
        ax.set_title('')
        ax.set_xlim([xLowerLimit, xUpperLimit])
        ax.set_ylim([yUpperLimit, yLowerLimit])
        ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%d'))

        # GENERATE UT DATE AXIS FOR TOP OF PLOT
        lower, upper = ax.get_xlim()
        utLower = converter.mjd_to_ut_datetime(mjd=lower, datetimeObject=True)
        utUpper = converter.mjd_to_ut_datetime(mjd=upper, datetimeObject=True)
        ax3 = ax.twiny()
        ax3.set_xlim([utLower, utUpper])
        ax3.grid(True)
        ax.xaxis.grid(False)
        plt.setp(ax3.xaxis.get_majorticklabels(),
                 rotation=45, horizontalalignment='left', fontsize=14)
        ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d, %y'))

        # Y TICK FORMAT
        y_formatter = mpl.ticker.FormatStrFormatter("%2.1f")
        ax.yaxis.set_major_formatter(y_formatter)

        # PRINT CURRENT MAG AS SANITY CHECK
        # fig.text(0.1, 1.02, currentMag, ha="left", fontsize=40)

        # RECURSIVELY CREATE MISSING DIRECTORIES
        if not os.path.exists(saveLocation):
            try:
                os.makedirs(saveLocation)
            except:
                pass
        # SAVE THE PLOT
        filepath = """%(saveLocation)s%(saveFileName)s.png""" % locals()
        plt.savefig(filepath, format='PNG', bbox_inches='tight', transparent=False,
                    pad_inches=0.4)
        # plt.show()
        plt.clf()  # clear figure
        plt.close()

        # TEST THAT PLOT FILE HAS ACTUALLY BEEN GENERATED
        try:
            with open(filepath):
                pass
            fileExists = True
        except IOError:
            raise IOError(
                "the path --pathToFile-- %s does not exist on this machine" %
                (filepath,))
            filepath = False

        self.log.debug('completed the ``_create_lightcurve_plot_file`` method')

        return filepath, currentMag, gradient

    def plot(
            self):
        """*generate a batch of lightcurves using multiprocessing given their transientBucketIds*

        **Return**

        - ``filepath`` -- path to the last generated plot file


        **Usage**

        ```python
        from marshallEngine.lightcurves import marshall_lightcurves
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=[28421489, 28121353, 4637952, 27409808]
        )
        lc.plot()
        ```

        """
        self.log.debug('starting the ``plot`` method')

        # DEFINE AN INPUT ARRAY
        total = len(self.transientBucketIds)

        thisDict = {"database settings": self.settings["database settings"]}

        if total:
            print("updating lightcurves for %(total)s transients" % locals())
            print()

        # USE IF ISSUES IN _plot_one FUNCTION
        # for transientBucketId in self.transientBucketIds:
        #     _plot_one(
        #         transientBucketId=transientBucketId,
        #         log=self.log,
        #         settings=self.settings
        #     )

        results = fmultiprocess(log=self.log, function=_plot_one,
                                inputArray=self.transientBucketIds, poolSize=False, timeout=3600, settings=self.settings)

        sqlQuery = ""
        updatedTransientBucketIds = []
        for t, r in zip(self.transientBucketIds, results):
            if not r[0]:
                # LIGHTCURVE NOT GENERATED
                continue
            updatedTransientBucketIds.append(t)
            filepath = r[0]
            currentMagnitude = r[1]
            gradient = r[2]
            sqlQuery += """update transientBucketSummaries set currentMagnitudeEstimate = %(currentMagnitude)s, currentMagnitudeEstimateUpdated = NOW(), recentSlopeOfLightcurve = %(gradient)s where transientBucketId = %(t)s;
            """ % locals()
        ids = []
        ids[:] = [str(i) for i in updatedTransientBucketIds]
        updatedTransientBucketIds = (",").join(ids)
        sqlQuery += "update pesstoObjects set master_pessto_lightcurve = 1 where transientBucketId in (%(updatedTransientBucketIds)s);" % locals(
        )

        if len(updatedTransientBucketIds):
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn,
            )
        else:
            filepath = False

        self.log.debug('completed the ``plot`` method')

        return filepath


def _plot_one(
        transientBucketId,
        log,
        settings):
    """*plot a single transeint lightcurve*

    **Key Arguments**

    - ``transientBucketId`` -- the id of the single transient to plot.
    - ``settings`` -- dictionary of settings
    - ``dbConn`` -- marshall database connection


    **Return**

    - ``filepath`` -- path to the plot file
    - ``currentMag`` -- an estimate of the current magnitude (from slope of recent LC). -9999 if inaccurate.
    - ``gradient`` -- gradient of slope of the recent LC. -9999 if inaccurate.

    """
    log.debug('starting the ``_plot_one`` method')

    # MULTIPROCESSING NEEDS ONE CONNECTION PERPROCESS
    sys.stdout.write("\x1b[1A\x1b[2K")
    print("updating LC for transient %(transientBucketId)s" % locals())
    dbConn = database(
        log=log,
        dbSettings=settings["database settings"]
    ).connect()

    # LC OBJECT
    lc = marshall_lightcurves(
        log=log,
        dbConn=dbConn,
        settings=settings,
        transientBucketIds=transientBucketId
    )

    cacheFolder = settings[
        "cache-directory"] + "/transients/"
    saveLocation = """%(cacheFolder)s/%(transientBucketId)s/""" % locals()

    # SELECT DATA AND PLOT THE SOURCE
    dataset, flatdata, flatLimits = lc._select_data_for_transient(
        transientBucketId)
    if dataset:
        filepath, currentMag, gradient = lc._create_lightcurve_plot_file(
            dataset=dataset,
            flatdata=flatdata,
            flatLimits=flatLimits,
            objectNames="test object",
            saveLocation=saveLocation,
            saveFileName="master_lightcurve"
        )
    else:
        return False, False, False

    log.debug('completed the ``_plot_one`` method')
    return filepath, currentMag, gradient

    # use the tab-trigger below for new method
    # xt-class-method
