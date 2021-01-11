#!/usr/local/bin/python
# encoding: utf-8
"""
*Generate the force-photometry lightcurves for ATLAS sources found in the marshall database*

:Author:
    David Young
"""
from __future__ import print_function
from __future__ import division
from builtins import zip
from builtins import str
from past.utils import old_div
import sys
import os
# SUPPRESS MATPLOTLIB WARNINGS
import warnings
warnings.filterwarnings("ignore")
import matplotlib as mpl
import matplotlib.pyplot as plt
import math
import numpy as np
from matplotlib import dates
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery, writequery
from datetime import datetime


def create_lc(
        log,
        cacheDirectory,
        epochs):
    """*create the atlas lc for one transient*

    **Key Arguments**

    - ``cacheDirectory`` -- the directory to add the lightcurve to
    - ``log`` -- logger
    - ``epochs`` -- dictionary of lightcurve data-points


    **Return**

    - None


    **Usage**

    .. todo::

        add usage info
        create a sublime snippet for usage

    ```python
    usage code
    ```

    """
    log.debug('starting the ``create_lc`` function')

    from astrocalc.times import conversions
    # CONVERTER TO CONVERT MJD TO DATE
    converter = conversions(
        log=log
    )

    # c = cyan, o = arange
    magnitudes = {
        'c': {'mjds': [], 'mags': [], 'magErrs': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': []},
    }

    summedMagnitudes = {
        'c': {'mjds': [], 'mags': [], 'magErrs': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': []},
    }

    limits = {
        'c': {'mjds': [], 'mags': [], 'magErrs': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': []},
    }

    discoveryMjd = False
    for epoch in epochs:
        objectName = epoch["atlas_designation"]
        if not epoch["fnu"]:
            continue

        if epoch["mjd_obs"] < 50000.:
            continue

        if not epoch["snr"] <= 5 and (not discoveryMjd or discoveryMjd > epoch["mjd_obs"]):
            discoveryMjd = epoch["mjd_obs"]

        if epoch["snr"] <= 3 and epoch["filter"] in ["c", "o", "I"]:
            limits[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
            limits[epoch["filter"]]["mags"].append(epoch["fnu"])
            limits[epoch["filter"]]["magErrs"].append(epoch["fnu_error"])
        elif epoch["filter"] in ["c", "o", "I"]:
            magnitudes[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
            magnitudes[epoch["filter"]]["mags"].append(epoch["fnu"])
            magnitudes[epoch["filter"]]["magErrs"].append(epoch["fnu_error"])

    for fil, d in list(magnitudes.items()):
        distinctMjds = {}
        for m, f, e in zip(d["mjds"], d["mags"], d["magErrs"]):
            key = str(int(math.floor(m)))
            if key not in distinctMjds:
                distinctMjds[key] = {
                    "mjds": [m],
                    "mags": [f],
                    "magErrs": [e]
                }
            else:
                distinctMjds[key]["mjds"].append(m)
                distinctMjds[key]["mags"].append(f)
                distinctMjds[key]["magErrs"].append(e)

        for k, v in list(distinctMjds.items()):
            summedMagnitudes[fil]["mjds"].append(
                old_div(sum(v["mjds"]), len(v["mjds"])))
            summedMagnitudes[fil]["mags"].append(
                old_div(sum(v["mags"]), len(v["mags"])))
            summedMagnitudes[fil]["magErrs"].append(sum(v["magErrs"]) / len(v["magErrs"]
                                                                            ) / math.sqrt(len(v["magErrs"])))

    if not discoveryMjd:
        return

    # COMMENT THIS LINE OUT TO PLOT ALL MAGNITUDE MEASUREMENTS INSTEAD OF
    # SUMMED
    magnitudes = summedMagnitudes

    # DUMP OUT SUMMED ATLAS MAGNITUDE
    # for m, l, e in zip(limits['o']["mjds"], limits['o']["mags"], limits['o']["magErrs"]):
    #     print "%(m)s, o, %(l)s, %(e)s, <3" % locals()
    # for m, l, e in zip(limits['c']["mjds"], limits['c']["mags"], limits['c']["magErrs"]):
    #     print "%(m)s, c, %(l)s, %(e)s, <3" % locals()

    # for m, l, e in zip(magnitudes['o']["mjds"], magnitudes['o']["mags"], magnitudes['o']["magErrs"]):
    #     print "%(m)s, o, %(l)s, %(e)s," % locals()
    # for m, l, e in zip(magnitudes['c']["mjds"], magnitudes['c']["mags"], magnitudes['c']["magErrs"]):
    #     print "%(m)s, c, %(l)s, %(e)s," % locals()

    discoveryUT = converter.mjd_to_ut_datetime(
        mjd=discoveryMjd, datetimeObject=True)

    discoveryUT = discoveryUT.strftime("%Y %m %d %H:%M")

    summedMagnitudes = {}

    # GENERATE THE FIGURE FOR THE PLOT
    fig = plt.figure(
        num=None,
        figsize=(10, 10),
        dpi=100,
        facecolor=None,
        edgecolor=None,
        frameon=True)

    mpl.rc('ytick', labelsize=20)
    mpl.rc('xtick', labelsize=20)
    mpl.rcParams.update({'font.size': 22})

    # FORMAT THE AXES
    ax = fig.add_axes(
        [0.1, 0.1, 0.8, 0.8],
        polar=False,
        frameon=True)
    ax.set_xlabel('MJD', labelpad=20)
    ax.set_ylabel('Apparent Magnitude', labelpad=15)

    # ax.set_yscale('log')

    # ATLAS OBJECT NAME LABEL AS TITLE
    fig.text(0.1, 1.02, objectName, ha="left", fontsize=40)

    # RHS AXIS TICKS
    plt.setp(ax.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='right')
    import matplotlib.ticker as mtick
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%5.0f'))

    # ADD MAGNITUDES AND LIMITS FOR EACH FILTER
    handles = []

    # SET AXIS LIMITS FOR MAGNTIUDES
    upperMag = -99
    lowerMag = 99

    # DETERMINE THE TIME-RANGE OF DETECTION FOR THE SOURCE
    mjdList = magnitudes['o']['mjds'] + \
        magnitudes['c']['mjds'] + magnitudes['I']['mjds']

    if len(mjdList) == 0:
        return

    lowerDetectionMjd = min(mjdList)
    upperDetectionMjd = max(mjdList)
    mjdLimitList = limits['o']['mjds'] + \
        limits['c']['mjds'] + limits['I']['mjds']
    priorLimitsFlavour = None
    for l in sorted(mjdLimitList):
        if l < lowerDetectionMjd and l > lowerDetectionMjd - 30.:
            priorLimitsFlavour = 1
    if not priorLimitsFlavour:
        for l in mjdLimitList:
            if l < lowerDetectionMjd - 30.:
                priorLimitsFlavour = 2
                lowerMJDLimit = l - 2

    if not priorLimitsFlavour:
        fig.text(0.1, -0.08, "* no recent pre-discovery detection limit > $5\\sigma$",
                 ha="left", fontsize=16)

    postLimitsFlavour = None

    for l in sorted(mjdLimitList):
        if l > upperDetectionMjd and l < upperDetectionMjd + 10.:
            postLimitsFlavour = 1
    if not postLimitsFlavour:
        for l in reversed(mjdLimitList):
            if l > upperDetectionMjd + 10.:
                postLimitsFlavour = 2
                upperMJDLimit = l + 2

    if priorLimitsFlavour or postLimitsFlavour:
        limits = {
            'c': {'mjds': [], 'mags': [], 'magErrs': []},
            'o': {'mjds': [], 'mags': [], 'magErrs': []},
            'I': {'mjds': [], 'mags': [], 'magErrs': []},
        }
        for epoch in epochs:
            if epoch["filter"] not in ["c", "o", "I"]:
                continue
            objectName = epoch["atlas_designation"]
            if not epoch["fnu"]:
                continue

            if epoch["mjd_obs"] < 50000.:
                continue

            if (epoch["snr"] <= 3 and ((priorLimitsFlavour == 1 and epoch["mjd_obs"] > lowerDetectionMjd - 30.) or (priorLimitsFlavour == 2 and epoch["mjd_obs"] > lowerMJDLimit) or priorLimitsFlavour == None) and ((postLimitsFlavour == 1 and epoch["mjd_obs"] < upperDetectionMjd + 10.) or (postLimitsFlavour == 2 and epoch["mjd_obs"] < upperMJDLimit) or postLimitsFlavour == None)):
                limits[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
                limits[epoch["filter"]]["mags"].append(epoch["fnu"])
                # 000 limits[epoch["filter"]]["magErrs"].append(epoch["dm"])
                limits[epoch["filter"]]["magErrs"].append(epoch["fnu_error"])

    allMags = magnitudes['o']['mags'] + magnitudes['c']['mags']
    magRange = max(allMags) - min(allMags)

    deltaMag = magRange * 0.1

    if len(limits['o']['mjds']):
        limitLeg = ax.errorbar(limits['o']['mjds'], limits['o']['mags'], yerr=limits[
            'o']['magErrs'], color='#FFA500', fmt='o', mfc='white', mec='#FFA500', zorder=1, ms=12., alpha=0.8, linewidth=0.4,  label='<3$\\sigma$ ', capsize=10, markeredgewidth=1.2)

        # ERROBAR CAP THICKNESS
        handles.append(limitLeg)
        limitLeg[1][0].set_markeredgewidth('0.4')
        limitLeg[1][1].set_markeredgewidth('0.4')

        # if min(limits['o']['mags']) < lowerMag:
        #     lowerMag = min(limits['o']['mags'])
    if len(limits['c']['mjds']):
        limitLeg = ax.errorbar(limits['c']['mjds'], limits['c']['mags'], yerr=limits[
            'c']['magErrs'], color='#2aa198', fmt='o', mfc='white', mec='#2aa198', zorder=1, ms=12., alpha=0.8, linewidth=0.4, label='<3$\\sigma$ ', capsize=10, markeredgewidth=1.2)
        # ERROBAR CAP THICKNESS
        limitLeg[1][0].set_markeredgewidth('0.4')
        limitLeg[1][1].set_markeredgewidth('0.4')
        if not len(handles):
            handles.append(limitLeg)

    if len(limits['I']['mjds']):
        limitLeg = ax.errorbar(limits['I']['mjds'], limits['I']['mags'], yerr=limits[
            'I']['magErrs'], color='#dc322f', fmt='o', mfc='white', mec='#dc322f', zorder=1, ms=12., alpha=0.8, linewidth=0.4, label='<3$\\sigma$ ', capsize=10, markeredgewidth=1.2)
        # ERROBAR CAP THICKNESS
        limitLeg[1][0].set_markeredgewidth('0.4')
        limitLeg[1][1].set_markeredgewidth('0.4')
        if not len(handles):
            handles.append(limitLeg)

    if len(magnitudes['o']['mjds']):
        orangeMag = ax.errorbar(magnitudes['o']['mjds'], magnitudes['o']['mags'], yerr=magnitudes[
            'o']['magErrs'], color='#FFA500', fmt='o', mfc='#FFA500', mec='#FFA500', zorder=1, ms=12., alpha=0.8, linewidth=1.2,  label='o-band mag ', capsize=10)

        # ERROBAR CAP THICKNESS
        orangeMag[1][0].set_markeredgewidth('0.7')
        orangeMag[1][1].set_markeredgewidth('0.7')
        handles.append(orangeMag)
        if max(np.array(magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs']))

        if min(np.array(magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs']))
            lowerMagIndex = np.argmin((
                magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs']))

    if len(magnitudes['c']['mjds']):
        cyanMag = ax.errorbar(magnitudes['c']['mjds'], magnitudes['c']['mags'], yerr=magnitudes[
            'c']['magErrs'], color='#2aa198', fmt='o', mfc='#2aa198', mec='#2aa198', zorder=1, ms=12., alpha=0.8, linewidth=1.2, label='c-band mag ', capsize=10)
        # ERROBAR CAP THICKNESS
        cyanMag[1][0].set_markeredgewidth('0.7')
        cyanMag[1][1].set_markeredgewidth('0.7')
        handles.append(cyanMag)
        if max(np.array(magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs']))

        if min(np.array(magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs']))
            lowerMagIndex = np.argmin(
                (magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs']))

    if len(magnitudes['I']['mjds']):
        cyanMag = ax.errorbar(magnitudes['I']['mjds'], magnitudes['I']['mags'], yerr=magnitudes[
            'I']['magErrs'], color='#dc322f', fmt='o', mfc='#dc322f', mec='#dc322f', zorder=1, ms=12., alpha=0.8, linewidth=1.2, label='I-band mag ', capsize=10)
        # ERROBAR CAP THICKNESS
        cyanMag[1][0].set_markeredgewidth('0.7')
        cyanMag[1][1].set_markeredgewidth('0.7')
        handles.append(cyanMag)
        if max(np.array(magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs']))

        if min(np.array(magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs']))
            lowerMagIndex = np.argmin(
                (magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs']))

    plt.legend(handles=handles, prop={
               'size': 13.5}, bbox_to_anchor=(0.95, 1.2), loc=0, borderaxespad=0., ncol=4, scatterpoints=1)

    # SET THE TEMPORAL X-RANGE
    allMjd = magnitudes['o']['mjds'] + magnitudes['c']['mjds']
    xmin = min(allMjd) - 5.
    xmax = max(allMjd) + 5.
    ax.set_xlim([xmin, xmax])

    ax.set_ylim([0. - deltaMag, upperMag + deltaMag])
    y_formatter = mpl.ticker.FormatStrFormatter("%2.1f")
    ax.yaxis.set_major_formatter(y_formatter)

    # PLOT THE MAGNITUDE SCALE
    axisUpperFlux = upperMag
    axisLowerFlux = 1e-29

    axisLowerMag = -2.5 * math.log10(axisLowerFlux) - 48.6
    axisUpperMag = -2.5 * math.log10(axisUpperFlux) - 48.6

    ax.set_yticks([2.2])
    import matplotlib.ticker as ticker

    magLabels = [20., 19.5, 19.0, 18.5,
                 18.0, 17.5, 17.0, 16.5, 16.0, 15.5, 15.0]
    magFluxes = [pow(10, old_div(-(m + 48.6), 2.5)) * 1e27 for m in magLabels]

    ax.yaxis.set_major_locator(ticker.FixedLocator((magFluxes)))
    ax.yaxis.set_major_formatter(ticker.FixedFormatter((magLabels)))
    # FLIP THE MAGNITUDE AXIS
    # plt.gca().invert_yaxis()

    # ADD SECOND Y-AXIS
    ax2 = ax.twinx()
    ax2.set_ylim([0. - deltaMag, upperMag + deltaMag])
    ax2.yaxis.set_major_formatter(y_formatter)

    # RELATIVE TIME SINCE DISCOVERY
    lower, upper = ax.get_xlim()
    utLower = converter.mjd_to_ut_datetime(mjd=lower, datetimeObject=True)
    utUpper = converter.mjd_to_ut_datetime(mjd=upper, datetimeObject=True)

    # ADD SECOND X-AXIS
    ax3 = ax.twiny()
    ax3.set_xlim([utLower, utUpper])
    ax3.grid(True)
    ax.xaxis.grid(False)
    plt.setp(ax3.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='left')
    ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d'))
    # ax3.set_xlabel('Since Discovery (d)',  labelpad=10,)

    # # Put a legend on plot
    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # ax.legend(loc='top right', bbox_to_anchor=(1.1, 0.5), prop={'size': 8})

    # from matplotlib.ticker import LogLocator
    # minorLocator = LogLocator(base=10, subs=[2.0, 5.0])
    # if magRange < 1.5:
    #     minorLocator = LogLocator(
    #         base=10, subs=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])
    # ax2.yaxis.set_minor_locator(minorLocator)
    # ax2.yaxis.set_minor_formatter(y_formatter)
    # ax2.tick_params(axis='y', which='major', pad=5)
    # ax2.tick_params(axis='y', which='minor', pad=5)
    ax2.set_ylabel('$F_{nu} \\times 1e^{27}$', rotation=-90.,  labelpad=27)

    discoveryText = "discovery epoch\nmjd %(discoveryMjd)2.2f\n%(discoveryUT)s UT" % locals(
    )
    ax.text(0.05, 0.95, discoveryText,
            verticalalignment='top', horizontalalignment='left',
            transform=ax.transAxes,
            color='black', fontsize=12, linespacing=1.5)

    ax2.grid(False)
    # SAVE PLOT TO FILE
    pathToOutputPlotFolder = ""
    title = objectName + " forced photometry lc"
    # Recursively create missing directories
    if not os.path.exists(cacheDirectory):
        os.makedirs(cacheDirectory)
    fileName = cacheDirectory + "/atlas_fp_lightcurve.png"
    plt.savefig(fileName, bbox_inches='tight', transparent=False,
                pad_inches=0.1)

    # CLEAR FIGURE
    plt.clf()

    log.debug('completed the ``create_lc`` function')
    return None


def generate_atlas_lightcurves(
        dbConn,
        log,
        settings):
    """generate atlas lightcurves

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``settings`` -- settings for the marshall.


    **Return**

    - None


    **Usage**

    ..todo::

        add usage info
        create a sublime snippet for usage

    ```python
    usage code
    ```


    ..todo::

        - @review: when complete, clean generate_atlas_lightcurves function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug('starting the ``generate_atlas_lightcurves`` function')

    # SELECT OUT THE SOURCES THAT NEED THEIR LCS UPDATED
    sqlQuery = u"""
        SELECT
            a.transientBucketId
        FROM
            (SELECT
                transientBucketId, dateCreated
            FROM
                transientBucket
            WHERE
                survey = 'ATLAS FP' and limitingMag = 0
            ORDER BY dateCreated DESC) a,
            pesstoObjects p
            where p.transientBucketId=a.transientBucketId
            and ((p.atlas_fp_lightcurve < a.dateCreated) or p.atlas_fp_lightcurve is null)
        GROUP BY a.transientBucketId;
    """ % locals()
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    total = len(rows)
    if total > 300:
        print("ATLAS lightcurves need generated for %(total)s sources - generating next 300" % locals())
        rows = rows[:300]
        total = len(rows)
    else:
        print("Generating ATLAS lightcurves for %(total)s sources" % locals())

    index = 1
    for row in rows:

        # SELECT OUT THE LIGHT CURVE DATA FOR A GIVEN ATLAS TRANSIENT
        transientBucketId = row["transientBucketId"]

        if index > 1:
            # Cursor up one line and clear line
            sys.stdout.write("\x1b[1A\x1b[2K")

        percent = (old_div(float(index), float(total))) * 100.
        print('%(index)s/%(total)s (%(percent)1.1f%% done): generating ATLAS LC for transientBucketId: %(transientBucketId)s' % locals())
        index += 1

        sqlQuery = u"""
            SELECT
                atlas_designation,
                mjd_obs,
                filter,
                marshall_mag as mag,
                marshall_mag_error as dm,
                fnu*1e27 as fnu,
                fnu_error*1e27 as fnu_error,
                snr,
                zp,
                marshall_limiting_mag as limiting_mag
            FROM
                fs_atlas_forced_phot
            WHERE
                (skyfit > 0) and
                atlas_designation in (SELECT distinct name
            FROM
                transientBucket
            WHERE
                survey = 'ATLAS FP'
                    AND transientBucketId = %(transientBucketId)s
                    AND dateDeleted IS NULL)
            and fnu is not null;
        """ % locals()
        epochs = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        # FIND THE CACHE DIR FOR THE SOURCE
        cacheDirectory = settings[
            "cache-directory"] + "/transients/" + str(transientBucketId)

        # CREATE THE PLOT FOR THIS ONE ATLAS SOURCE
        create_lc(
            log=log,
            cacheDirectory=cacheDirectory,
            epochs=epochs
        )

        # UPDATE THE OBJECTS FLAG
        sqlQuery = """update pesstoObjects set atlas_fp_lightcurve = NOW() where transientBucketID = %(transientBucketId)s """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

    log.debug('completed the ``generate_atlas_lightcurves`` function')
    return None


def create_lc_depreciated(
        log,
        cacheDirectory,
        epochs):
    """*create the atlas lc for one transient*

    **Key Arguments**

    - ``cacheDirectory`` -- the directory to add the lightcurve to
    - ``log`` -- logger
    - ``epochs`` -- dictionary of lightcurve data-points


    **Return**

    - None


    **Usage**

    .. todo::

        add usage info
        create a sublime snippet for usage

    ```python
    usage code
    ```

    """
    log.debug('starting the ``create_lc`` function')

    # c = cyan, o = arange
    magnitudes = {
        'c': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
    }

    limits = {
        'c': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
    }

    discoveryMjd = False
    for epoch in epochs:
        if epoch["filter"] not in ["c", "o", "I"]:
            continue
        objectName = epoch["atlas_designation"]
        if epoch["limiting_mag"] == 1:
            limits[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
            limits[epoch["filter"]]["mags"].append(epoch["mag"])
            limits[epoch["filter"]]["magErrs"].append(epoch["dm"])
            limits[epoch["filter"]]["zp"].append(epoch["zp"])
            flux = 10**(old_div((float(epoch["zp"]) -
                                 float(epoch["mag"])), 2.5))
            limits[epoch["filter"]]["flux"].append(flux)
        else:
            if not discoveryMjd or discoveryMjd > epoch["mjd_obs"]:
                discoveryMjd = epoch["mjd_obs"]
            magnitudes[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
            magnitudes[epoch["filter"]]["mags"].append(epoch["mag"])
            magnitudes[epoch["filter"]]["magErrs"].append(epoch["dm"])
            magnitudes[epoch["filter"]]["zp"].append(epoch["zp"])
            flux = 10**(old_div((float(epoch["zp"]) -
                                 float(epoch["mag"])), 2.5))
            magnitudes[epoch["filter"]]["flux"].append(flux)

    # GENERATE THE FIGURE FOR THE PLOT
    fig = plt.figure(
        num=None,
        figsize=(10, 10),
        dpi=100,
        facecolor=None,
        edgecolor=None,
        frameon=True)

    mpl.rc('ytick', labelsize=20)
    mpl.rc('xtick', labelsize=20)
    mpl.rcParams.update({'font.size': 22})

    # FORMAT THE AXES
    ax = fig.add_axes(
        [0.1, 0.1, 0.8, 0.8],
        polar=False,
        frameon=True)
    ax.set_xlabel('MJD', labelpad=20)
    ax.set_ylabel('Apparent Magnitude', labelpad=15)

    # fig.text(0.1, 1.0, "ATLAS", ha="left", color="#2aa198", fontsize=40)
    # fig.text(0.275, 1.0, objectName.replace("ATLAS", ""),
    #          color="#FFA500", ha="left", fontsize=40)
    fig.text(0.1, 1.02, objectName, ha="left", fontsize=40)

    # ax.set_title(objectName, y=1.10, ha='left', position=(0, 1.11))
    plt.setp(ax.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='right')
    import matplotlib.ticker as mtick
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%5.0f'))

    # ADD MAGNITUDES AND LIMITS FOR EACH FILTER
    # plt.scatter(magnitudes['o']['mjds'], magnitudes['o']['mags'], s=20., c=None, alpha=0.9,
    # edgecolors='#FFA500', linewidth=1.0, facecolors='#FFA500')
    handles = []

    # SET AXIS LIMITS FOR MAGNTIUDES
    upperMag = -99
    lowerMag = 99

    # DETERMINE THE TIME-RANGE OF DETECTION FOR THE SOURCE
    mjdList = magnitudes['o']['mjds'] + \
        magnitudes['c']['mjds'] + magnitudes['I']['mjds']

    if len(mjdList) == 0:
        return

    lowerDetectionMjd = min(mjdList)
    upperDetectionMjd = max(mjdList)
    mjdLimitList = limits['o']['mjds'] + \
        limits['c']['mjds'] + limits['I']['mjds']
    priorLimitsFlavour = None
    for l in sorted(mjdLimitList):
        if l < lowerDetectionMjd and l > lowerDetectionMjd - 30.:
            priorLimitsFlavour = 1
    if not priorLimitsFlavour:
        for l in mjdLimitList:
            if l < lowerDetectionMjd - 30.:
                priorLimitsFlavour = 2
                lowerMJDLimit = l - 2

    if not priorLimitsFlavour:
        fig.text(0.1, -0.08, "* no recent pre-discovery detection limit > $5\\sigma$",
                 ha="left", fontsize=16)

    postLimitsFlavour = None

    for l in sorted(mjdLimitList):
        if l > upperDetectionMjd and l < upperDetectionMjd + 10.:
            postLimitsFlavour = 1
    if not postLimitsFlavour:
        for l in reversed(mjdLimitList):
            if l > upperDetectionMjd + 10.:
                postLimitsFlavour = 2
                upperMJDLimit = l + 2

    if priorLimitsFlavour or postLimitsFlavour:
        limits = {
            'c': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
            'o': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
            'I': {'mjds': [], 'mags': [], 'magErrs': [], 'flux': [], 'zp': []},
        }
        for epoch in epochs:
            objectName = epoch["atlas_designation"]
            if (epoch["limiting_mag"] == 1 and ((priorLimitsFlavour == 1 and epoch["mjd_obs"] > lowerDetectionMjd - 30.) or (priorLimitsFlavour == 2 and epoch["mjd_obs"] > lowerMJDLimit) or priorLimitsFlavour == None) and ((postLimitsFlavour == 1 and epoch["mjd_obs"] < upperDetectionMjd + 10.) or (postLimitsFlavour == 2 and epoch["mjd_obs"] < upperMJDLimit) or postLimitsFlavour == None)):
                limits[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
                limits[epoch["filter"]]["mags"].append(epoch["mag"])
                limits[epoch["filter"]]["magErrs"].append(epoch["dm"])
                limits[epoch["filter"]]["zp"].append(epoch["zp"])
                flux = 10**(old_div((float(epoch["zp"]) -
                                     float(epoch["mag"])), 2.5))
                limits[epoch["filter"]]["flux"].append(flux)

    allMags = limits['o']['mags'] + limits['c']['mags'] + \
        magnitudes['o']['mags'] + magnitudes['c']['mags']
    magRange = max(allMags) - min(allMags)
    if magRange < 4.:
        deltaMag = 0.1
    else:
        deltaMag = magRange * 0.08

    if len(limits['o']['mjds']):
        limitLeg = plt.scatter(limits['o']['mjds'], limits['o']['mags'], s=170., c=None, alpha=0.8,
                               edgecolors='#FFA500', linewidth=1.0, facecolors='none', label="$5\\sigma$ limit  ")
        handles.append(limitLeg)
        if max(limits['o']['mags']) > upperMag:
            upperMag = max(limits['o']['mags'])
            upperMagIndex = np.argmax(limits['o']['mags'])
            # MAG PADDING
            upperFlux = limits['o']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))

        # if min(limits['o']['mags']) < lowerMag:
        #     lowerMag = min(limits['o']['mags'])
    if len(limits['c']['mjds']):
        limitLeg = plt.scatter(limits['c']['mjds'], limits['c']['mags'], s=170., c=None, alpha=0.8,
                               edgecolors='#2aa198', linewidth=1.0, facecolors='none', label="$5\\sigma$ limit  ")
        if len(handles) == 0:
            handles.append(limitLeg)
        if max(limits['c']['mags']) > upperMag:
            upperMag = max(limits['c']['mags'])
            upperMagIndex = np.argmax(limits['c']['mags'])
            # MAG PADDING
            upperFlux = limits['c']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))
        # if min(limits['c']['mags']) < lowerMag:
        #     lowerMag = min(limits['c']['mags'])

    if len(limits['I']['mjds']):
        limitLeg = plt.scatter(limits['I']['mjds'], limits['I']['mags'], s=170., c=None, alpha=0.8,
                               edgecolors='#dc322f', linewidth=1.0, facecolors='none', label="$5\\sigma$ limit  ")
        if len(handles) == 0:
            handles.append(limitLeg)
        if max(limits['I']['mags']) > upperMag:
            upperMag = max(limits['I']['mags'])
            upperMagIndex = np.argmax(limits['I']['mags'])
            # MAG PADDING
            upperFlux = limits['I']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))
    if len(magnitudes['o']['mjds']):
        orangeMag = plt.errorbar(magnitudes['o']['mjds'], magnitudes['o']['mags'], yerr=magnitudes[
            'o']['magErrs'], color='#FFA500', fmt='o', mfc='#FFA500', mec='#FFA500', zorder=1, ms=12., alpha=0.8, linewidth=1.2,  label='o-band mag ', capsize=10)

        # ERROBAR STYLE
        orangeMag[-1][0].set_linestyle('--')
        # ERROBAR CAP THICKNESS
        orangeMag[1][0].set_markeredgewidth('0.7')
        orangeMag[1][1].set_markeredgewidth('0.7')
        handles.append(orangeMag)
        if max(np.array(magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['o']['mags']) + np.array(magnitudes['o']['magErrs']))
            # MAG PADDING
            upperFlux = magnitudes['o']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))

        if min(np.array(magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs']))
            lowerMagIndex = np.argmin((
                magnitudes['o']['mags']) - np.array(magnitudes['o']['magErrs']))
            # MAG PADDING
            lowerFlux = magnitudes['o']['flux'][
                lowerMagIndex] + 10**(old_div(deltaMag, 2.5))
    if len(magnitudes['c']['mjds']):
        cyanMag = plt.errorbar(magnitudes['c']['mjds'], magnitudes['c']['mags'], yerr=magnitudes[
            'c']['magErrs'], color='#2aa198', fmt='o', mfc='#2aa198', mec='#2aa198', zorder=1, ms=12., alpha=0.8, linewidth=1.2, label='c-band mag ', capsize=10)
        # ERROBAR STYLE
        cyanMag[-1][0].set_linestyle('--')
        # ERROBAR CAP THICKNESS
        cyanMag[1][0].set_markeredgewidth('0.7')
        cyanMag[1][1].set_markeredgewidth('0.7')
        handles.append(cyanMag)
        if max(np.array(magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['c']['mags']) + np.array(magnitudes['c']['magErrs']))
            # MAG PADDING
            upperFlux = magnitudes['c']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))
        if min(np.array(magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs']))
            lowerMagIndex = np.argmin(
                (magnitudes['c']['mags']) - np.array(magnitudes['c']['magErrs']))
            # MAG PADDING
            lowerFlux = magnitudes['c']['flux'][
                lowerMagIndex] + 10**(old_div(deltaMag, 2.5))
    if len(magnitudes['I']['mjds']):
        cyanMag = plt.errorbar(magnitudes['I']['mjds'], magnitudes['I']['mags'], yerr=magnitudes[
            'I']['magErrs'], color='#dc322f', fmt='o', mfc='#dc322f', mec='#dc322f', zorder=1, ms=12., alpha=0.8, linewidth=1.2, label='I-band mag ', capsize=10)
        # ERROBAR STYLE
        cyanMag[-1][0].set_linestyle('--')
        # ERROBAR CAP THICKNESS
        cyanMag[1][0].set_markeredgewidth('0.7')
        cyanMag[1][1].set_markeredgewidth('0.7')
        handles.append(cyanMag)
        if max(np.array(magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs'])) > upperMag:
            upperMag = max(
                np.array(magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs']))
            upperMagIndex = np.argmax((
                magnitudes['I']['mags']) + np.array(magnitudes['I']['magErrs']))
            # MAG PADDING
            upperFlux = magnitudes['I']['flux'][
                upperMagIndex] - 10**(old_div(deltaMag, 2.5))
        if min(np.array(magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs'])) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs']))
            lowerMagIndex = np.argmin(
                (magnitudes['I']['mags']) - np.array(magnitudes['I']['magErrs']))
            # MAG PADDING
            lowerFlux = magnitudes['I']['flux'][
                lowerMagIndex] + 10**(old_div(deltaMag, 2.5))

    plt.legend(handles=handles, prop={
               'size': 13.5}, bbox_to_anchor=(1., 1.2), loc=0, borderaxespad=0., ncol=4, scatterpoints=1)

    # SET THE TEMPORAL X-RANGE
    allMjd = limits['o']['mjds'] + limits['c']['mjds'] + \
        magnitudes['o']['mjds'] + magnitudes['c']['mjds']
    xmin = min(allMjd) - 2.
    xmax = max(allMjd) + 2.
    ax.set_xlim([xmin, xmax])

    ax.set_ylim([lowerMag - deltaMag, upperMag + deltaMag])
    # FLIP THE MAGNITUDE AXIS
    plt.gca().invert_yaxis()

    # ADD SECOND Y-AXIS
    ax2 = ax.twinx()
    ax2.set_yscale('log')
    ax2.set_ylim([upperFlux, lowerFlux])
    y_formatter = mpl.ticker.FormatStrFormatter("%d")
    ax2.yaxis.set_major_formatter(y_formatter)

    # RELATIVE TIME SINCE DISCOVERY
    lower, upper = ax.get_xlim()
    from astrocalc.times import conversions
    # CONVERTER TO CONVERT MJD TO DATE
    converter = conversions(
        log=log
    )
    utLower = converter.mjd_to_ut_datetime(mjd=lower, datetimeObject=True)
    utUpper = converter.mjd_to_ut_datetime(mjd=upper, datetimeObject=True)

    # ADD SECOND X-AXIS
    ax3 = ax.twiny()
    ax3.set_xlim([utLower, utUpper])
    ax3.grid(True)
    ax.xaxis.grid(False)
    plt.setp(ax3.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='left')
    ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d'))
    # ax3.set_xlabel('Since Discovery (d)',  labelpad=10,)

    # # Put a legend on plot
    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # ax.legend(loc='top right', bbox_to_anchor=(1.1, 0.5), prop={'size': 8})

    from matplotlib.ticker import LogLocator
    minorLocator = LogLocator(base=10, subs=[2.0, 5.0])
    if magRange < 1.5:
        minorLocator = LogLocator(
            base=10, subs=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])
    ax2.yaxis.set_minor_locator(minorLocator)
    ax2.yaxis.set_minor_formatter(y_formatter)
    ax2.tick_params(axis='y', which='major', pad=5)
    ax2.tick_params(axis='y', which='minor', pad=5)
    ax2.set_ylabel('Approx. Counts', rotation=-90.,  labelpad=27)

    ax2.grid(False)
    # SAVE PLOT TO FILE
    pathToOutputPlotFolder = ""
    title = objectName + " forced photometry lc"
    # Recursively create missing directories
    if not os.path.exists(cacheDirectory):
        os.makedirs(cacheDirectory)
    fileName = cacheDirectory + "/atlas_fp_lightcurve.png"
    plt.savefig(fileName, bbox_inches='tight', transparent=False,
                pad_inches=0.1)

    # CLEAR FIGURE
    plt.clf()

    log.debug('completed the ``create_lc`` function')
    return None

    # use the tab-trigger below for new function
    # xt-def-function
