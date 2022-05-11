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
from datetime import datetime
from operator import itemgetter
from fundamentals.stats import rolling_window_sigma_clip
import matplotlib.ticker as mtick
from astrocalc.times import conversions
from fundamentals import fmultiprocess
from fundamentals.mysql import database, readquery, writequery


def generate_atlas_lightcurves(
        dbConn,
        log,
        settings):
    """generate all atlas FP lightcurves (clipped and stacked)

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``settings`` -- settings for the marshall.

    ```python
    from marshallEngine.feeders.atlas.lightcurve import generate_atlas_lightcurves
    generate_atlas_lightcurves(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    ```
    """
    log.debug('starting the ``generate_atlas_lightcurves`` function')

    # SELECT SOURCES THAT NEED THEIR ATLAS FP LIGHTCURVES CREATED/UPDATED
    sqlQuery = u"""
        SELECT
                t.transientBucketId
            FROM
                transientBucket t ,pesstoObjects p
            WHERE
                p.transientBucketId=t.transientBucketId
                and t.survey = 'ATLAS FP' and t.limitingMag = 0
                and ((DATE_SUB(p.atlas_fp_lightcurve, INTERVAL 2 HOUR) < t.dateCreated and p.atlas_fp_lightcurve != 0) or p.atlas_fp_lightcurve is null)
            GROUP BY t.transientBucketId;
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    transientIds = [r["transientBucketId"] for r in rows]

    total = len(transientIds)
    if total > 1000:
        print("ATLAS lightcurves need generated for %(total)s sources - generating next 1000" % locals())
        transientIds = transientIds[:1000]
        total = len(transientIds)
    else:
        print("Generating ATLAS lightcurves for %(total)s sources" % locals())

    # SETUP THE INITIAL FIGURE FOR THE PLOT (ONLY ONCE)
    fig = plt.figure(
        num=None,
        figsize=(10, 10),
        dpi=100,
        facecolor=None,
        edgecolor=None,
        frameon=True)
    mpl.rc('ytick', labelsize=18)
    mpl.rc('xtick', labelsize=18)
    mpl.rcParams.update({'font.size': 22})

    # FORMAT THE AXES
    ax = fig.add_axes(
        [0.1, 0.1, 0.8, 0.8],
        polar=False,
        frameon=True)
    ax.set_xlabel('MJD', labelpad=20)
    ax.set_yticks([2.2])

    # RHS AXIS TICKS
    plt.setp(ax.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='right')
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%5.0f'))

    y_formatter = mpl.ticker.FormatStrFormatter("%2.1f")
    ax.yaxis.set_major_formatter(y_formatter)
    ax.xaxis.grid(False)

    # ADD SECOND Y-AXIS
    ax2 = ax.twinx()
    ax2.yaxis.set_major_formatter(y_formatter)
    ax2.set_ylabel('Flux ($\mu$Jy)', rotation=-90.,  labelpad=27)
    ax2.grid(False)

    # ADD SECOND X-AXIS
    ax3 = ax.twiny()
    ax3.grid(True)
    plt.setp(ax3.xaxis.get_majorticklabels(),
             rotation=45, horizontalalignment='left')

    # CONVERTER TO CONVERT MJD TO DATE
    converter = conversions(
        log=log
    )

    if len(transientIds) < 3:
        plotPaths = []
        for transientBucketId in transientIds:
            plotPaths.append(
                plot_single_result(
                    log=log,
                    transientBucketId=transientBucketId,
                    fig=fig,
                    converter=converter,
                    ax=ax,
                    settings=settings)
            )
    else:
        log.info("""starting multiprocessing""")
        plotPaths = fmultiprocess(
            log=log,
            function=plot_single_result,
            inputArray=transientIds,
            poolSize=False,
            timeout=7200,
            fig=fig,
            converter=converter,
            ax=ax,
            settings=settings
        )
        log.info("""finished multiprocessing""")

    # REMOVE MISSING PLOTStrn
    transientIdGood = [t for p, t in zip(plotPaths, transientIds) if p]
    transientIdBad = [t for p, t in zip(plotPaths, transientIds) if p is None]

    # UPDATE THE atlas_fp_lightcurve DATE FOR TRANSIENTS WE HAVE JUST
    # GENERATED PLOTS FOR
    if len(transientIdGood):
        transientIdGood = (",").join([str(t) for t in transientIdGood])
        sqlQuery = f"""update pesstoObjects set atlas_fp_lightcurve = NOW() where transientBucketID in ({transientIdGood})"""
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

    # UPDATE THE atlas_fp_lightcurve DATE FOR TRANSIENTS WE HAVE JUST
    # GENERATED PLOTS FOR
    if len(transientIdBad):
        transientIdBad = (",").join([str(t) for t in transientIdBad])
        sqlQuery = f"""update pesstoObjects set atlas_fp_lightcurve = NOW() where transientBucketID in ({transientIdBad})"""
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

    log.debug('completed the ``generate_atlas_lightcurves`` function')
    return None


def plot_single_result(
        transientBucketId,
        log,
        fig,
        converter,
        ax,
        settings):
    """*plot single result*

    **Key Arguments:**
        - `transientBucketId` -- thr transient ID for the source in the database
        - `log` -- logger
        - `fig` -- the matplotlib figure to use for the plot
        - `converter` -- converter to switch mjd to ut-date
        - `ax` -- plot axis
        - `settings` -- dictionary of settings (from yaml settings file)

    **Return:**
        - `filePath` -- path to the output PNG plot
    """
    log.info('starting the ``plot_single_result`` method')

    # SETUP DATABASE CONNECTION
    dbConn = database(
        log=log,
        dbSettings=settings["database settings"]
    ).connect()

    # GET THE DATA FROM THE DATABASE
    sqlQuery = u"""
        SELECT
            atlas_designation,
            mjd_obs,
            filter,
            marshall_mag as mag,
            marshall_mag_error as dm,
            fnu*1e29 as uJy,
            fnu_error*1e29 as duJy,
            snr,
            zp,
            marshall_limiting_mag as limiting_mag
        FROM
            fs_atlas_forced_phot
        WHERE
            transientBucketId = %(transientBucketId)s
        and fnu is not null;
    """ % locals()

    epochs = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    ax2 = get_twin_axis(ax, "x")
    ax3 = get_twin_axis(ax, "y")

    # ax = fig.gca()
    epochs = sigma_clip_data(log=log, fpData=epochs)


    # c = cyan, o = arange
    magnitudes = {
        'c': {'mjds': [], 'mags': [], 'magErrs': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': []},
    }

    # SPLIT BY FILTER
    for epoch in epochs:
        if epoch["filter"] in ["c", "o", "I"]:
            magnitudes[epoch["filter"]]["mjds"].append(epoch["mjd_obs"])
            magnitudes[epoch["filter"]]["mags"].append(epoch["uJy"])
            magnitudes[epoch["filter"]]["magErrs"].append(epoch["duJy"])

    # STACK PHOTOMETRY IF REQUIRED
    magnitudes = stack_photometry(log=log,
                                  magnitudes=magnitudes, binningDays=1.)

    # ADD MAGNITUDES AND LIMITS FOR EACH FILTER
    handles = []

    # SET AXIS LIMITS FOR MAGNTIUDES
    upperMag = -99999999999
    lowerMag = 99999999999

    # DETERMINE THE TIME-RANGE OF DETECTION FOR THE SOURCE
    mjdList = magnitudes['o']['mjds'] + \
        magnitudes['c']['mjds'] + magnitudes['I']['mjds']
    if len(mjdList) == 0:
        log.error(f'{transientBucketId} does not have enough data to plot LC')
        return None
    lowerDetectionMjd = min(mjdList)
    upperDetectionMjd = max(mjdList)

    # DETERMIN MAGNITUDE RANGE
    allMags = magnitudes['o']['mags'] + magnitudes['c']['mags']
    magRange = max(allMags) - min(allMags)
    deltaMag = magRange * 0.1

    if len(magnitudes['o']['mjds']):
        orangeMag = ax.errorbar(magnitudes['o']['mjds'], magnitudes['o']['mags'], yerr=magnitudes[
            'o']['magErrs'], color='#FFA500', fmt='o', mfc='#FFA500', mec='#FFA500', zorder=1, ms=12., alpha=0.8, linewidth=1.2,  label='o-band mag ', capsize=10)

        # ERROBAR CAP THICKNESS
        orangeMag[1][0].set_markeredgewidth('0.7')
        orangeMag[1][1].set_markeredgewidth('0.7')
        handles.append(orangeMag)
        errMask = np.array(magnitudes['o']['magErrs'])
        np.putmask(errMask, errMask > 30, 30)

        if max(np.array(magnitudes['o']['mags']) + errMask) > upperMag:
            upperMag = max(
                np.array(magnitudes['o']['mags']) + errMask)
            upperMagIndex = np.argmax((
                magnitudes['o']['mags']) + errMask)

        if min(np.array(magnitudes['o']['mags']) - errMask) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['o']['mags']) - errMask)
            lowerMagIndex = np.argmin((
                magnitudes['o']['mags']) - errMask)

    if len(magnitudes['c']['mjds']):
        cyanMag = ax.errorbar(magnitudes['c']['mjds'], magnitudes['c']['mags'], yerr=magnitudes[
            'c']['magErrs'], color='#2aa198', fmt='o', mfc='#2aa198', mec='#2aa198', zorder=1, ms=12., alpha=0.8, linewidth=1.2, label='c-band mag ', capsize=10)
        # ERROBAR CAP THICKNESS
        cyanMag[1][0].set_markeredgewidth('0.7')
        cyanMag[1][1].set_markeredgewidth('0.7')
        handles.append(cyanMag)
        errMask = np.array(magnitudes['c']['magErrs'])
        np.putmask(errMask, errMask > 30, 30)

        if max(np.array(magnitudes['c']['mags']) + errMask) > upperMag:
            upperMag = max(
                np.array(magnitudes['c']['mags']) + errMask)
            upperMagIndex = np.argmax((
                magnitudes['c']['mags']) + errMask)

        if min(np.array(magnitudes['c']['mags']) - errMask) < lowerMag:
            lowerMag = min(
                np.array(magnitudes['c']['mags']) - errMask)
            lowerMagIndex = np.argmin(
                (magnitudes['c']['mags']) - errMask)

    # if self.firstPlot:
    plt.legend(handles=handles, prop={
        'size': 13.5}, bbox_to_anchor=(0.95, 1.2), loc=0, borderaxespad=0., ncol=4, scatterpoints=1)

    # SET THE TEMPORAL X-RANGE
    allMjd = magnitudes['o']['mjds'] + magnitudes['c']['mjds']
    xmin = min(allMjd) - 5.
    xmax = max(allMjd) + 5.
    mjdRange = xmax - xmin
    ax.set_xlim([xmin, xmax])
    ax.set_ylim([lowerMag - deltaMag, upperMag + deltaMag])

    # PLOT THE MAGNITUDE SCALE
    axisUpperFlux = upperMag
    axisLowerFlux = 1e-29
    axisLowerMag = -2.5 * math.log10(axisLowerFlux) + 23.9
    if axisUpperFlux > 0.:
        axisUpperMag = -2.5 * math.log10(axisUpperFlux) + 23.9
    else:
        axisUpperMag = None
    if axisUpperMag:
        ax.set_ylabel('Apparent Magnitude', labelpad=15)
        magLabels = [20., 17.0, 15.0, 14.0, 13.5, 13.0]
        if axisUpperMag < 14:
            magLabels = [20.0, 16.0, 15.0, 14.0,
                         13.0, 12.5, 12.0, 11.5, 11.0]
        elif axisUpperMag < 17:
            magLabels = [20.,
                         18.0, 17.0, 16.5, 16.0, 15.5, 15.0]
        elif axisUpperMag < 18:
            magLabels = [20., 19.5, 19.0, 18.5,
                         18.0, 17.5, 17.0, 16.5, 16.0, 15.5, 15.0]
        elif axisUpperMag < 20:
            magLabels = [20.5, 20.0, 19.5, 19.0, 18.5, 18.0]

        magFluxes = [pow(10, old_div(-(m - 23.9), 2.5)) for m in magLabels]

        ax.yaxis.set_major_locator(mtick.FixedLocator((magFluxes)))
        ax.yaxis.set_major_formatter(mtick.FixedFormatter((magLabels)))
    else:
        ax.set_yticks([])

    # ADD SECOND Y-AXIS
    ax2.set_ylim([lowerMag - deltaMag, upperMag + deltaMag])

    # RELATIVE TIME SINCE DISCOVERY
    lower, upper = ax.get_xlim()
    utLower = converter.mjd_to_ut_datetime(mjd=lower, datetimeObject=True)
    utUpper = converter.mjd_to_ut_datetime(mjd=upper, datetimeObject=True)
    ax3.set_xlim([utLower, utUpper])

    if mjdRange > 365:
        ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d %y'))
    else:
        ax3.xaxis.set_major_formatter(dates.DateFormatter('%b %d'))

    # FIND THE CACHE DIR FOR THE SOURCE
    cacheDirectory = settings[
        "cache-directory"] + "/transients/" + str(transientBucketId)
    if not os.path.exists(cacheDirectory):
        os.makedirs(cacheDirectory)
    filePath = cacheDirectory + "/atlas_fp_lightcurve.png"
    plt.savefig(filePath, bbox_inches='tight', transparent=False,
                pad_inches=0.1, optimize=True, progressive=True)

    try:
        cyanMag.remove()
    except:
        pass

    try:
        orangeMag.remove()
    except:
        pass

    firstPlot = False

    log.debug('completed the ``plot_single_result`` method')
    return filePath


def sigma_clip_data(
        log,
        fpData,
        clippingSigma=2.2):
    """*clean up rouge data from the files by performing some basic clipping*

    **Key Arguments:**

    - `fpData` -- data dictionary of force photometry
    - `clippingSigma` -- the level at which to clip flux data

    **Return:**

    - `epochs` -- sigma clipped and cleaned epoch data
    """
    log.info('starting the ``sigma_clip_data`` function')

    mjdMin = False
    mjdMax = False

    # PARSE DATA WITH SOME FIXED CLIPPING
    oepochs = []
    cepochs = []

    for row in fpData:
        # REMOVE VERY HIGH ERROR DATA POINTS
        if row["duJy"] > 4000:
            continue
        if mjdMin and mjdMax:
            if row["mjd_obs"] < mjdMin or row["mjd_obs"] > mjdMax:
                continue
        if row["filter"] == "c":
            cepochs.append(row)
        if row["filter"] == "o":
            oepochs.append(row)

    # SORT BY MJD
    cepochs = sorted(cepochs, key=itemgetter('mjd_obs'), reverse=False)
    oepochs = sorted(oepochs, key=itemgetter('mjd_obs'), reverse=False)

    # SIGMA-CLIP THE DATA WITH A ROLLING WINDOW
    cdataFlux = []
    cdataFlux[:] = [row["uJy"] for row in cepochs]
    odataFlux = []
    odataFlux[:] = [row["uJy"] for row in oepochs]

    maskList = []
    for flux in [cdataFlux, odataFlux]:
        fullMask = rolling_window_sigma_clip(
            log=log,
            array=flux,
            clippingSigma=clippingSigma,
            windowSize=7)
        maskList.append(fullMask)

    try:
        cepochs = [e for e, m in zip(
            cepochs, maskList[0]) if m == False]
    except:
        cepochs = []

    try:
        oepochs = [e for e, m in zip(
            oepochs, maskList[1]) if m == False]
    except:
        oepochs = []

    log.debug('completed the ``sigma_clip_data`` function')
    return cepochs + oepochs


def stack_photometry(
        log,
        magnitudes,
        binningDays=1.):
    """*stack the photometry for the given temporal range*

    **Key Arguments:**
        - `magnitudes` -- dictionary of photometry divided into filter sets
        - `binningDays` -- the binning to use (in days)

    **Return:**
        - `summedMagnitudes` -- the stacked photometry
    """
    log.debug('starting the ``stack_photometry`` method')

    # IF WE WANT TO 'STACK' THE PHOTOMETRY
    summedMagnitudes = {
        'c': {'mjds': [], 'mags': [], 'magErrs': [], 'n': []},
        'o': {'mjds': [], 'mags': [], 'magErrs': [], 'n': []},
        'I': {'mjds': [], 'mags': [], 'magErrs': [], 'n': []},
    }

    # MAGNITUDES/FLUXES ARE DIVIDED IN UNIQUE FILTER SETS - SO ITERATE OVER
    # FILTERS
    allData = []
    for fil, data in list(magnitudes.items()):
        # WE'RE GOING TO CREATE FURTHER SUBSETS FOR EACH UNQIUE MJD (FLOORED TO AN INTEGER)
        # MAG VARIABLE == FLUX (JUST TO CONFUSE YOU)
        distinctMjds = {}
        for mjd, flx, err in zip(data["mjds"], data["mags"], data["magErrs"]):
            # DICT KEY IS THE UNIQUE INTEGER MJD
            key = str(int(math.floor(mjd / float(binningDays))))
            # FIRST DATA POINT OF THE NIGHTS? CREATE NEW DATA SET
            if key not in distinctMjds:
                distinctMjds[key] = {
                    "mjds": [mjd],
                    "mags": [flx],
                    "magErrs": [err]
                }
            # OR NOT THE FIRST? APPEND TO ALREADY CREATED LIST
            else:
                distinctMjds[key]["mjds"].append(mjd)
                distinctMjds[key]["mags"].append(flx)
                distinctMjds[key]["magErrs"].append(err)

        # ALL DATA NOW IN MJD SUBSETS. SO FOR EACH SUBSET (I.E. INDIVIDUAL
        # NIGHTS) ...
        for k, v in list(distinctMjds.items()):
            # GIVE ME THE MEAN MJD
            meanMjd = old_div(sum(v["mjds"]), len(v["mjds"]))
            summedMagnitudes[fil]["mjds"].append(meanMjd)
            # GIVE ME THE MEAN FLUX
            meanFLux = old_div(sum(v["mags"]), len(v["mags"]))
            summedMagnitudes[fil]["mags"].append(meanFLux)
            # GIVE ME THE COMBINED ERROR
            combError = sum(v["magErrs"]) / len(v["magErrs"]
                                                ) / math.sqrt(len(v["magErrs"]))
            summedMagnitudes[fil]["magErrs"].append(combError)
            # GIVE ME NUMBER OF DATA POINTS COMBINED
            n = len(v["mjds"])
            summedMagnitudes[fil]["n"].append(n)
            allData.append({
                'MJD': f'{meanMjd:0.2f}',
                'uJy': f'{meanFLux:0.2f}',
                'duJy': f'{combError:0.2f}',
                'F': fil,
                'n': n
            })

    log.debug('completed the ``stack_photometry`` method')
    return summedMagnitudes


def get_twin(ax, axis):

    for sibling in siblings:
        if sibling.bbox.bounds == ax.bbox.bounds and sibling is not ax:
            return sibling
    return None


def get_twin_axis(ax, axis):
    assert axis in ("x", "y")
    for other_ax in ax.figure.axes:
        if other_ax is ax:
            siblings = getattr(ax, f"get_shared_{axis}_axes")().get_siblings(ax)
            for sibling in siblings:
                if sibling.bbox.bounds == ax.bbox.bounds and sibling is not ax:
                    return sibling
    return None
