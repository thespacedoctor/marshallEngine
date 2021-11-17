#!/usr/bin/env python
# encoding: utf-8
"""
*request, update and read status of observation blocks with the SOXS Scheduler  *

:Author:
    David Young

:Date Created:
    November 17, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
import requests
import json

# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


class soxs_scheduler(object):
    """
    *The worker class for the soxs_scheduler module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a soxs_scheduler object, use the following:

    ```eval_rst
    .. todo::

        - add usage info
        - create a sublime snippet for usage
        - create cl-util for this class
        - add a tutorial about ``soxs_scheduler`` to documentation
        - create a blog post about what ``soxs_scheduler`` does
    ```

    ```python
    usage code 
    ```

    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'soxs_scheduler' object")
        self.settings = settings
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        self.baseurl = self.settings["scheduler_baseurl"]

        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the soxs_scheduler object*

        **Return:**
            - ``soxs_scheduler``

        **Usage:**

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - create cl-util for this method
            - update the package tutorial if needed
        ```

        ```python
        usage code 
        ```
        """
        self.log.debug('starting the ``get`` method')

        soxs_scheduler = None

        self.log.debug('completed the ``get`` method')
        return soxs_scheduler

    def create_single_auto_ob(
            self,
            transientBucketID,
            target_name,
            raDeg,
            decDeg,
            magnitude_list,
            existenceCheck=True):
        """*request to generate a single auto ob from the soxs scheduler*

        **Key Arguments:**
            - ``transientBucketID`` -- the transients ID from the marshall.
            - ``target_name`` -- the master name of the target.
            - ``raDeg`` -- the target RA.
            - ``decDeg`` -- the target declination.
            - ``magnitude_list`` -- the list of lists of magnitudes. [['g':19.06],['r':19.39]]
            - ``existenceCheck`` -- check local database to see if OB already exists for this transient. Default *True*.

        **Return:**
            - None

        **Usage:**

        ```python
        usage code 
        ```

        ---

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - write a command-line tool for this method
            - update package tutorial with command-line tool info if needed
        ```
        """
        self.log.debug('starting the ``create_single_auto_ob`` method')

        if existenceCheck:
            # CHECK FOR EXISTENCE OF OB IN LOCAL DATABASE
            from fundamentals.mysql import readquery
            sqlQuery = f"""
                
            """
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

        try:
            schd_status_code = 0
            http_status_code = 500
            response = requests.post(
                url="https://soxs-scheduler-pwoxq.ondigitalocean.app/createAutoOB",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                },
                data=json.dumps({
                    "magnitude_list": magnitude_list,
                    "declination": float(decDeg),
                    "target_name": target_name,
                    "transientBucketID": transientBucketID,
                    "right_ascension": float(raDeg)
                })
            )

            response = response.json()
            schd_status_code = response["status"]
            content = response["data"]
            http_status_code = content["status_code"]

        except requests.exceptions.RequestException:
            self.log.error(
                'HTTP Request failed to scheduler `createAutoOB` resource failed')
        if http_status_code != 201 or schd_status_code != 1:
            error = content["payload"]
            print(f"createAutoOB failed with error: '{error}'")
            return -1

        obid = content["payload"][0]['OB_ID']

        self.log.debug('completed the ``create_single_auto_ob`` method')
        return obid

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
