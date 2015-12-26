import json
import logging
import os
#import sys
from emop.lib.emop_base import EmopBase
from emop.lib.emop_payload import EmopPayload
from emop.lib.models.emop_page import EmopPage
from emop.lib.transfer.globus import GlobusAPIClient

logger = logging.getLogger('emop')


class EmopTransfer(EmopBase):
    def __init__(self, config_path):
        super(self.__class__, self).__init__(config_path)
        self.globus = GlobusAPIClient(settings=self.settings)
        self.cluster_endpoint = self.settings.globus_cluster_endpoint
        self.remote_endpoint = self.settings.globus_remote_endpoint

    def stage_in_files(self, files, wait=0):
        """ Stage in files

        This function will start a Globus transfer of the specified files.

        Args:
            files (list): List of files to stage in
            wait (bool): Whether the stage in should wait for the transfer to complete

        Returns:
            str: Globus Task ID
        """
        stage_in_data = self._get_stage_in_data(files)
        src = self.remote_endpoint
        dest = self.cluster_endpoint
        label = "emop-stage-in-files"
        logger.debug("Stage in files:\n%s", json.dumps(stage_in_data, indent=4, sort_keys=True))

        task_id = self.start(src=src, dest=dest, data=stage_in_data, label=label, wait=wait)
        return task_id

    def stage_in_data(self, data, wait=0):
        """ Stage in data

        This function will extract necessary items from data and
        initiate a Globus transfer.

        Args:
            data (dict): Data that contains files to transfer
            wait (bool): Whether the stage in should wait for the transfer to complete

        Returns:
            str: Globus Task ID
        """
        files = self._get_stage_in_files_from_data(data)
        if not files:
            return ''
        task_id = self.stage_in_files(files=files, wait=wait)
        return task_id

    def stage_in_proc_ids(self, proc_ids, wait=0):
        """ Stage in proc_ids

        This function will find the necessary data from provided proc_ids and
        initiate a Globus transfer.

        Args:
            proc_ids (list): List of proc_ids to stage in
            wait (bool): Whether the stage in should wait for the transfer to complete

        Returns:
            str: Globus Task ID
        """
        stage_in_data = []
        src = self.remote_endpoint
        dest = self.cluster_endpoint
        label = "emop-stage-in-files"
        for proc_id in proc_ids:
            payload = EmopPayload(self.settings, proc_id)
            if not payload.input_exists():
                logger.error("EmopTransfer: Could not find input payload for proc_id %s", proc_id)
                continue
            data = payload.load_input()
            _files = self._get_stage_in_files_from_data(data)
            _stage_in_data = self._get_stage_in_data(_files)
            stage_in_data = stage_in_data + _stage_in_data

        task_id = self.start(src=src, dest=dest, data=stage_in_data, label=label, wait=wait)
        return task_id

    def stage_out_proc_id(self, proc_id, wait=0):
        """ Stage out proc_id

        This function will find the necessary data from the provided proc_id and
        initiate a Globus transfer.

        Args:
            proc_id (str): proc_id to stage out
            wait (bool): Whether the stage out should wait for the transfer to complete

        Returns:
            str: Globus Task ID
        """
        payload = EmopPayload(self.settings, proc_id)
        if payload.completed_output_exists():
            filename = payload.completed_output_filename
        elif payload.output_exists():
            filename = payload.output_filename
        elif payload.uploaded_output_exists():
            filename = payload.uploaded_output_filename
        else:
            logger.error("EmopTransfer: Could not find payload file for proc_id %s" % proc_id)
            return ''

        data = payload.load(filename)
        if not data:
            logger.error("EmopTransfer: Unable to load payload data")
            return ''
        stage_out_data = self._get_stage_out_data(data)
        src = self.cluster_endpoint
        dest = self.remote_endpoint
        label = "emop-stage-out-%s" % proc_id

        logger.debug("Stage out files:\n%s", json.dumps(stage_out_data, indent=4, sort_keys=True))

        task_id = self.start(src=src, dest=dest, data=stage_out_data, label=label, wait=wait)
        return task_id

    def check_endpoints(self, fail_on_warn=False):
        """ Check if endpoints are activated

        Check if cluster_endpoint and remote_endpoint are activated.
        Return True is activated and False otherwise.

        Args:
            fail_on_warn (bool): Consider endpoint activation about to expire as failure

        Returns:
            bool: Whether endpoints are activated and don't expire soon
        """
        _valid = True
        for endpoint in [self.cluster_endpoint, self.remote_endpoint]:
            _activated = self._check_activation(endpoint=endpoint, fail_on_warn=fail_on_warn)
            if not _activated:
                _valid = False
                logger.error("Endpoint %s is not activated!", endpoint)
                logger.error("To activate, visit this URL:\n\t%s", self.globus.get_activate_url(endpoint=endpoint))
            else:
                logger.info("Endpoint %s activated.", endpoint)

        return _valid

    def start(self, src, dest, data, label="emop", wait=0):
        """ Start a Globus transfer

        Start a globus transfer from src to dest of files contained in data.  The 
        data format is:
        [
            {'src': '/path/to/src/file', 'dest': '/path/to/dest/file'}
        ]

        Args:
            src (str): Source Globus endpoint name
            dest (str): Destination Globus endpoint name
            data (list): List of dictionaries defining files to transfer
            label (str): Label to give Globus transfer
            wait (int): Number of seconds to wait for transfer to complete, 0 means no wait.

        Returns:
            str: Globus Task ID
        """
        # TODO: Determine when to change sync_level
        _transfer = self.globus.create_transfer(src, dest, label=label, sync_level=2)
        if not data:
            logger.error("EmopTransfer.start: No data to transfer")
            return ''
        for d in data:
            logger.debug("TRANSFER: %s:%s -> %s:%s", src, d['src'], dest, d['dest'])
            _transfer.add_item(d['src'], d['dest'])
        task_id = self.globus.send_transfer(_transfer)
        if task_id:
            logger.info("Successfully submitted transfer with task ID %s", task_id)
        if not wait:
            return task_id
        status = self.globus.wait_for_task(task_id=task_id, timeout=wait)
        if status is None:
            logging.warn("Task did not complete before timeout!")
        else:
            logging.info("Task %s completed with status %s", task_id, status)
        return task_id

    def ls(self, endpoint, path):
        """ Globus ls

        Perform a Globus ls of endpoint's path.

        Args:
            endpoint (str): Globus endpoint name
            path (str): Path to ls

        Returns:
            dict: Globus ls data
        """
        data = self.globus.endpoint_ls(endpoint, path)
        return data

    def display_task(self, task_id, wait=0):
        """ Display task data

        Information about a Globus task will be printed.  This includes files, files skipped,
        files transferred, and task status.  The path of files successfully transferred will also
        be printed.

        Args:
            task_id (str): Globus Task ID
            wait (int): Number of seconds to wait for transfer to complete, 0 means no wait.

        Returns:
            str: Globus Task Status
        """
        task_data = self.globus.get_task(task_id)
        if not task_data:
            print "Unable to get task %s data" % task_id
            return None
        print "Task: %s" % task_id
        for k,v in task_data.iteritems():
            if k not in ["files", "files_skipped", "files_transferred", "status"]:
                continue
            print "\t%s=%s" % (k,v)
        if wait:
            status = self.globus.wait_for_task(task_id=task_id, timeout=wait)
        else:
            status = task_data["status"]
        successful_task_data = self.globus.get_successful_task(task_id)
        if not successful_task_data:
            print "Unable to get successful task %s data" % task_id
            return None
        transfer_list = successful_task_data["DATA"]
        if transfer_list:
            print "Successful Transfers (src -> dst)"
            for t in transfer_list:
                print "  %s:%s -> %s:%s" % (task_data['source_endpoint'], t[u'source_path'], task_data['destination_endpoint'], t[u'destination_path'])
        return status

    def _check_activation(self, endpoint, fail_on_warn=False):
        """ Private function - check endpoint activation

        An endpoint's activation will be checked and if not active, auto-activation
        is attempted.  The activation time is checked against globus/min_activation_time setting.

        Args:
            endpoint (str): Globus endpoint name
            fail_on_warn (bool): If endpoint activation time less than min_activation_time is a failure

        Returns:
            int: Number of seconds remaining for endpoint activation
        """
        _activated = True
        _expires_in = self.globus.check_activated(endpoint)
        if not _expires_in:
            _activated = False
        else:
            _expires_in_m, _expires_in_s = divmod(_expires_in, 60)
            _expires_in_h, _expires_in_m = divmod(_expires_in_m, 60)
            _expires_in_d, _expires_in_h = divmod(_expires_in_h, 24)
            logger.info("Endpoint %s expires in %d-%02d:%02d:%02d", endpoint, _expires_in_d, _expires_in_h, _expires_in_m, _expires_in_s)

        if _expires_in < self.settings.globus_min_activation_time:
            logger.warn("Endpoint %s expires before minimum activation time setting", endpoint)
            if fail_on_warn:
                _activated = False
        return _activated

    def _task_list(self, **kw):
        data = self.globus.task_list(**kw)
        return data

    def _get_stage_in_files_from_data(self, data):
        """ Private function - convert data to stage in files

        eMOP dashboard API data is converted to list of files to transfer.  The file_attributes
        that define what data to transfer are set in EmopPage.transfer_attributes class variable.

        Args:
            data (list): API data containing page information

        Returns:
            list: Files to tranfer
        """
        files = []
        page_keys = EmopPage.transfer_attributes
        for p in data:
            _page = p.get('page')
            if _page is None:
                continue
            for key in page_keys:
                _file = _page.get(key)
                if _file is not None:
                    files.append(_file)
        return files

    def _get_stage_in_data(self, files):
        """ Private function - convert stage in files to transfer data

        A list of files is turned into src/dest key/value pairs based on controller/input_path_prefix.

        Args:
            files (list): Stage in file list.

        Returns:
            list: List of dicts that contain necessary src/dest key/value pairs
        """
        _data = []
        for f in files:
            _paths = {}
            _paths['src'] = f
            _local_path = EmopBase.add_prefix(prefix=self.settings.input_path_prefix, path=f)
            _paths['dest'] = _local_path
            _data.append(_paths)
        return _data

    def _get_stage_out_data(self, data):
        """ Private function - convert stage out files to transfer data

        The output API data produced by controller is checked for absolute file paths in data and 
        then that data is converted to src/dest key/value pairs for transfer.  Currently only the 
        page_results are searched for data.

        Args:
            data (dict): Dictionary containing output API data.

        Returns:
            list: List of dicts that contain necessary src/dest key/value pairs
        """
        _data = []
        _page_results = data.get("page_results", [])
        for _result in _page_results:
            for _value in _result.values():
                if not isinstance(_value, basestring):
                    continue
                if os.path.isabs(_value):
                    _paths = {}
                    _paths['dest'] = _value
                    _local_path = EmopBase.add_prefix(prefix=self.settings.output_path_prefix, path=_value)
                    _paths['src'] = _local_path
                    _data.append(_paths)
        return _data

    # def _get_local_path(self, prefix, path):
    #     return EmopBase.add_prefix(prefix=prefix, path=path)
