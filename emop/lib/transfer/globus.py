import json
import os
#import sys
import time
#import datetime, dateutil.parser
import logging
#from pprint import pprint
from globusonline.transfer import api_client
#from globusonline.transfer.api_client.x509_proxy import create_proxy_from_file, implementation
from globusonline.transfer.api_client.goauth import get_access_token

logger = logging.getLogger('emop')


OK_STATUS_CODES = ['OK', 200, 202, 204]

# class GlobusTransfer(api_client.Transfer):
#     def __init__(self, api, submission_id, source_endpoint, destination_endpoint):
#         super(GlobusTransfer, self).__init__(submission_id, source_endpoint, destination_endpoint,
#                                                 notify_on_succeeded=False, notify_on_failed=False, notify_on_inactive=False)
#         self._api = api
#
#     def add_item(self, src, dest, recursive=False, verify_size=None):
#         super(GlobusTransfer, self).add_item(src, dest, recursive, verify_size)


class GlobusAPIClient(object):
    def __init__(self, settings):
        self.settings = settings
        username, goauth = self.get_goauth_data()
        self.username = username
        self.goauth = goauth
        # self.api = api_client.TransferAPIClient(username=self.username, cert_file=self.cert_file, key_file=self.key_file)
        self.api = api_client.TransferAPIClient(username=self.username, goauth=self.goauth)

    def get_goauth_data(self):
        """ Get GOAuth data

        The file at $EMOP_HOME/.globus-auth (settings.globus_auth_file) is read, if it exists.  If the .globus-auth
        file does not exist, the data is pulled from Globus Online by prompting user for username (if not provided in config.ini)
        and password.  This function will save the goauth token to .globus-auth if it did not already exist.

        Args:

        Returns:
            tuple: username and goauth token
        """
        data = None
        if os.path.isfile(self.settings.globus_auth_file):
            with open(self.settings.globus_auth_file) as datafile:
                data = datafile.read().strip()
            token = data
            parsed_token = self._parse_goauth_token(token)
            username = parsed_token['un']
        else:
            api_token = get_access_token(username=self.settings.globus_username)
            username = api_token.username
            token = api_token.token
            parsed_token = self._parse_goauth_token(token)
            with os.fdopen(os.open(self.settings.globus_auth_file, os.O_WRONLY | os.O_CREAT, 0600), 'w') as datafile:
                datafile.write(token)

        self.settings.globus_goauth_token = token
        self.settings.globus_goauth_parsed_token = parsed_token
        self.settings.globus_username = username
        return (username, token)

    def get_endpoint_data(self, endpoint, **kwargs):
        """ Globus API - endpoint

        Args:
            endpoint (str): Globus Endpoint

        Returns:
            dict: Globus endpoint data
        """
        descr = "endpoint %s" % endpoint
        data = self._api_call(descr=descr, func='endpoint', endpoint_name=endpoint, **kwargs)
        return data

    def check_activated(self, endpoint):
        """ Globus API - check activation and autoactivate

        Args:
            endpoint (str): Globus Endpoint

        Returns:
            int: Seconds left in endpoint activation
        """
        endpoint_data = self.get_endpoint_data(endpoint=endpoint, fields="activated,expires_in")
        if not endpoint_data:
            return 0
        _endpoint_activated = endpoint_data["activated"]

        if _endpoint_activated:
            _expires_in = int(endpoint_data["expires_in"])
        else:
            logger.info("Endpoint %s not activated, attempting autoactivate" % endpoint)
            _autoactivate_data = self.autoactivate(endpoint=endpoint)
            _expires_in = int(_autoactivate_data["expires_in"])

        if not _expires_in:
            return 0
        else:
            return _expires_in

    def autoactivate(self, endpoint):
        """ Globus API - autoactivate

        Args:
            endpoint (str): Globus Endpoint

        Returns:
            dict: Globus autoactivate data
        """
        descr = "Autoactivate %s" % endpoint
        data = self._api_call(descr=descr, func='endpoint_autoactivate', endpoint_name=endpoint)
        return data

    def get_activate_url(self, endpoint):
        """ Globus API - endpoint acivation URL

        Args:
            endpoint (str): Globus Endpoint

        Returns:
            str: Globus endpoint activation URL
        """
        data = self.get_endpoint_data(endpoint, fields='canonical_name,id')
        if not data:
            return 'UNKNOWN'
        _id = data.get('id')
        _name = data.get('canonical_name')
        _url = "https://www.globus.org/activate" + api_client.encode_qs(ep=_name, ep_ids=_id)
        return _url

    # def activate_endpoint(self):
    #     _requirements_data = {}
    #     _activate_data = {}
    #     status_code, status_message, requirements_data = self.api.endpoint_activation_requirements(self.endpoint)
    #
    #     if status_message == "OK":
    #         _requirements_data = json.loads(requirements_data.as_json())
    #         logger.debug("Requirements data: %s", json.dumps(_requirements_data, indent=4, sort_keys=True))
    #     else:
    #         logger.error("FAILED: Getting endpoint activation requirements")
    #         logger.error("Code: %s, Message: %s", status_code, status_message)
    #         return None
    #
    #     _public_key = requirements_data.get_requirement_value(type="delegate_proxy", name="public_key")
    #     _proxy = create_proxy_from_file(issuer_cred_file=self.cred_file, public_key=_public_key, lifetime_hours=24)
    #     requirements_data.set_requirement_value(type="delegate_proxy", name="proxy_chain", value=_proxy)
    #     logger.debug("Requirements data modified: %s", json.dumps(json.loads(requirements_data.as_json()), indent=4, sort_keys=True))
    #
    #     status_code, status_message, activate_data = self.api.endpoint_activate(endpoint_name=self.endpoint, filled_requirements=requirements_data, if_expires_in=60*60)
    #
    #     if status_message == "OK":
    #         _activate_data = json.loads(activate_data.as_json())
    #         logger.debug("Activation data: %s", json.dumps(_activate_data, indent=4, sort_keys=True))
    #     else:
    #         logger.error("FAILED: Activating endpoint")
    #         logger.error("Code: %s, Message: %s", status_code, status_message)
    #     return _activate_data

    def _get_submission_id(self):
        """ Globus API - submission_id

        Args:

        Returns:
            str: Globus submission ID
        """
        descr = "Get submission_id"
        data = self._api_call(descr=descr, func='submission_id')
        return data.get("value")

    def create_transfer(self, src, dest, **kw):
        """ Globus API - create transfer

        The Globus submission ID is retrieved and a Transfer object created

        Args:
            src (str): Globus source transfer endpoint
            dest (str): Globus destination transfer endpoint

        Returns:
            Transfer: Globus Transfer object
        """
        _submission_id = self._get_submission_id()
        if not _submission_id:
            logger.error("Globus: Unable to obtain Globus transfer submission ID")
            return None
        _transfer = api_client.Transfer(_submission_id, src, dest,
                                        notify_on_succeeded=False, notify_on_failed=False, notify_on_inactive=False, **kw)
        self.transfer = _transfer
        return _transfer

    def send_transfer(self, transfer):
        """ Globus API - transfer

        Args:
            transfer (Transfer): Globus Transfer object

        Returns:
            str: Globus transfer Task ID
        """
        descr = "Transfer"
        data = self._api_call(descr=descr, func='transfer', transfer=transfer)
        return data.get("task_id")

    def get_task(self, task_id, **kwargs):
        """ Globus API - task

        Args:
            task_id (str): Globus Task ID

        Returns:
            dict: Globus Task data
        """
        descr = "Get task %s" % task_id
        data = self._api_call(descr=descr, func='task', task_id=task_id, **kwargs)
        return data

    def get_successful_task(self, task_id, **kwargs):
        """ Globus API - task_successful_transfers

        Args:
            task_id (str): Globus task ID

        Returns:
            dict: Globus task successful transfer data
        """
        descr = "Get successful task %s" % task_id
        data = self._api_call(descr=descr, func='task_successful_transfers', task_id=task_id, **kwargs)
        return data

    def _get_task_status(self, task_id):
        """ Globus API - status

        Args:
            task_id (str): Globus task ID

        Returns:
            str: Globus task status
        """
        data = self.get_task(task_id=task_id, fields="status")
        return data.get("status")

    def wait_for_task(self, task_id, timeout=120, poll_interval=10):
        """ Globus API - wait for task

        A task status is queried and this function will wait for the status to be SUCCEEDED or
        FAILED up to timeout value.

        Args:
            task_id (str): Globus task ID to wait for
            timeout (int): How long to wait for the task before timing out
            poll_interval (int): How often to query the task status

        Returns:
            str: Globus task status - None is returned if wait times out
        """
        _timeout_left = timeout
        while _timeout_left >= 0:
            _status = self._get_task_status(task_id)
            if _status in ("SUCCEEDED", "FAILED"):
                return _status
            if _timeout_left > 0:
                time.sleep(poll_interval)
            _timeout_left -= poll_interval
        return None

    def task_list(self):
        """ Globus API - task_list

        Args:

        Returns:
            dict: Globus task_list data
        """
        descr = "Get task list"
        data = self._api_call(descr=descr, func='task_list')
        return data

    def endpoint_ls(self, endpoint, path):
        """ Globus API - endpoint_ls

        Args:
            endpoint (str): Globus Endpoint
            path (str): Path to ls

        Returns:
            dict: Globus endpoint_ls data
        """
        descr = "ls %s:%s" % (endpoint, path)
        data = self._api_call(descr=descr, func='endpoint_ls', endpoint_name=endpoint, path=path)
        return data.get("DATA")

    def _parse_goauth_token(self, token):
        """ Globus API - parse GOAuth token

        A GOAuth token is converted into a dict.  "un=test|token=foo" becomes
        {
            'un': 'test',
            'token': 'foo',
        }

        Args:
            token (str): GOAuth token

        Returns:
            dict: Parsed GOAuth token
        """
        data = {}
        for d in token.split('|'):
            v = d.split('=')
            data[v[0]] = v[1]
        return data

    def _api_call(self, *args, **kwargs):
        """ Globus API - wrapper for all API calls

        This function is a shortcut wrapper to all the Globus API calls.

        Args:
            descr (str): Descrtiption of API call - used in log output
            func (str): Globus Python API function to call
            **kwargs: Remaining arguments to pass to API library function

        Returns:
            dict: Globus API function return data
        """
        descr = kwargs.pop("descr")
        func = kwargs.pop("func")
        _func = getattr(self.api, func)
        try:
            code, reason, data = _func(*args, **kwargs)
        except Exception as e:
            code = "Exception"
            reason = str(e)
            data = None
        if not data:
            data = {}
            _data = {}
        elif type(data).__name__ == 'ActivationRequirementList':
            _data = json.loads(data.as_json())
        else:
            _data = data
        if code in OK_STATUS_CODES:
            logger.debug("%s DATA:\n%s", descr, json.dumps(_data, indent=4, sort_keys=True))
        else:
            logger.error("FAILED: %s", descr)
            logger.error("Code: %s, Message: %s", code, reason)
        return data
