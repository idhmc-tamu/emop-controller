import pytest
from unittest import TestCase
from unittest import TestLoader
#from flexmock import flexmock
import os
from collections import namedtuple
from mock import MagicMock, Mock, patch
from tests.utilities import default_settings, fixture_file, load_fixture_file
from globusonline.transfer import api_client
from emop.lib.transfer.globus import GlobusAPIClient

skipif = pytest.mark.skipif

class TestGlobus(TestCase):
    @pytest.fixture(autouse=True)
    def setup(self, tmpdir):
        settings = default_settings()
        self.fake_goauth_token = 'un=test|tokenid=fake-token-id'
        settings = default_settings()
        with open(settings.globus_auth_file, 'w') as auth_file:
            auth_file.write(self.fake_goauth_token)
        self.globus = GlobusAPIClient(settings=settings)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch("emop.lib.transfer.globus.get_access_token")
    @patch("emop.lib.transfer.globus.os.path.isfile")
    def test_get_goauth_data_fromfile(self, mock_goauth_isfile, mock_get_access_token):
        mock_goauth_isfile.return_value = True
        expected_retval = ('test', self.fake_goauth_token)
        retval = self.globus.get_goauth_data()
        assert not mock_get_access_token.called
        self.assertEqual(expected_retval, retval)

    @patch("emop.lib.transfer.globus.get_access_token")
    @patch("emop.lib.transfer.globus.os.path.isfile")
    def test_get_goauth_data_from_api(self, mock_goauth_isfile, mock_get_access_token):
        mock_goauth_isfile.return_value = False
        GOAuthResult = namedtuple("GOAuthResult", "username password token")
        mock_get_access_token.return_value = GOAuthResult('test', 'foo', self.fake_goauth_token)
        expected_retval = ('test', self.fake_goauth_token)
        retval = self.globus.get_goauth_data()
        mock_get_access_token.assert_called_once_with(username='test')
        self.assertEqual(expected_retval, retval)

    def test_get_endpoint_data(self):
        data = load_fixture_file('globus_endpoint_data.json')
        self.globus._api_call = MagicMock(return_value=data)
        retval = self.globus.get_endpoint_data(endpoint='go#ep1')
        self.globus._api_call.assert_called_once_with(descr='endpoint go#ep1', func='endpoint', endpoint_name='go#ep1')
        self.assertEqual(data, retval)

    def test_check_activated_already_activated(self):
        endpoint_data = {
            "activated": True,
            "expires_in": 788825
        }
        self.globus.get_endpoint_data = MagicMock(return_value=endpoint_data)
        self.globus.autoactivate = MagicMock()
        retval = self.globus.check_activated(endpoint='go#ep1')
        self.globus.get_endpoint_data.assert_called_once_with(endpoint='go#ep1', fields="activated,expires_in")
        assert not self.globus.autoactivate.called
        self.assertEqual(788825, retval)

    def test_check_activated_autoactivate(self):
        endpoint_data = {
            "activated": False,
            "expires_in": 0
        }
        autoactivate_data = {
            "code": "AutoActivated.CachedCredential",
            "resource": "/endpoint/go#ep1/activate",
            "DATA_TYPE": "activation_result",
            "expires_in": 788825,
            "length": 0,
            "endpoint": "go#ep1",
            "request_id": 'test',
            "expire_time": 'test',
            "message": "Endpoint activated successfully using cached credential",
            "DATA": [],
            "oauth_server": None,
            "subject": None
        }
        self.globus.get_endpoint_data = MagicMock(return_value=endpoint_data)
        self.globus.autoactivate = MagicMock(return_value=autoactivate_data)
        retval = self.globus.check_activated(endpoint='go#ep1')
        self.globus.get_endpoint_data.assert_called_once_with(endpoint='go#ep1', fields="activated,expires_in")
        self.globus.autoactivate.assert_called_once_with(endpoint='go#ep1')
        self.assertEqual(788825, retval)

    def test_check_activated_not_activated(self):
        endpoint_data = {
            "activated": False,
            "expires_in": 0
        }
        autoactivate_data = {
            "code": "AutoActivationFailed",
            "resource": "/endpoint/go#ep1/activate",
            "DATA_TYPE": "activation_result",
            "expires_in": 0,
            "length": 0,
            "endpoint": "go#ep1",
            "request_id": None,
            "expire_time": None,
            "message": "Auto activation failed",
            "DATA": [],
            "oauth_server": None,
            "subject": None
        }
        self.globus.get_endpoint_data = MagicMock(return_value=endpoint_data)
        self.globus.autoactivate = MagicMock(return_value=autoactivate_data)
        retval = self.globus.check_activated(endpoint='go#ep1')
        self.globus.get_endpoint_data.assert_called_once_with(endpoint='go#ep1', fields="activated,expires_in")
        self.globus.autoactivate.assert_called_once_with(endpoint='go#ep1')
        self.assertEqual(0, retval)

    def test_check_activated_no_endpoint_data(self):
        self.globus.get_endpoint_data = MagicMock(return_value={})
        self.globus.autoactivate = MagicMock()
        retval = self.globus.check_activated(endpoint='go#ep1')
        self.globus.get_endpoint_data.assert_called_once_with(endpoint='go#ep1', fields="activated,expires_in")
        assert not self.globus.autoactivate.called
        self.assertEqual(0, retval)

    def test_autoactivate(self):
        autoactivate_data = {
            "code": "AutoActivated.CachedCredential",
            "resource": "/endpoint/go#ep1/activate",
            "DATA_TYPE": "activation_result",
            "expires_in": 788825,
            "length": 0,
            "endpoint": "go#ep1",
            "request_id": 'test',
            "expire_time": 'test',
            "message": "Endpoint activated successfully using cached credential",
            "DATA": [],
            "oauth_server": None,
            "subject": None
        }
        self.globus._api_call = MagicMock(return_value=autoactivate_data)
        retval = self.globus.autoactivate(endpoint="go#ep1")
        self.globus._api_call.assert_called_once_with(descr="Autoactivate go#ep1", func="endpoint_autoactivate", endpoint_name="go#ep1")
        self.assertEqual(autoactivate_data, retval)

    def test_get_activate_url_success(self):
        data = {
            "canonical_name": "go#ep1",
            "id": "ddb59aef-6d04-11e5-ba46-22000b92c6ec",
        }
        self.globus.get_endpoint_data = MagicMock(return_value=data)
        retval = self.globus.get_activate_url(endpoint="go#ep1")
        self.assertEqual("https://www.globus.org/activate?ep=go%23ep1&ep_ids=ddb59aef-6d04-11e5-ba46-22000b92c6ec", retval)

    def test_get_activate_url_fail(self):
        self.globus.get_endpoint_data = MagicMock(return_value={})
        retval = self.globus.get_activate_url(endpoint="go#ep1")
        self.assertEqual("UNKNOWN", retval)

    def test__get_submission_id(self):
        data = {
            "value": "2978e1ce-99d7-11e5-9996-22000b96db58",
            "DATA_TYPE": "submission_id"
        }
        self.globus._api_call = MagicMock(return_value=data)
        retval = self.globus._get_submission_id()
        self.globus._api_call.assert_called_once_with(descr="Get submission_id", func="submission_id")
        self.assertEqual("2978e1ce-99d7-11e5-9996-22000b96db58", retval)

    def test_create_transfer(self):
        with patch('globusonline.transfer.api_client.Transfer') as transfer_class:
            mock_transfer = MagicMock()
            transfer_class.return_value = mock_transfer
            self.globus._get_submission_id = MagicMock(return_value="2978e1ce-99d7-11e5-9996-22000b96db58")
            retval = self.globus.create_transfer(src="go#ep1", dest="go#ep2")
            transfer_class.assert_called_once_with("2978e1ce-99d7-11e5-9996-22000b96db58", "go#ep1", "go#ep2", notify_on_succeeded=False, notify_on_failed=False, notify_on_inactive=False)
            self.assertEqual(mock_transfer, retval)

    def test_create_transfer_fail(self):
        with patch('globusonline.transfer.api_client.Transfer') as transfer_class:
            mock_transfer = MagicMock()
            transfer_class.return_value = mock_transfer
            self.globus._get_submission_id = MagicMock(return_value=None)
            retval = self.globus.create_transfer(src="go#ep1", dest="go#ep2")
            assert not transfer_class.called
            self.assertEqual(None, retval)

    def test_send_transfer(self):
        data = {
            "task_id": "d237692e-99f1-11e5-9996-22000b96db58",
        }
        mock_transfer = MagicMock()
        self.globus._api_call = MagicMock(return_value=data)
        retval = self.globus.send_transfer(transfer=mock_transfer)
        self.globus._api_call.assert_called_once_with(descr="Transfer", func="transfer", transfer=mock_transfer)
        self.assertEqual("d237692e-99f1-11e5-9996-22000b96db58", retval)

    def test_get_task(self):
        data = {
            "status": "ACTIVE",
            "task_id": "d237692e-99f1-11e5-9996-22000b96db58",
        }
        self.globus._api_call = MagicMock(return_value=data)
        retval = self.globus.get_task(task_id="d237692e-99f1-11e5-9996-22000b96db58", fields="status,task_id")
        self.globus._api_call.assert_called_once_with(descr="Get task d237692e-99f1-11e5-9996-22000b96db58", func="task", task_id="d237692e-99f1-11e5-9996-22000b96db58", fields="status,task_id")
        self.assertEqual(data, retval)

    def test_get_successful_task(self):
        data = {
            "DATA": [
                {
                    "DATA_TYPE": "successful_transfer",
                    "destination_path": "/~/test-out.txt",
                    "source_path": "/~/test-in.txt"
                }
            ],
            "DATA_TYPE": "successful_transfers",
        }
        self.globus._api_call = MagicMock(return_value=data)
        retval = self.globus.get_successful_task(task_id="d237692e-99f1-11e5-9996-22000b96db58")
        self.globus._api_call.assert_called_once_with(descr="Get successful task d237692e-99f1-11e5-9996-22000b96db58", func="task_successful_transfers", task_id="d237692e-99f1-11e5-9996-22000b96db58")
        self.assertEqual(data, retval)

    def test__get_task_status(self):
        data = {
            "status": "ACTIVE",
        }
        mock_transfer = MagicMock()
        self.globus.get_task = MagicMock(return_value=data)
        retval = self.globus._get_task_status(task_id="d237692e-99f1-11e5-9996-22000b96db58")
        self.globus.get_task.assert_called_once_with(task_id="d237692e-99f1-11e5-9996-22000b96db58", fields="status")
        self.assertEqual("ACTIVE", retval)

    @skipif(True, reason="Not yet implemented")
    def test_wait_for_task(self):
        pass

    @skipif(True, reason="Not yet implemented")
    def test_task_list(self):
        pass

    @skipif(True, reason="Not yet implemented")
    def test_endpoint_ls(self):
        pass

def suite():
    return TestLoader().loadTestsFromTestCase(TestGlobus)
