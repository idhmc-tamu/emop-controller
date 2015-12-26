import pytest
from unittest import TestCase
from unittest import TestLoader
#from flexmock import flexmock
import os
from mock import MagicMock#, Mock
from tests.utilities import default_config_path, fixture_file, load_fixture_file
from emop.emop_query import EmopQuery

xfail = pytest.mark.xfail

class TestEmopQuery(TestCase):
    def setUp(self):
        self.query = EmopQuery(config_path=default_config_path())

    def tearDown(self):
        pass

    def test_get_job_status_id(self):
        mock_response = {
            "total": 1,
            "subtotal": 1,
            "page": 1,
            "per_page": 1,
            "total_pages": 1,
            "results": [
                {
                    "id": 1,
                    "name": "Not Started"
                },
            ]
        }
        self.query.emop_api.get_request = MagicMock()
        self.query.emop_api.get_request.return_value = mock_response
        retval = self.query._get_job_status_id()
        self.assertEqual(1, retval)

    def test_pending_pages_count(self):
        mock_response = {
            "job_queue": {
                "count": 2
            }
        }
        self.query._get_job_status_id = MagicMock()
        self.query._get_job_status_id.return_value = 1
        self.query.emop_api.get_request = MagicMock()
        self.query.emop_api.get_request.return_value = mock_response
        retval = self.query.pending_pages_count(q_filter='{"batch_id": 1}')
        self.assertEqual(2, retval)

    def test_pending_pages_1(self):
        mock_response = load_fixture_file('job_queues_1.json')
        self.query._get_job_status_id = MagicMock()
        self.query._get_job_status_id.return_value = 1
        self.query.emop_api.get_request = MagicMock()
        self.query.emop_api.get_request.return_value = mock_response
        retval = self.query.pending_pages(q_filter='{"batch_id": 1}')
        self.assertEqual(mock_response['results'], retval)

    @xfail
    def test_pending_pages_2(self):
        mock_response = load_fixture_file('job_queues_1.json')
        expected = [
            {
                'page': {
                    "pg_ground_truth_file": "/data/shared/text-xml/EEBO-TCP-pages-text/e0006/40099/1.txt",
                    'pg_image_path': '/data/eebo/e0006/40099/00001.000.001.tif',
                },
            },
            {
                'page': {
                    "pg_ground_truth_file": "/data/shared/text-xml/EEBO-TCP-pages-text/e0006/40099/2.txt",
                    'pg_image_path': '/data/eebo/e0006/40099/00002.000.001.tif',
                },
            },
        ]
        self.query._get_job_status_id = MagicMock()
        self.query._get_job_status_id.return_value = 1
        self.query.emop_api.get_request = MagicMock()
        self.query.emop_api.get_request.return_value = mock_response
        retval = self.query.pending_pages(q_filter='{"batch_id": 1}', r_filter='page.pg_image_path,pg_ground_truth_file')
        self.maxDiff = None
        self.assertEqual(expected, retval)

    def test_get_runtimes_1(self):
        expected = {
            'total_pages': 10,
            'total_page_runtime': 630.943,
            'average_page_runtime': 63.094,
            'total_jobs': 1,
            'average_job_runtime': 631.018,
            'processes': [
                {'name': "OCR", 'count': 10, 'total': 69.422,'avg': 6.942},
                {'name': "Denoise", 'count': 10, 'total': 69.579,'avg': 6.958},
                {'name': "MultiColumnSkew", 'count': 10, 'total': 69.331,'avg': 6.933},
                {'name': "XML_To_Text", 'count': 10, 'total': 0.205,'avg': 0.021},
                {'name': "PageEvaluator", 'count': 10, 'total': 9.852,'avg': 0.985},
                {'name': "PageCorrector", 'count': 10, 'total': 402.345,'avg': 40.234},
                {'name': "JuxtaCompare", 'count': 10, 'total': 10.118,'avg': 1.012},
            ],
        }
        self.query.settings.scheduler_logdir = os.path.dirname(fixture_file('log-1.out'))
        retval = self.query.get_runtimes()
        self.maxDiff = None
        self.assertEqual(expected, retval)


def suite():
    return TestLoader().loadTestsFromTestCase(TestEmopQuery)
