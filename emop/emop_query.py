import glob
import logging
import os
import re
from decimal import Decimal
from emop.lib.emop_base import EmopBase

logger = logging.getLogger('emop')

processes = [
    "OCR",
    "Denoise",
    "MultiColumnSkew",
    "XML_To_Text",
    "PageEvaluator",
    "PageCorrector",
    "JuxtaCompare",
    # "RetasCompare",
]


class EmopQuery(EmopBase):

    def __init__(self, config_path):
        super(self.__class__, self).__init__(config_path)

    def pending_pages_count(self, q_filter):
        """ Query number of pending pages

        The q_filter would be in form of '{"batch_id": 6}', for example.

        Args:
            q_filter (dict): Query filter passed to EmopDashboard API

        Returns:
            int: Number of pending pages
        """
        job_status_id = self._get_job_status_id(name='Not Started')
        if not job_status_id:
            return None
        if q_filter and isinstance(q_filter, dict):
            job_queue_params = q_filter.copy()
        else:
            job_queue_params = {}
        job_queue_params["job_status_id"] = str(job_status_id)
        job_queue_request = self.emop_api.get_request("/api/job_queues/count", job_queue_params)
        if not job_queue_request:
            return None
        job_queue_results = job_queue_request.get('job_queue')
        count = job_queue_results.get('count')
        return count

    def pending_pages(self, q_filter, r_filter=None):
        """ Query pending pages

        The q_filter would be in form of '{"batch_id": 6}', for example.

        The r_filter would be in form of 'page.pg_image_path,pg_ground_truth_file' where each period
        denotes how far in the returned data to filter.  So the key page containing the key
        pg_image path would be returned.

        Currently r_filter only supports 2 levels deep.

        Args:
            q_filter (dict): Query filter passed to EmopDashboard API
            r_filter (str): Results filter used to filter returned results.

        Returns:
            list: List of pending pages.  Each element is a dict.
        """
        job_status_id = self._get_job_status_id(name='Not Started')
        if not job_status_id:
            return None
        if q_filter and isinstance(q_filter, dict):
            job_queue_params = q_filter.copy()
        else:
            job_queue_params = {}
        job_queue_params["job_status_id"] = str(job_status_id)
        job_queue_request = self.emop_api.get_request("/api/job_queues", job_queue_params)
        if not job_queue_request:
            return None
        job_queue_results = job_queue_request.get('results')
        return job_queue_results
        # if not r_filter:
        #     return job_queue_results
        # _filters = r_filter.split(':')
        # print "_filters: %s" % _filters
        # _pending_pages = []
        # for r in job_queue_results:
        #     for key1, val1 in r.iteritems():
        #         for _f in _filters:
        #             _filter = _f.split('.')
        #             print "_filter: %s" % _filter
        #             if _filter[0] != key1:
        #                 #print "Filter[0]: %s != key1:%s" % (_filter[0], key1)
        #                 continue
        #             _data = {}
        #             if len(_filter) == 1:
        #                 _data[key1] = val1
        #             else:
        #                 _filter_keys = _filter[1].split(',')
        #                 for key2 in _filter_keys:
        #                     if key1 not in _data:
        #                         _data[key1] = {}
        #                     _data[key1][key2] = val1.get(key2)
        #             if _data:
        #                 _pending_pages.append(_data)
        # return _pending_pages

    def get_runtimes(self):
        results = {}
        results["processes"] = []
        runtimes = {}
        runtimes["pages"] = []
        runtimes["total"] = []
        for process in processes:
            runtimes[process] = []

        glob_path = os.path.join(self.settings.scheduler_logdir, "*.out")
        files = glob.glob(glob_path)

        for f in files:
            file_runtimes = self._parse_file_for_runtimes(f)
            runtimes["pages"] = runtimes["pages"] + file_runtimes["pages"]
            runtimes["total"] = runtimes["total"] + file_runtimes["total"]
            for process in processes:
                runtimes[process] = runtimes[process] + file_runtimes["processes"][process]

        total_pages = len(runtimes["pages"])
        total_jobs = len(runtimes["total"])

        if total_pages > 0:
            total_page_runtime = sum(runtimes["pages"])
            average_page_runtime = total_page_runtime / total_pages
        else:
            total_page_runtime = sum(runtimes["pages"])
            average_page_runtime = 0

        if total_jobs > 0:
            total_job_runtime = sum(runtimes["total"])
            average_job_runtime = total_job_runtime / total_jobs
        else:
            total_job_runtime = sum(runtimes["total"])
            average_job_runtime = 0

        for process in processes:
            process_runtimes = runtimes[process]
            cnt = len(process_runtimes)
            if cnt > 0:
                total = sum(process_runtimes)
                avg = total / cnt
            else:
                total = sum(process_runtimes)
                avg = 0
            process_results = {"name": process, "count": cnt, "total": round(total, 3), "avg": round(avg, 3)}
            results["processes"].append(process_results.copy())

        results["total_pages"] = total_pages
        results["total_page_runtime"] = round(total_page_runtime, 3)
        results["average_page_runtime"] = round(average_page_runtime, 3)
        results["total_jobs"] = total_jobs
        results["average_job_runtime"] = round(average_job_runtime, 3)
        return results

    def _get_job_status_id(self, name='Not Started'):
        job_status_params = {
            'name': name,
        }
        job_status_request = self.emop_api.get_request("/api/job_statuses", job_status_params)
        if not job_status_request:
            return None
        job_status_results = job_status_request.get('results')[0]
        job_status_id = job_status_results.get('id')
        return job_status_id

    def _parse_file_for_runtimes(self, filename):
        runtimes = {}
        runtimes["pages"] = []
        runtimes["total"] = []
        runtimes["processes"] = {}
        for process in processes:
            runtimes["processes"][process] = []

        with open(filename) as f:
            lines = f.readlines()

        for line in lines:
            page_match = re.search("Job \[.*\] COMPLETE: Duration: ([0-9.]+) secs", line)
            total_match = re.search("TOTAL TIME: ([0-9.]+)$", line)

            if page_match:
                page_runtime = page_match.group(1)
                runtimes["pages"].append(Decimal(page_runtime))
            elif total_match:
                total_runtime = total_match.group(1)
                runtimes["total"].append(Decimal(total_runtime))
            else:
                for process in processes:
                    process_match = re.search("%s \[.*\] COMPLETE: Duration: ([0-9.]+) secs" % process, line)
                    if process_match:
                        process_runtime = process_match.group(1)
                        runtimes["processes"][process].append(Decimal(process_runtime))
        return runtimes
