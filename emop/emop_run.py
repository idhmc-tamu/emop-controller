import json
import logging
import signal
import sys
from emop.lib.emop_base import EmopBase
from emop.lib.emop_payload import EmopPayload
from emop.lib.emop_job import EmopJob
from emop.lib.emop_scheduler import EmopScheduler
from emop.lib.processes.tesseract import Tesseract
from emop.lib.processes.xml_to_text import XML_To_Text
from emop.lib.processes.denoise import Denoise
from emop.lib.processes.multi_column_skew import MultiColumnSkew
from emop.lib.processes.page_evaluator import PageEvaluator
from emop.lib.processes.page_corrector import PageCorrector
from emop.lib.processes.juxta_compare import JuxtaCompare
from emop.lib.processes.retas_compare import RetasCompare

logger = logging.getLogger('emop')
job_ids = []
instance = None


def signal_exit(signum, frame):
    """Signal handler

    This function will mark all non-completed jobs as failed
    and exit.  This is intended to catch SIGUSR1 signals that indicate
    a job is nearing its time limit.
    """
    for job_id in job_ids:
        if job_id not in instance.jobs_completed:
            results = "%s JOB %s: time limit reached" % (instance.scheduler.name, instance.scheduler.job_id)
            logger.error(results)
            instance.jobs_failed.append({"id": job_id, "results": results})
    current_results = instance.get_results()
    instance.payload.save_output(data=current_results, overwrite=True)
    sys.exit(1)


class EmopRun(EmopBase):

    def __init__(self, config_path, proc_id):
        """ Initialize EmopRun object and attributes

        Args:
            config_path (str): path to application config file
            proc_id (str or int): proc-id of this run
        """
        super(self.__class__, self).__init__(config_path)
        self.proc_id = proc_id
        self.payload = EmopPayload(self.settings, proc_id)
        self.scheduler = EmopScheduler.get_scheduler_instance(name=self.settings.scheduler, settings=self.settings)
        self.results = {}
        self.jobs_completed = []
        self.jobs_failed = []
        self.page_results = []
        self.postproc_results = []

    def append_result(self, job, results, failed=False):
        """Append a page's results to job's results payload

        The results are saved to the output JSON file so that the status
        of each page is saved upon failure or success.

        Args:
            job (EmopJob): EmopJob object
            results (str): The error output of a particular process
            failed (bool, optional): Sets if the result is a failure
        """
        if failed:
            results_ext = "%s JOB %s: %s" % (self.scheduler.name, self.scheduler.job_id, results)
            logger.error(results_ext)
            self.jobs_failed.append({"id": job.id, "results": results_ext})
        else:
            self.jobs_completed.append(job.id)

        # TODO: Do we need to handle adding page_results and postproc_results differently??
        if job.page_result.has_data():
            self.page_results.append(job.page_result.to_dict())
        if job.postproc_result.has_data():
            self.postproc_results.append(job.postproc_result.to_dict())

        current_results = self.get_results()
        self.payload.save_output(data=current_results, overwrite=True)

    def get_results(self):
        """Get this object's results

        Returns:
            dict: Results to be used as payload to API
        """
        job_queues_data = {
            "completed": self.jobs_completed,
            "failed": self.jobs_failed,
        }
        data = {
            "job_queues": job_queues_data,
            "page_results": self.page_results,
            "postproc_results": self.postproc_results,
        }

        return data

    @EmopBase.run_timing
    def do_process(self, obj, job, **kwargs):
        """ Run a process

        This function is intended to handle calling and getting the
        success or failure of a job's post process.

        If a process does not return an exitcode of 0 then a failure has occurred
        and the stderr is added to the job's results.

        Args:
            obj (object): The class of a process
            job (EmopJob): EmopJob object
            **kwargs: Arbitrary keyword arguments.

        Returns:
            bool: True if successful, False otherwise.
        """
        klass = obj.__class__.__name__
        if self.settings.controller_skip_existing and not obj.should_run():
            logger.info("Skipping %s job [%s]" % (klass, job.id))
            return True
        result = obj.run(**kwargs)
        if result.exitcode != 0:
            err = "%s Failed: %s" % (klass, result.stderr)
            # TODO need to rework so failed doesn't mean done
            self.append_result(job=job, results=err, failed=True)
            return False
        else:
            return True

    @EmopBase.run_timing
    def do_ocr(self, job):
        """Run the OCR

        The actual OCR class is called from here.  Based on the value
        of the ocr_engine, a different class will be called.

        The ocr_results returned by the OCR class are used to determine if
        the ocr was successful and the results are appended to global results.

        Args:
            job (EmopJob): EmopJob object

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.info(
            "Got job [%s] - Batch: %s JobType: %s OCR Engine: %s" %
            (job.id, job.batch_job.name, job.batch_job.job_type, job.batch_job.ocr_engine)
        )

        # OCR #
        ocr_engine = job.batch_job.ocr_engine
        if ocr_engine == "tesseract":
            ocr = Tesseract(job=job)
        else:
            ocr_engine_err = "OCR with %s not yet supported" % ocr_engine
            self.append_result(job=job, results=ocr_engine_err, failed=True)
            return False

        if self.settings.controller_skip_existing and not ocr.should_run():
            logger.info("Skipping OCR job [%s]" % job.id)
            return True
        ocr_result = ocr.run()

        if ocr_result.exitcode != 0:
            ocr_err = "%s OCR Failed: %s" % (ocr_engine, ocr_result.stderr)
            self.append_result(job=job, results=ocr_err, failed=True)
            return False
        else:
            return True

    def do_postprocesses(self, job):
        """Run the post processes

        Each post process class is called from here.

        Currently the steps are executed in the following order:
            * Denoise
            * MultiColumnSkew
            * XML_To_Text
            * PageEvaluator
            * PageCorrector
            * JuxtaCompare (postprocess)
            * JuxtaCompare - COMMENTED OUT
            * RetasCompare (postprocess)
            * RetasCompare - COMMENTED OUT

        If any step fails, the function terminates and returns False.

        Args:
            job (EmopJob): EmopJob object

        Returns:
            bool: True if successful, False otherwise.
        """
        # DeNoise #
        denoise = Denoise(job=job)
        denoise_proc = self.do_process(obj=denoise, job=job)
        if not denoise_proc:
            return False

        # MultiColumnSkew #
        if self.settings.multi_column_skew_enabled:
            multi_column_skew = MultiColumnSkew(job=job)
            multi_column_skew_proc = self.do_process(obj=multi_column_skew, job=job)
            if not multi_column_skew_proc:
                return False

        # _IDHMC.xml to _IDHMC.txt #
        xml_to_text = XML_To_Text(job=job)
        xml_to_text_proc = self.do_process(obj=xml_to_text, job=job)
        if not xml_to_text_proc:
            return False

        # PageEvaluator #
        page_evaluator = PageEvaluator(job=job)
        page_evaluator_proc = self.do_process(obj=page_evaluator, job=job)
        if not page_evaluator_proc:
            return False

        # PageCorrector #
        page_corrector = PageCorrector(job=job)
        page_corrector_proc = self.do_process(obj=page_corrector, job=job)
        if not page_corrector_proc:
            return False

        # JuxtaCompare postprocess and OCR output #
        juxta_compare = JuxtaCompare(job=job)
        juxta_compare_proc_pp = self.do_process(obj=juxta_compare, job=job, postproc=True)
        if not juxta_compare_proc_pp:
            return False
        # juxta_compare_proc = self.do_process(obj=juxta_compare, job=job, postproc=False)
        # if not juxta_compare_proc:
        #     return False

        # RetasCompare postprocess and OCR output #
        # retas_compare = RetasCompare(job=job)
        # retas_compare_proc_pp = self.do_process(obj=retas_compare, job=job, postproc=True)
        # if not retas_compare_proc_pp:
        #    return False
        # retas_compare_proc = self.do_process(obj=retas_compare, job=job, postproc=False)
        # if not retas_compare_proc:
        #     return False

        return True

    @EmopBase.run_timing
    def do_job(self, job):
        """Execute the parts of a page's job

        Args:
            job (EmopJob): EmopJob object

        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.do_ocr(job=job):
            return False
        if not self.do_postprocesses(job=job):
            return False
        return True

    @EmopBase.run_timing
    def run(self, force=False):
        """Run the EmopJob

        This function is intended to be what's called by external scripts
        like emop.py to start all work.

        Based on the payload's data, all pages are iterated over from here.

        Once the loop of all jobs is complete the final results are saved
        to a file as completed payload

        Args:
            force (bool): Run even if output file exists.

        Returns:
            bool: True if successful, False otherwise.
        """
        global instance
        global job_ids
        data = self.payload.load_input()
        logger.debug("Payload: \n%s" % json.dumps(data, sort_keys=True, indent=4))

        if not data:
            logger.error("No payload data to load.")
            return False
        if not force:
            if self.payload.output_exists():
                logger.error("Output file %s already exists." % self.payload.output_filename)
                return False
            if self.payload.completed_output_exists():
                logger.error("Output file %s already exists." % self.payload.completed_output_filename)
                return False

        # Assign global variables and respond to signals
        for job in data:
            job_ids.append(job["id"])
        instance = self
        signal.signal(signal.SIGUSR1, signal_exit)

        # Loop over jobs to perform actual work
        for job in data:
            emop_job = EmopJob(job_data=job, settings=self.settings, scheduler=self.scheduler)
            if emop_job.batch_job.job_type == "ocr":
                job_succcessful = self.do_job(job=emop_job)
                if not job_succcessful:
                    continue
                # Append successful completion of page #
                self.append_result(job=emop_job, results=None, failed=False)
            # TODO
            # elif batch_job.job_type == "ground truth compare":
            else:
                logger.error("JobType of %s is not yet supported." % emop_job.batch_job.job_type)
                return False

        logger.debug("Payload: \n%s" % json.dumps(self.get_results(), sort_keys=True, indent=4))
        self.payload.save_completed_output(data=self.get_results(), overwrite=force)
        return True
