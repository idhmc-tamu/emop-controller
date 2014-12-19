import ConfigParser
import os


# TODO: Need sane defaults for some settings
class EmopSettings(object):

    def __init__(self, config_path):
        self.config_path = config_path
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.config_path)

        if os.getenv("EMOP_HOME"):
            self.emop_home = os.getenv("EMOP_HOME")
        else:
            self.emop_home = os.path.dirname(self.config_path)

        # Settings for communicating with dashboard
        self.api_version = self.get_value('dashboard', 'api_version')
        self.url_base = self.get_value('dashboard', 'url_base')
        self.auth_token = self.get_value('dashboard', 'auth_token')
        self.api_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/emop; version=%s' % self.api_version,
            'Authorization': 'Token token=%s' % self.auth_token,
        }

        # Settings used by controller
        self.payload_input_path = self.get_value('controller', 'payload_input_path')
        self.payload_output_path = self.get_value('controller', 'payload_output_path')
        self.ocr_root = self.get_value('controller', 'ocr_root')
        self.input_path_prefix = self.get_value('controller', 'input_path_prefix')
        self.output_path_prefix = self.get_value('controller', 'output_path_prefix')
        self.log_level = self.get_value('controller', 'log_level')

        # Settings used to interact with the cluster scheduler
        self.max_jobs = int(self.get_value('scheduler', 'max_jobs'))
        self.slurm_queue = self.get_value('scheduler', 'queue')
        self.slurm_job_name = self.get_value('scheduler', 'name')
        self.min_job_runtime = int(self.get_value('scheduler', 'min_job_runtime'))
        self.max_job_runtime = int(self.get_value('scheduler', 'max_job_runtime'))
        self.avg_page_runtime = int(self.get_value('scheduler', 'avg_page_runtime'))
        self.slurm_logdir = self.get_value('scheduler', 'logdir')
        self.slurm_logfile = os.path.join(self.slurm_logdir, "%s-%%j.out" % self.slurm_job_name)

        # Settings used by Juxta-cl
        self.juxta_cl_jx_algorithm = self.get_value('juxta-cl', 'jx_algorithm')

    def get_value(self, section, option, default=None):
        interpolation_map = {
            "home": os.getenv("HOME"),
            "emop_home": self.emop_home,
        }
        raw_value = self.config.get(section, option, 0, interpolation_map)

        return raw_value