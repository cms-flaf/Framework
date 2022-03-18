import law
import luigi
import math
import os
import yaml


law.contrib.load("htcondor")

class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()
    periods = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        self.all_periods = [ p for p in self.periods.split(',') if len(p) > 0 ]

    def load_sample_configs(self):
        self.samples = {}
        self.global_sample_params = {}
        for period in self.all_periods:
            self.samples[period] = {}
            sample_config = os.path.join(self.ana_path(), 'config', f'samples_{period}.yaml')
            with open(sample_config, 'r') as f:
                samples = yaml.safe_load(f)
            for key, value in samples.items():
                if(type(value) != dict):
                    raise RuntimeError(f'Invalid sample definition period="{period}", sample_name="{key}"' )
                if key == 'GLOBAL':
                    self.global_sample_params[period] = value
                else:
                    self.samples[period][key] = value

    def store_parts(self):
        return (self.__class__.__name__, self.version)

    def ana_path(self):
        return os.getenv("ANALYSIS_PATH")

    def ana_data_path(self):
        return os.getenv("ANALYSIS_DATA_PATH")

    def ana_big_data_path(self):
        return os.getenv("ANALYSIS_BIG_DATA_PATH")

    def central_path(self):
        return os.getenv("CENTRAL_STORAGE")

    def local_path(self, *path):
        parts = (self.ana_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_central_path(self, *path):
        parts = (self.ana_big_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondor. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the CERN HTCondor environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    max_runtime = law.DurationParameter(default=12.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 12")

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        # force to run on CC7, http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice
        config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        # maximum runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        # copy the entire environment
        config.custom_content.append(("getenv", "true"))
        # the CERN htcondor setup requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))
        return config
