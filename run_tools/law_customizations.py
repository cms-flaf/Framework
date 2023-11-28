import copy
import law
import luigi
import math
import os
import re
import yaml
import tempfile

from RunKit.envToJson import get_cmsenv
from RunKit.crabLaw import update_kinit

law.contrib.load("htcondor")

def copy_param(ref_param, new_default):
  param = copy.deepcopy(ref_param)
  param._default = new_default
  return param

def get_param_value(cls, param_name):
    param = getattr(cls, param_name)
    return param.task_value(cls.__name__, param_name)

def select_items(all_items, filters):
    def name_match(name, pattern):
        if pattern[0] == '^':
            return re.match(pattern, name) is not None
        return name == pattern

    selected_items = { c for c in all_items }
    excluded_items = set()
    keep_prefix = "keep "
    drop_prefix = "drop "
    used_filters = set()
    for item_filter in filters:
        if item_filter.startswith(keep_prefix):
            keep = True
            items_from = excluded_items
            items_to = selected_items
            prefix = keep_prefix
        elif item_filter.startswith(drop_prefix):
            keep = False
            items_from = selected_items
            items_to = excluded_items
            prefix = drop_prefix
        else:
            raise RuntimeError(f'Unsupported filter = "{item_filter}".')
        pattern = item_filter[len(prefix):]
        if len(pattern) == 0:
            raise RuntimeError(f'Filter with an empty pattern expression.')

        to_move = [ item for item in items_from if name_match(item, pattern) ]
        if len(to_move) > 0:
            used_filters.add(item_filter)
            for column in to_move:
                items_from.remove(column)
                items_to.add(column)

    unused_filters = set(filters) - used_filters
    if len(unused_filters) > 0:
        print("Unused filters: " + " ".join(unused_filters))

    return list(sorted(selected_items))

_global_params = None
_samples = None
_hists = None

def load_hist_config(hist_config):
    global _hists
    with open(hist_config, 'r') as f:
        _hists = yaml.safe_load(f)
    return _hists

def load_sample_configs(sample_config, period):
    global _global_params
    global _samples

    if _global_params is None:
        with open(sample_config, 'r') as f:
            _samples = yaml.safe_load(f)

        _global_params = _samples['GLOBAL']
        all_samples = []
        for key, value in _samples.items():
            if(type(value) != dict):
                raise RuntimeError(f'Invalid sample definition period="{period}", sample_name="{key}"' )
            if key != 'GLOBAL':
                all_samples.append(key)
        selected_samples = select_items(all_samples, _global_params.get('sampleSelection', []))
        _samples = { key : _samples[key] for key in selected_samples }
    return _global_params, _samples


class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()
    period = luigi.Parameter()
    customisations =luigi.Parameter(default="")
    test = luigi.BoolParameter(default=False)
    wantBTag = luigi.BoolParameter(default=False)
    mass = luigi.Parameter(default="500")

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        self.cmssw_env_ = None
        self.sample_config = os.path.join(self.ana_path(), 'config', f'samples_{self.period}.yaml')
        self.global_params, self.samples = load_sample_configs(self.sample_config, self.period)

    def store_parts(self):
        return (self.__class__.__name__, self.period, self.version)

    def ana_path(self):
        return os.getenv("ANALYSIS_PATH")

    def ana_data_path(self):
        return os.getenv("ANALYSIS_DATA_PATH")

    def ana_big_data_path(self):
        return os.getenv("ANALYSIS_BIG_DATA_PATH")

    def central_path(self):
        return os.getenv("CENTRAL_STORAGE")

    def central_nanoAOD_path(self):
        return os.path.join(self.central_path(), 'nanoAOD', self.period)

    def central_anaTuples_path(self):
        return os.path.join(self.central_path(), 'anaTuples', self.period, self.version)

    def valeos_path(self):
        return os.getenv("VDAMANTE_STORAGE")

    def central_Histograms_path(self):
        return os.path.join(self.valeos_path(), 'histograms', self.period, self.version)

    def central_anaCache_path(self):
        return os.path.join(self.central_path(), 'anaCache', self.period)

    def local_path(self, *path):
        parts = (self.ana_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_central_path(self, *path):
        parts = (self.ana_big_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))

    def cmssw_env(self):
        if self.cmssw_env_ is None:
            self.cmssw_env_ = get_cmsenv(cmssw_path=os.getenv("DEFAULT_CMSSW_BASE"))
            for var in [ 'HOME', 'ANALYSIS_PATH', 'ANALYSIS_DATA_PATH', 'X509_USER_PROXY', 'CENTRAL_STORAGE',
                         'ANALYSIS_BIG_DATA_PATH', 'DEFAULT_CMSSW_BASE']:
                if var in os.environ:
                    self.cmssw_env_[var] = os.environ[var]
        return self.cmssw_env_

    def law_job_home(self):
        if 'LAW_JOB_HOME' in os.environ:
            return os.environ['LAW_JOB_HOME'], False
        os.makedirs(self.local_path(), exist_ok=True)
        return tempfile.mkdtemp(dir=self.local_path()), True

    def poll_callback(self, poll_data):
        update_kinit(verbose=0)


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondor. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the CERN HTCondor environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    max_runtime = law.DurationParameter(default=12.0, unit="h", significant=False,
                                        description="maximum runtime, default unit is hours")
    n_cpus = luigi.IntParameter(default=1, description="number of cpus")
    poll_interval = copy_param(law.htcondor.HTCondorWorkflow.poll_interval, 5)

    def htcondor_check_job_completeness(self):
        return False

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return os.path.join(os.getenv("ANALYSIS_PATH"), "bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        ana_path = os.getenv("ANALYSIS_PATH")
        # render_variables are rendered into all files sent with a job
        config.render_variables["analysis_path"] = ana_path
        # force to run on CC7, https://batchdocs.web.cern.ch/local/submit.html
        config.custom_content.append(("requirements", 'TARGET.OpSysAndVer =?= "AlmaLinux9"'))

        # maximum runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        config.custom_content.append(("RequestCpus", self.n_cpus))
        return config
