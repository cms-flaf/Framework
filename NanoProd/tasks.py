import law
import luigi
import os
import yaml

from run_tools.law_customizations import Task, HTCondorWorkflow
from run_tools.grid_helper_tasks import CreateVomsProxy


class CreateNanoSkims(Task, HTCondorWorkflow, law.LocalWorkflow):
    sample_config = luigi.Parameter()

    def requires(self):
        return CreateVomsProxy.req(self)

    def __init__(self, *args, **kwargs):
        super(CreateNanoSkims, self).__init__(*args, **kwargs)

    def load_config(self):
        with open(self.sample_config, 'r') as f:
            samples = yaml.safe_load(f)
        self.samples = { key: value for key, value in samples.items() if key != 'GLOBAL' }
        self.global_params = samples['GLOBAL']

    def create_branch_map(self):
        self.load_config()
        period = self.global_params['period']
        return { n: (sample_name, period) for n, sample_name in enumerate(self.samples.keys())}

    def output(self):
        sample_name, period = self.branch_data
        sample_out = os.path.join(self.central_path(), 'nanoAOD', self.version, period, sample_name + '.root' )
        return law.LocalFileTarget(sample_out)

    def run(self):
        #self.load
        print(f'{self.branch} {self.output().path}')
