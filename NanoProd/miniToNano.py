import json
import law
import luigi
import os
import shutil
import yaml

from RunKit.grid_helper_tasks import CreateVomsProxy
from RunKit.sh_tools import sh_call, xrd_copy
from RunKit.crab import CrabTask
from run_tools.law_customizations import Task, HTCondorWorkflow

class MiniToNanoBase(Task):
    selected_samples = luigi.Parameter(default='')

    def getSampleOutput(self, period, sample_name):
        return os.path.join(self.central_path(), self.version, 'nanoAOD', period, sample_name + '.root' )

    def getSelectedSamples(self):
        selected_samples = []
        if len(self.selected_samples) > 0:
            selected_samples = self.selected_samples.split(',')
        return selected_samples


class CreateCrabJobsCfg(MiniToNanoBase):

    def crab_work_area(self):
        return os.path.join(self.ana_data_path(), CrabTask.__name__, self.version)

    def output(self):
        out = os.path.join(self.crab_work_area(), "crabJobs.json")
        return law.LocalFileTarget(out)

    def run(self):
        self.load_sample_configs()
        selected_samples = self.getSelectedSamples()
        tasks = []
        for period, sample_descs in self.samples.items():
            for sample_name in sorted(sample_descs.keys()):
                if len(selected_samples) != 0 and sample_name not in selected_samples: continue
                sample = sample_descs[sample_name]
                sample_out = self.getSampleOutput(period, sample_name)
                task = {
                    'name': f'{period}_{sample_name}',
                    'das_path': sample["miniAOD"],
                    'output': sample_out,
                }
                tasks.append(task)
        self.output().dump({ "tasks" : tasks }, indent=2)

class ProduceNano(MiniToNanoBase):
    selected_samples = luigi.Parameter(default='')

    def __init__(self, *args, **kwargs):
        super(ProduceNano, self).__init__(*args, **kwargs)
        self.load_sample_configs()

    def requires(self):
        cfg = CreateCrabJobsCfg.req(self, selected_samples=self.selected_samples)
        return [
            cfg,
            CrabTask.req(self, jobs_cfg=cfg.output().path, work_area=cfg.crab_work_area() )
        ]

    def output(self):
        selected_samples = self.getSelectedSamples()
        files = []
        for period, sample_descs in self.samples.items():
            for sample_name in sorted(sample_descs.keys()):
                if len(selected_samples) != 0 and sample_name not in selected_samples: continue
                sample_out = self.getSampleOutput(period, sample_name)
                files.append(law.LocalFileTarget(sample_out))
        return files

    def run(self):
        pass