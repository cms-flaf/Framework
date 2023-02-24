import law
import luigi
import os
import shutil
from RunKit.sh_tools import sh_call

from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param

class AnaCacheTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 2.0)

    def create_branch_map(self):
        self.load_sample_configs()
        n = 0
        branches = {}
        for sample_name in sorted(self.samples.keys()):
            isData = self.samples[sample_name]['sampleType'] == 'data'
            branches[n] = (sample_name, isData)
            n += 1
        return branches

    def output(self):
        sample_name, isData = self.branch_data
        sample_out = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name, isData = self.branch_data
        print(f'Creating anaCache for sample {sample_name} into {self.output().path}')
        if isData:
            self.output().touch()
        else:
            producer = os.path.join(self.ana_path(), 'AnaProd', 'anaCacheProducer.py')
            inDir = os.path.join(self.central_nanoAOD_path(), sample_name)
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            sh_call(['python3', producer, '--config', self.sample_config, '--inDir', inDir,
                    '--outFile', self.output().path], env=self.cmssw_env())
        print(f'anaCache for sample {sample_name} is created in {self.output().path}')

class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):

    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self) }

    def requires(self):
        return [ AnaCacheTask.req(self) ]

    def create_branch_map(self):
        self.load_sample_configs()
        n = 0
        branches = {}
        for sample_name in sorted(self.samples.keys()):
            branches[n] = sample_name
            n += 1
        return branches

    def output(self):
        sample_name = self.branch_data
        sample_out = os.path.join(self.central_anaTuples_path(), sample_name + '.root')
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name = self.branch_data
        print(f'Creating anaTuple for sample {sample_name} into {self.output().path}')
        producer = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        out_dir, out_file = os.path.split(self.output().path)
        tmp_file = self.local_central_path(out_file)
        os.makedirs(out_dir, exist_ok=True)
        os.makedirs(self.local_central_path(), exist_ok=True)
        inFile = os.path.join(self.central_nanoAOD_path(), sample_name, '*.root')
        anaCache = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        sh_call([ 'python3', producer, '--config', self.sample_config, '--inFile', inFile,
                  '--outFile', tmp_file, '--sample', sample_name, '--anaCache', anaCache ], env=self.cmssw_env())
        shutil.move(tmp_file, self.output().path)
        print(f'anaCache for sample {sample_name} is created in {self.output().path}')
