import law
import luigi
import os
import shutil
import time
import tempfile
from RunKit.sh_tools import sh_call
from RunKit.checkRootFile import checkRootFileSafe

from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param, get_param_value

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
                    '--outFile', self.output().path, '--customisations', self.customisations ], env=self.cmssw_env())
        print(f'anaCache for sample {sample_name} is created in {self.output().path}')

class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 12.0)
    max_files_per_job = luigi.IntParameter(default=1, description="maximum number of input files per job")

    def law_job_home(self):
        if 'LAW_JOB_HOME' in os.environ:
            return os.environ['LAW_JOB_HOME'], False
        os.makedirs(self.local_path(), exist_ok=True)
        return tempfile.mkdtemp(dir=self.local_path()), True

    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self) }

    def requires(self):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        return [ AnaCacheTask.req(self, branch=sample_id, max_runtime=AnaCacheTask.max_runtime._default) ]

    def create_branch_map(self):
        self.load_sample_configs()
        return AnaTupleTask.getBranches(self.samples, self.central_nanoAOD_path(), self.max_files_per_job)

    def output(self, force_pre_output=False):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        out = os.path.join(self.central_anaTuples_path(), sample_name)
        return law.LocalDirectoryTarget(out)

    def run(self):
        job_home, remove_job_home = self.law_job_home()
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        producer_anatuples = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        anaCache = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        outdir_anatuples = os.path.join(job_home, 'anaTuples', sample_name)
        sh_call([ 'python3', producer_anatuples, '--config', self.sample_config, '--inFile', ','.join(input_files),
                  '--outDir', outdir_anatuples, '--sample', sample_name, '--anaCache', anaCache, '--customisations',
                  self.customisations, '--compute_unc_variations', 'True', '--store-noncentral'], env=self.cmssw_env())
        producer_skimtuples = os.path.join(self.ana_path(), 'Analysis', 'SkimProducer.py')
        outdir_skimtuples = os.path.join(job_home, 'skim', sample_name)
        sh_call([ 'python3', producer_skimtuples, '--inputDir',outdir_anatuples, '--workingDir', outdir_skimtuples, '--outputFile', 'skim.root'],verbose=1)
        outdir_final = self.output().path
        print(os.listdir(outdir_skimtuples))
        shutil.move(outdir_skimtuples, outdir_final)
        if remove_job_home:
            shutil.rmtree(job_home)
        print(f'anaTuple for sample {sample_name}  split_idx={split_idx} is created in {outdir_final}')

    @staticmethod
    def getInputFiles(central_nanoAOD_path, sample_name):
        inDir = os.path.join(central_nanoAOD_path, sample_name)
        input_files = []
        for root, dirs, files in os.walk(inDir):
            for file in files:
                if file.endswith('.root') and not file.startswith('.'):
                    input_files.append(os.path.join(root, file))
        return list(sorted(input_files))

    @staticmethod
    def getOutputDir(central_anaTuples_path, sample_name):
        return os.path.join(central_anaTuples_path, sample_name)

    @staticmethod
    def getBranches(samples, central_nanoAOD_path, max_files_per_job):
        n = 0
        branches = {}
        for sample_id, sample_name in enumerate(sorted(samples.keys())):
            input_files = AnaTupleTask.getInputFiles(central_nanoAOD_path, sample_name)
            if len(input_files) == 0:
                raise RuntimeError(f"AnaTupleTask: no input files found for {sample_name}")
            split_idx = 0
            while True:
                start_idx, stop_idx = split_idx * max_files_per_job, (split_idx + 1) * max_files_per_job
                branches[n] = (sample_id, sample_name, samples[sample_name]['sampleType'], split_idx,
                               input_files[start_idx:stop_idx])
                split_idx += 1
                n += 1
                if stop_idx >= len(input_files):
                    break
        return branches
