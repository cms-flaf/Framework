import law
import luigi
import os
import shutil
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
                    '--outFile', self.output().path, '--customisations', load_customisations()], env=self.cmssw_env())
        print(f'anaCache for sample {sample_name} is created in {self.output().path}')

class AnaTuplePreTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 2.0)
    max_files_per_job = luigi.IntParameter(default=1, description="maximum number of input files per job")

    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self) }

    def requires(self):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        return [ AnaCacheTask.req(self, branch=sample_id, max_runtime=AnaCacheTask.max_runtime._default) ]

    def create_branch_map(self):
        self.load_sample_configs()
        return AnaTuplePreTask.getBranches(self.samples, self.central_nanoAOD_path(), self.max_files_per_job)

    def output(self, force_pre_output=False):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        final_output = AnaTupleTask.getOutputFile(self.central_anaTuples_path(), sample_type)
        if force_pre_output or not os.path.exists(final_output):
            out = AnaTuplePreTask.getOutputFile(self.central_anaTuples_path(), sample_name, split_idx)
        else:
            out = final_output
        return law.LocalFileTarget(out)

    def run(self):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        output = self.output(force_pre_output=True).path
        print(f'Creating anaTuple for sample {sample_name} split_idx={split_idx} into {output}')
        producer = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        out_dir, out_file = os.path.split(output)
        tmp_file = self.local_central_path(sample_name, out_file)
        os.makedirs(out_dir, exist_ok=True)
        os.makedirs(self.local_central_path(sample_name), exist_ok=True)
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        anaCache = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        sh_call([ 'python3', producer, '--config', self.sample_config, '--inFile', ','.join(input_files),
                  '--outFile', tmp_file, '--sample', sample_name, '--anaCache', anaCache, '--customisations', load_customisations()], env=self.cmssw_env())
        if not checkRootFileSafe(tmp_file, 'Events', verbose=1):
            os.remove(tmp_file)
            raise RuntimeError(f'Produced anaTuple {tmp_file} is corrupted')
        shutil.move(tmp_file, output)
        print(f'anaTuple for sample {sample_name}  split_idx={split_idx} is created in {output}')

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
    def getOutputFile(central_anaTuples_path, sample_name, split_idx):
        return os.path.join(central_anaTuples_path, '_pre', sample_name, f'anaTuple_{split_idx}.root')

    @staticmethod
    def getBranches(samples, central_nanoAOD_path, max_files_per_job):
        n = 0
        branches = {}
        for sample_id, sample_name in enumerate(sorted(samples.keys())):
            input_files = AnaTuplePreTask.getInputFiles(central_nanoAOD_path, sample_name)
            if len(input_files) == 0:
                raise RuntimeError(f"AnaTuplePreTask: no input files found for {sample_name}")
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

class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 2.0)

    def workflow_requires(self):
        return { "AnaTuplePreTask" : AnaTuplePreTask.req(self) }

    def requires(self):
        sample_type, pre_files = self.branch_data
        required = []
        for pre_branch in pre_files.keys():
            required.append(AnaTuplePreTask.req(self, branch=pre_branch, max_runtime=AnaCacheTask.max_runtime._default))
        return required

    def create_branch_map(self):
        self.load_sample_configs()
        max_files_per_job = get_param_value(AnaTuplePreTask, 'max_files_per_job')
        all_pre_branches = AnaTuplePreTask.getBranches(self.samples, self.central_nanoAOD_path(), max_files_per_job)
        n = 0
        branches = {}
        sample_types = {}
        for sample, sample_desc in self.samples.items():
            sample_type = sample_desc['sampleType']
            if sample_type not in sample_types:
                sample_types[sample_type] = []
            sample_types[sample_type].append(sample)
        for sample_type, samples in sample_types.items():
            pre_outputs = {}
            for pre_branch, pre_branch_data in all_pre_branches.items():
                sample_id, sample_name, pre_sample_type, split_idx, input_files = pre_branch_data
                if sample_name in samples:
                    pre_output = AnaTuplePreTask.getOutputFile(self.central_anaTuples_path(), sample_name, split_idx)
                    if pre_branch not in pre_outputs:
                        pre_outputs[pre_branch] = []
                    pre_outputs[pre_branch].append(pre_output)
            branches[n] = (sample_type, pre_outputs)
            n += 1
        return branches

    def output(self):
        sample_type, pre_files = self.branch_data
        output_file = AnaTupleTask.getOutputFile(self.central_anaTuples_path(), sample_type)
        return law.LocalFileTarget(output_file)

    def run(self):
        sample_type, pre_files = self.branch_data
        output = self.output().path
        print(f'Merging anaTuple for {sample_type} into {output}')
        input_files = []
        for pre_branch, branch_files in pre_files.items():
            input_files.extend(branch_files)
        out_dir, out_file = os.path.split(output)
        tmp_file = self.local_central_path(out_file)
        os.makedirs(out_dir, exist_ok=True)
        os.makedirs(self.local_central_path(), exist_ok=True)
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        sh_call([ 'hadd', '-f209', tmp_file ] + input_files, verbose=1)
        if not checkRootFileSafe(tmp_file, 'Events', verbose=1):
            os.remove(tmp_file)
            raise RuntimeError(f'Produced anaTuple {tmp_file} is corrupted')
        shutil.move(tmp_file, output)
        print(f'anaTuple for {sample_type} is created in {output}')
        for input_file in input_files:
            os.remove(input_file)

    @staticmethod
    def getOutputFile(central_anaTuples_path, sample_type):
        return os.path.join(central_anaTuples_path, f'{sample_type}.root')
