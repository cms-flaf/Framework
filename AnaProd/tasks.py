import law
import luigi
import os
import shutil
import time
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



class InputFileTask(Task, law.LocalWorkflow):
    max_files_per_job = luigi.IntParameter(default=1, description="maximum number of input files per job")

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
        sample_out = os.path.join(self.central_anaCache_path(), sample_name, 'input_files.txt')
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name = self.branch_data
        print(f'Creating anaCache for sample {sample_name} into {self.output().path}')
        os.makedirs(os.path.join(self.local_path(),sample_name), exist_ok=True)
        txtFile_tmp = os.path.join(self.local_path(), sample_name, "tmp.txt")
        if os.path.exists(txtFile_tmp):
            os.remove(txtFile_tmp)
        with open(txtFile_tmp, 'a'):
            os.utime(txtFile_tmp)
        inDir = os.path.join(self.central_nanoAOD_path(), sample_name)
        input_files = []
        for root, dirs, files in os.walk(inDir):
            for file in files:
                if file.endswith('.root') and not file.startswith('.'):
                    if os.path.join(root, file) not in input_files:
                        input_files.append(os.path.join(root, file))
        with open(txtFile_tmp, 'r') as inputFileTxt:
            input_lines = inputFileTxt.read().splitlines()
            for input_line in input_files:
                if input_line + '\n' in input_lines:
                    continue
                input_lines.append(input_line + '\n')
        with open(txtFile_tmp, 'a') as inputtxtFile:
            inputtxtFile.writelines(input_lines)
        finalFile = self.output().path
        if os.path.exists(txtFile_tmp):
            shutil.move(txtFile_tmp,finalFile)
            #os.remove(txtFile_tmp)
        print(f'inputFile for sample {sample_name} is created in {self.output().path}')



class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 20.0)
    max_files_per_job = luigi.IntParameter(default=1, description="maximum number of input files per job")


    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self), "inputFile": InputFileTask.req(self) }

    def requires(self):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        return [ AnaCacheTask.req(self, branch=sample_id, max_runtime=AnaCacheTask.max_runtime._default), InputFileTask.req(self, branch=sample_id) ]

    def create_branch_map(self):
        self.load_sample_configs()
        return AnaTupleTask.getBranches(self.samples, self.max_files_per_job, self.central_anaCache_path())

    def output(self, force_pre_output=False):
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        outFileName = input_files[0].split('/')[-1]#.strip('.root')
        out = os.path.join(self.central_anaTuples_path(), sample_name,outFileName)
        return law.LocalFileTarget(out)

    def run(self):
        job_home, remove_job_home = self.law_job_home()
        sample_id, sample_name, sample_type, split_idx, input_files = self.branch_data
        print(sample_id, sample_name, sample_type, split_idx, input_files)
        '''
        producer_anatuples = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        anaCache = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        outdir_anatuples = os.path.join(job_home, 'anaTuples', sample_name)
        sh_call([ 'python3', producer_anatuples, '--config', self.sample_config, '--inFile', ','.join(input_files),
                  '--outDir', outdir_anatuples, '--sample', sample_name, '--anaCache', anaCache, '--customisations',
                  #self.customisations, '--compute_unc_variations', 'True', '--store-noncentral', '--nEvents', '10'], env=self.cmssw_env(),verbose=1)
                  self.customisations, '--compute_unc_variations', 'True', '--store-noncentral'], env=self.cmssw_env(),verbose=1)
        producer_skimtuples = os.path.join(self.ana_path(), 'Analysis', 'SkimProducer.py')
        outdir_skimtuples = os.path.join(job_home, 'skim', sample_name)
        outFileName = input_files[0].split('/')[-1]
        if sample_type!='data':
            sh_call([ 'python3', producer_skimtuples, '--inputDir',outdir_anatuples, '--centralFile',outFileName, '--workingDir', outdir_skimtuples,
                     '--outputFile', outFileName],verbose=1)
                     #'--outputFile', outFileName , '--test' , 'True'],verbose=1)
        tmpFile = os.path.join(outdir_skimtuples, outFileName)
        if sample_type=='data':
            tmpFile = os.path.join(outdir_anatuples, outFileName)
        outdir_final = os.path.join(self.central_anaTuples_path(), sample_name)
        os.makedirs(outdir_final, exist_ok=True)
        finalFile = self.output().path
        shutil.copy(tmpFile, finalFile)
        if os.path.exists(finalFile):
            os.remove(tmpFile)
        else:
            raise RuntimeError(f"file {tmpFile} was not copied in {finalFile} ")
        if remove_job_home:
            shutil.rmtree(job_home)
        print(f'anaTuple for sample {sample_name}  split_idx={split_idx} is created in {outdir_final}')
        '''


    @staticmethod
    def getOutputDir(central_anaTuples_path, sample_name):
        return os.path.join(central_anaTuples_path, sample_name)

    @staticmethod
    def getBranches(samples, max_files_per_job, central_anacache_path):
        n = 0
        branches = {}
        for sample_id, sample_name in enumerate(sorted(samples.keys())):
            inputFileTxt = os.path.join(central_anacache_path, sample_name, 'input_files.txt')
            with open(inputFileTxt, 'r') as inputtxtFile:
                input_files = inputtxtFile.read().splitlines()
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

