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
        branches = {}
        for n, sample_name in enumerate(sorted(self.samples.keys())):
            branches[n] = sample_name
        return branches

    def output(self):
        sample_name = self.branch_data
        sample_out = os.path.join(self.local_path(sample_name, "input_files.txt"))
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name = self.branch_data
        print(f'Creating anaCache for sample {sample_name} into {self.output().path}')
        os.makedirs(os.path.join(self.local_path(),sample_name), exist_ok=True)
        txtFile_tmp = os.path.join(self.local_path(), sample_name, "tmp.txt")
        inDir = os.path.join(self.central_nanoAOD_path(), sample_name)
        input_files = []
        for root, dirs, files in os.walk(inDir):
            for file in files:
                if file.endswith('.root') and not file.startswith('.'):
                    if os.path.join(root, file) not in input_files:
                        input_files.append(os.path.join(root, file))
        with open(txtFile_tmp, 'w') as inputFileTxt:
            for input_line in input_files:
                inputFileTxt.write(input_line+'\n')
        finalFile = self.output().path
        shutil.move(txtFile_tmp,finalFile)
        print(f'inputFile for sample {sample_name} is created in {self.output().path}')



class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 5.0)

    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self, branches=()), "inputFile": InputFileTask.req(self,workflow='local', branches=()) }

    def requires(self):
        sample_id, sample_name, sample_type, input_file = self.branch_data
        return [ AnaCacheTask.req(self, branch=sample_id, max_runtime=AnaCacheTask.max_runtime._default, branches=()), InputFileTask.req(self, branch=sample_id, workflow='local', branches=()) ]

    def create_branch_map(self):
        n = 0
        branches = {}
        for sample_id, sample_name in enumerate(sorted(self.samples.keys())):
            inputFileTxt = InputFileTask.req(self, branch=sample_id,workflow='local', branches=(sample_id,)).output().path
            with open(inputFileTxt, 'r') as inputtxtFile:
                input_files = inputtxtFile.read().splitlines()
            if len(input_files) == 0:
                raise RuntimeError(f"AnaTupleTask: no input files found for {sample_name}")
            for input_file in input_files:
                branches[n] = (sample_id, sample_name, self.samples[sample_name]['sampleType'], input_file)
                n += 1
        return branches

    def output(self, force_pre_output=False):
        sample_id, sample_name, sample_type, input_file = self.branch_data
        outFileName = os.path.basename(input_file)# input_file[0].split('/')[-1]#.strip('.root')
        out = os.path.join(self.central_anaTuples_path(), sample_name,outFileName)
        return law.LocalFileTarget(out)

    def run(self):
        job_home, remove_job_home = self.law_job_home()
        sample_id, sample_name, sample_type, input_file = self.branch_data
        if self.test: print(f"sample_id= {sample_id}\nsample_name = {sample_name}\nsample_type = {sample_type}\ninput_file = {input_file}")
        producer_anatuples = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        anaCache = os.path.join(self.central_anaCache_path(), sample_name, 'anaCache.yaml')
        outdir_anatuples = os.path.join(job_home, 'anaTuples', sample_name)
        anatuple_cmd = [ 'python3', producer_anatuples, '--config', self.sample_config, '--inFile', input_file,
                    '--outDir', outdir_anatuples, '--sample', sample_name, '--anaCache', anaCache, '--customisations',
                    self.customisations, '--compute_unc_variations', 'True', '--store-noncentral']
        if self.test:
            anatuple_cmd.extend(['--nEvents', '100'])
        sh_call(anatuple_cmd, env=self.cmssw_env(),verbose=1)
        producer_skimtuples = os.path.join(self.ana_path(), 'Analysis', 'SkimProducer.py')
        outdir_skimtuples = os.path.join(job_home, 'skim', sample_name)
        outFileName = os.path.basename(input_file)
        if self.test: print(f"outFileName is {outFileName}")
        if sample_type!='data':
            skimtuple_cmd = ['python3', producer_skimtuples, '--inputDir',outdir_anatuples, '--centralFile',outFileName, '--workingDir', outdir_skimtuples,
                     '--outputFile', outFileName]
            #if self.test:
                #skimtuple_cmd.extend(['--test' , 'True'])
            sh_call(skimtuple_cmd,verbose=1)
        tmpFile = os.path.join(outdir_skimtuples, outFileName)
        if sample_type=='data':
            tmpFile = os.path.join(outdir_anatuples, outFileName)
        outdir_final = os.path.join(self.central_anaTuples_path(), sample_name)
        os.makedirs(outdir_final, exist_ok=True)
        finalFile = self.output().path
        if self.test: print(f"finalFile is {finalFile}")
        shutil.copy(tmpFile, finalFile)
        if os.path.exists(finalFile):
            os.remove(tmpFile)
        else:
            raise RuntimeError(f"file {tmpFile} was not copied in {finalFile} ")
        if remove_job_home:
            shutil.rmtree(job_home)
        print(f'anaTuple for sample {sample_name} is created in {outdir_final}')

    def getBranches():
        return AnaTupleTask.branches

    @staticmethod
    def getOutputDir(central_anaTuples_path, sample_name):
        return os.path.join(central_anaTuples_path, sample_name)

class DataMergeTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 5.0)

    def workflow_requires(self):
        prod_branches = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branches=tuple((br,) for br in branches))
            for idx, branches in prod_branches.items()
        }
        return workflow_dict

    def requires(self):
        prod_branches = self.branch_data
        deps = [AnaTupleTask.req(self, max_runtime=AnaCacheTask.max_runtime._default, branch=prod_br) for prod_br in prod_branches ]
        return deps


    def create_branch_map(self):
        deps = []
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        prod_branches = []
        for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
            if sample_type != "data": continue
            prod_branches.append(prod_br)
        return { 0: prod_branches }

    def output(self, force_pre_output=False):
        out = os.path.join(self.central_anaTuples_path(), 'data','nano.root')
        return law.LocalFileTarget(out)

    def run(self):
        inputs = ' '.join(x.path for x in self.input())
        producer_dataMerge = os.path.join(self.ana_path(), 'AnaProd', 'MergeNtuples.py')
        tmpFile = os.path.join(outdir_dataMerge, 'data.root')
        dataMerge_cmd = ['python3', producer_dataMerge, inputs, '--outFile', tmpFile ]
        sh_call(dataMerge_cmd,verbose=1)
        finalFile = self.output().path
        if self.test: print(f"finalFile is {finalFile}")
        shutil.copy(tmpFile, finalFile)
        if os.path.exists(finalFile):
            os.remove(tmpFile)
