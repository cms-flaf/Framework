import law
import luigi
import os
import shutil
import time
import threading
import yaml
import contextlib

from RunKit.run_tools import ps_call
from RunKit.grid_tools import gfal_copy_safe,gfal_ls
from RunKit.checkRootFile import checkRootFileSafe
from RunKit.crabLaw import cond as kInit_cond,update_kinit_thread
from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param, get_param_value
from RunKit.law_wlcg import WLCGFileSystem, WLCGFileTarget, WLCGDirectoryTarget



def remote_nanoAOD(name,fs_files):
    return WLCGFileTarget(name,fs_files)

def remote_nanoAOD_directory(dir_name,fs_files):
    return WLCGDirectoryTarget(dir_name,fs_files)

def remote_file_target(name,fs_files):
    return WLCGFileTarget(name, fs_files)

unc_cfg_dict = None
def load_unc_config(unc_cfg):
    global unc_cfg_dict
    with open(unc_cfg, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    return unc_cfg_dict

def getYear(period):
    year_dict = {
        'Run2_2016_HIPM':'2016_HIPM',
        'Run2_2016':'2016',
        'Run2_2017':'2017',
        'Run2_2018':'2018',
    }
    return year_dict[period]

class AnaCacheTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 10.0)

    def create_branch_map(self):
        n = 0
        branches = {}
        for sample_name in sorted(self.samples.keys()):
            isData = self.samples[sample_name]['sampleType'] == 'data'
            branches[n] = (sample_name, isData)
            n += 1
        #print(branches)
        return branches

    def output(self):
        sample_name, isData = self.branch_data
        outFileName = 'anaCache.yaml'
        outDir = os.path.join('anaCache', self.period, sample_name)
        finalFile = os.path.join(outDir, outFileName)
        return remote_file_target(finalFile,self.fs_read)

    def run(self):
        sample_name, isData = self.branch_data
        print(f'Creating anaCache for sample {sample_name} into {self.output().path}')
        producer = os.path.join(self.ana_path(), 'AnaProd', 'anaCacheProducer.py')
        with self.output().localize("w") as outFile_anaCache:
            outFile_str = outFile_anaCache.path
            #print(outFile_str)
            inDir = os.path.join(self.central_nanoAOD_path_HLepRare(), sample_name)
            if not isData:
                ps_call(['python3', producer, '--config', self.sample_config, '--inDir', inDir, '--sample', sample_name, '--outFile', outFile_str, '--customisations', self.customisations ], env=self.cmssw_env())
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
        print(f'Creating inputFile for sample {sample_name} into {self.output().path}')
        os.makedirs(os.path.join(self.local_path(),sample_name), exist_ok=True)
        #print(sample_name)
        with self.output().localize("w") as out_local_file:
            #out_local_file = os.path.join(self.local_path(), sample_name, "tmp.txt")
            inDir = os.path.join(self.central_nanoAOD_path_HLepRare(), sample_name)
            input_files = []
            for fileInfo in gfal_ls(inDir):
                if fileInfo.name.endswith("root") : #and not file.startswith('.'):
                    fileName_absolute_list = fileInfo.path.split("/") + fileInfo.name.split("/")
                    #print(fileName_absolute_list)
                    inDir_list = self.central_nanoAOD_path_HLepRare().split("/")
                    #print(inDir_list)
                    fileName_relative_list = []
                    for dir_idx in range(0, len(fileName_absolute_list)):
                        if dir_idx < len(inDir_list) and fileName_absolute_list[dir_idx] == inDir_list[dir_idx]:
                            continue
                        fileName_relative_list.append(fileName_absolute_list[dir_idx])
                    #print(fileName_relative_list)
                    file_name_relative = '/'.join(f for f in fileName_relative_list)
                    #print(file_name_relative)
                    if file_name_relative not in input_files:
                            input_files.append(file_name_relative)

            with open(out_local_file.path, 'w') as inputFileTxt:
                for input_line in input_files:
                    inputFileTxt.write(input_line+'\n')
        print(f'inputFile for sample {sample_name} is created in {self.output().path}')


class AnaTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    def workflow_requires(self):
        return { "anaCache" : AnaCacheTask.req(self, branches=()), "inputFile": InputFileTask.req(self,workflow='local', branches=()) }

    def requires(self):
        sample_id, sample_name, sample_type, input_file = self.branch_data
        #print(sample_name)
        return [ AnaCacheTask.req(self, branch=sample_id, max_runtime=AnaCacheTask.max_runtime._default, branches=()), InputFileTask.req(self, branch=sample_id, workflow='local', branches=()) ]

    def create_branch_map(self):
        n = 0
        branches = {}
        for sample_id, sample_name in enumerate(sorted(self.samples.keys())):
            inputFileTxt = InputFileTask.req(self, branch=sample_id,workflow='local', branches=(sample_id,)).output().path
            with open(inputFileTxt, 'r') as inputtxtFile:
                input_files = inputtxtFile.read().splitlines()
            #print(input_files)
            if len(input_files) == 0:
                raise RuntimeError(f"AnaTupleTask: no input files found for {sample_name}")
            for input_file in input_files:
                input_file_p = os.path.join(self.period, input_file)
                branches[n] = (sample_id, sample_name, self.samples[sample_name]['sampleType'], remote_file_target(input_file_p, self.fs_files_nanoAOD))
                n += 1
        return branches

    def output(self, force_pre_output=False):
        sample_id, sample_name, sample_type, input_file = self.branch_data
        outFileName = os.path.basename(input_file.path)
        finalFile = os.path.join(self.version, self.period, sample_name, outFileName)
        #finalFile = os.path.join('anaTuples',self.version, self.period, sample_name, outFileName)
        return remote_file_target(finalFile, self.fs_files)

    def run(self):
        sample_id, sample_name, sample_type, input_file = self.branch_data
        producer_anatuples = os.path.join(self.ana_path(), 'AnaProd', 'anaTupleProducer.py')
        producer_skimtuples = os.path.join(self.ana_path(), 'AnaProd', 'SkimProducer.py')
        thread = threading.Thread(target=update_kinit_thread)
        thread.start()
        anaCache_name = os.path.join('anaCache', self.period, sample_name, 'anaCache.yaml')
        try:
            job_home, remove_job_home = self.law_job_home()
            if self.test: print(f"sample_id= {sample_id}\nsample_name = {sample_name}\nsample_type = {sample_type}\ninput_file = {input_file.path}")
            print(f"sample_id= {sample_id}\nsample_name = {sample_name}\nsample_type = {sample_type}\ninput_file = {input_file.path}")
            # part 1 --> nano->anaTupleS
            outdir_anatuples = os.path.join(job_home, 'anaTuples', sample_name)
            with input_file.localize("r") as local_input, remote_file_target(anaCache_name, self.fs_read).localize("r") as anacache_input:
                local_input_path = local_input.path
                anacache_input_path = anacache_input.path
                anatuple_cmd = [ 'python3', producer_anatuples, '--config', self.sample_config, '--inFile', local_input_path, '--outDir', outdir_anatuples, '--sample', sample_name, '--anaCache', anacache_input_path, '--customisations', self.customisations, '--compute_unc_variations', 'True', '--store-noncentral']
                centralFileName = os.path.basename(local_input_path)
                if self.test:
                    anatuple_cmd.extend(['--nEvents', '100'])
                ps_call(anatuple_cmd, env=self.cmssw_env(),verbose=1)
            # part 1 --> anaTupleS -> skimTuplE
            outdir_skimtuples = os.path.join(job_home, 'skim', sample_name)
            outFileName = os.path.basename(input_file.path)

            if self.test: print(f"outFileName is {outFileName}")
            tmpFile = os.path.join(outdir_skimtuples, outFileName)
            if sample_type!='data':
                skimtuple_cmd = ['python3', producer_skimtuples, '--inputDir',outdir_anatuples, '--centralFile',centralFileName, '--workingDir', outdir_skimtuples, '--outputFile', outFileName]
                if self.test:
                    skimtuple_cmd.extend(['--test' , 'True'])
                ps_call(skimtuple_cmd,verbose=1)
            else:
                tmpFile = os.path.join(outdir_anatuples, centralFileName)

            with self.output().localize("w") as tmp_local_file:
                out_local_path = tmp_local_file.path
                shutil.move(tmpFile, out_local_path)

            if remove_job_home:
                shutil.rmtree(job_home)
            print(f'anaTuple for sample {sample_name} is created in {self.output().path}')
        finally:
            kInit_cond.acquire()
            kInit_cond.notify_all()
            kInit_cond.release()
            thread.join()


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
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        prod_branches = []
        for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
            if sample_type != "data": continue
            prod_branches.append(prod_br)
        return { 0: prod_branches }

    def output(self, force_pre_output=False):
        outFileName = 'nanoHTT_0.root'
        finalFile = os.path.join(self.version, self.period, 'data', outFileName)
        #finalFile = os.path.join('anaTuples',self.version, self.period, sample_name, outFileName)
        return remote_file_target(finalFile, self.fs_files)

    def run(self):
        producer_dataMerge = os.path.join(self.ana_path(), 'AnaProd', 'MergeNtuples.py')
        with contextlib.ExitStack() as stack:
            local_inputs = [stack.enter_context(inp.localize('r')).path for inp in self.input()]
            with self.output().localize("w") as tmp_local_file:
                tmpFile = tmp_local_file.path
                dataMerge_cmd = [ 'python3', producer_dataMerge, '--outFile', tmpFile]#, '--useUproot', 'True']
                dataMerge_cmd.extend(local_inputs)
                ps_call(dataMerge_cmd,verbose=1)



class AnaCacheTupleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branch=br, branches=())
            for idx, (sample, sample_type,br) in branch_map.items()
        }
        return workflow_dict

    def requires(self):
        sample_name,sample_type, prod_br = self.branch_data
        return [ AnaTupleTask.req(self, branch=prod_br, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        sample_id_data = 0
        for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
            if sample_type =='QCD':
                continue
            branches[n] = (sample_name, sample_type,prod_br)
            n+=1
        #branches[n+1] = ('data', 0)
        return branches


    def output(self):
        sample_name, sample_type,prod_br = self.branch_data
        outFileName = os.path.basename(self.input()[0].path)
        #print(outFileName)
        outDir = os.path.join('anaCache', self.period, sample_name, self.version)
        finalFile = os.path.join(outDir, outFileName)
        return remote_file_target(finalFile, self.fs_read)

    def run(self):
        sample_name, sample_type,prod_br = self.branch_data
        sample_config = self.sample_config
        unc_config = os.path.join(self.ana_path(), 'config', f'weight_definition_{getYear(self.period)}.yaml')
        unc_cfg_dict = load_unc_config(unc_config)
        producer_anacachetuples = os.path.join(self.ana_path(), 'AnaProd', 'anaCacheTupleProducer.py')

        thread = threading.Thread(target=update_kinit_thread)
        thread.start()
        try:
            job_home, remove_job_home = self.law_job_home()
            input_file = self.input()[0]
            with input_file.localize("r") as local_input, self.output().localize("w") as outFile:
                anaCacheTupleProducer_cmd = ['python3', producer_anacachetuples,'--inFileName', local_input.path, '--outFileName', outFile.path,  '--uncConfig', unc_config]
                if sample_name !='data':
                    anaCacheTupleProducer_cmd.extend(['--compute_unc_variations', 'True'])
                if self.version.split('_')[1]=='deepTau2p5':
                    anaCacheTupleProducer_cmd.extend([ '--deepTauVersion', 'v2p5'])
                #print(anaCacheTupleProducer_cmd)#, env=self.cmssw_env(),verbose=1)
                ps_call(anaCacheTupleProducer_cmd, env=self.cmssw_env(),verbose=1)
            print(f"finished to produce anacachetuple")

        finally:
            kInit_cond.acquire()
            kInit_cond.notify_all()
            kInit_cond.release()
            thread.join()


class DataCacheMergeTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 5.0)

    def workflow_requires(self):
        prod_branches = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaCacheTuple"] = {
            idx: AnaCacheTupleTask.req(self, branches=tuple((br,) for br in branches))
            for idx, branches in prod_branches.items()
        }
        return workflow_dict

    def requires(self):
        prod_branches = self.branch_data
        deps = [AnaCacheTupleTask.req(self, max_runtime=AnaCacheTask.max_runtime._default, branch=prod_br) for prod_br in prod_branches ]
        return deps

    def create_branch_map(self):
        anaProd_branch_map = AnaCacheTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        prod_branches = []
        for prod_br, (sample_name, sample_type,branch) in anaProd_branch_map.items():
            if sample_type != "data": continue
            prod_branches.append(prod_br)
        return { 0: prod_branches }

    def output(self, force_pre_output=False):
        outFileName = 'nanoHTT_0.root'
        outDir = os.path.join('anaCache', self.period, 'data', self.version)
        finalFile = os.path.join(outDir, outFileName)
        return remote_file_target(finalFile, self.fs_read)

    def run(self):
        producer_dataMerge = os.path.join(self.ana_path(), 'AnaProd', 'MergeNtuples.py')
        with contextlib.ExitStack() as stack:
            local_inputs = [stack.enter_context(inp.localize('r')).path for inp in self.input()]
            with self.output().localize("w") as tmp_local_file:
                tmpFile = tmp_local_file.path
                dataMerge_cmd = [ 'python3', producer_dataMerge, '--outFile', tmpFile]
                dataMerge_cmd.extend(local_inputs)
                #print(dataMerge_cmd)
                ps_call(dataMerge_cmd,verbose=1)



