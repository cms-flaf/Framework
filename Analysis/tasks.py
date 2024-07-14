import law
import luigi
import os
import shutil
import time
import yaml
import contextlib
import ROOT

from RunKit.run_tools import ps_call, natural_sort
from RunKit.crabLaw import cond as kInit_cond, update_kinit_thread
from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param
from Common.Utilities import SerializeObjectToString
from AnaProd.tasks import AnaTupleTask, DataMergeTask

unc_2018 = ['JES_BBEC1_2018', 'JES_Absolute_2018', 'JES_EC2_2018', 'JES_HF_2018', 'JES_RelativeSample_2018' ]
unc_2017 = ['JES_BBEC1_2017', 'JES_Absolute_2017', 'JES_EC2_2017', 'JES_HF_2017', 'JES_RelativeSample_2017' ]
unc_2016preVFP = ['JES_BBEC1_2016preVFP', 'JES_Absolute_2016preVFP', 'JES_EC2_2016preVFP', 'JES_HF_2016preVFP', 'JES_RelativeSample_2016preVFP' ]
unc_2016postVFP = ['JES_BBEC1_2016postVFP', 'JES_Absolute_2016postVFP', 'JES_EC2_2016postVFP', 'JES_HF_2016postVFP', 'JES_RelativeSample_2016postVFP' ]

uncs_to_exclude = {
    'Run2_2018': unc_2017+ unc_2016preVFP + unc_2016postVFP,
    'Run2_2017': unc_2018+ unc_2016preVFP + unc_2016postVFP,
    'Run2_2016': unc_2017+ unc_2018 + unc_2016preVFP,
    'Run2_2016_HIPM':unc_2017+ unc_2018 + unc_2016postVFP,
    }

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

def parseVarEntry(var_entry):
    if type(var_entry) == str:
        var_name = var_entry
        need_cache = False
    else:
        var_name = var_entry['name']
        need_cache = var_entry.get('need_cache', False)
    return var_name, need_cache

def GetSamples(samples, backgrounds, signals=['GluGluToRadion','GluGluToBulkGraviton']):
    global samples_to_consider
    samples_to_consider = ['data']

    for sample_name in samples.keys():
        sample_type = samples[sample_name]['sampleType']
        if sample_type in signals or sample_name in backgrounds:
            samples_to_consider.append(sample_name)
    return samples_to_consider

class HistProducerFileTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 5.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)

    def workflow_requires(self):
        anaTupleReq = {}
        dataMergeReq = {}
        for idx, (sample, br, var, need_cache) in self.branch_map.items():
            if sample == 'data':
                dataMergeReq[idx] = DataMergeTask.req(self, branch=0, branches=())
            else:
                anaTupleReq[idx] = AnaTupleTask.req(self, branch=br, branches=())
        return { "anaTuple": anaTupleReq, "dataMergeTuple": dataMergeReq }

    def requires(self):
        sample_name, prod_br, var, need_cache = self.branch_data
        if sample_name =='data':
            return [ DataMergeTask.req(self, max_runtime=DataMergeTask.max_runtime._default, branch=prod_br, branches=())]
        return [ AnaTupleTask.req(self, branch=prod_br, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds)
        for var_entry in self.global_params['vars_to_plot']:
            var_name, need_cache = parseVarEntry(var_entry)
            for prod_br,(sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
                isData = self.samples[sample_name]['sampleType'] == 'data'
                if sample_name not in samples_to_consider or isData: continue
                branches[n] = (sample_name, prod_br, var_name, need_cache)
                n += 1
            branches[n] = ('data', 0, var_name, need_cache)
            n += 1
        return branches

    def output(self):
        if len(self.branch_data) == 0:
            return self.local_target('dummy.txt')

        sample_name, prod_br, var, need_cache = self.branch_data
        input_file = self.input()[0].path
        outFileName = os.path.basename(self.input()[0].path)
        output_path = os.path.join(self.period, sample_name, self.version,'tmp', var, outFileName)
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        sample_name, prod_br, var, need_cache = self.branch_data
        if len(self.input()) > 1:
            raise RuntimeError(f"multple input files!! {' '.join(f.path for f in self.input())}")
        input_file = self.input()[0]
        inputFileName = os.path.basename(self.input()[0].path)
        print(f'input file is {input_file.path}')
        global_config = os.path.join(self.ana_path(), 'config','HH_bbtautau', f'global.yaml')
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        sample_config = os.path.join(self.ana_path(), 'config',self.period, f'samples.yaml')
        anaCacheTuple_path = os.path.join('anaCacheTuple', self.period, sample_name, self.version, inputFileName)
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistProducerFile.py')
        print(f'output file is {self.output().path}')
        with input_file.localize("r") as local_input, self.output().localize("w") as local_output:
            HistProducerFile_cmd = [ 'python3', HistProducerFile,
                                    '--inFile', local_input.path, '--outFileName',local_output.path,
                                    '--dataset', sample_name, '--uncConfig', unc_config,
                                    '--histConfig', self.setup.hist_config_path,
                                    '--globalConfig', global_config, '--sampleConfig', sample_config, '--var', var ]
            if self.global_params['store_noncentral']:
                HistProducerFile_cmd.extend(['--compute_unc_variations', 'True'])
            if self.global_params['compute_unc_variations']:
                HistProducerFile_cmd.extend(['--compute_rel_weights', 'True'])
            if 'deepTau2p5' in self.version.split('_'):
                print("deepTau2p5 in use")
                HistProducerFile_cmd.extend([ '--deepTauVersion', 'v2p5'])
            if need_cache:
                with self.remote_target(anaCache_path, fs=self.fs_anaCache).localize("r") as local_anacache:
                    HistProducerFile_cmd.extend(['--cacheFile', local_anacache.path])
                    ps_call(HistProducerFile_cmd, verbose=1)
            else:
                ps_call(HistProducerFile_cmd, verbose=1)

class HistProducerSampleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)

    def workflow_requires(self):
        workflow_deps = {}
        for br_idx, (sample_name, dep_br_list, var) in self.branch_map.items():
            workflow_deps[br_idx] = [
                HistProducerFileTask.req(self, branch=dep_br, branches=()) for dep_br in dep_br_list
            ]
        return workflow_deps

    def requires(self):
        sample_name, dep_br_list, var = self.branch_data
        deps = []
        for dep_br in dep_br_list:
            deps.append(HistProducerFileTask.req(self, max_runtime=HistProducerFileTask.max_runtime._default,
                                                 branch=dep_br, branches=()))
        return deps

    def create_branch_map(self):
        branches = {}
        histProducerFile_map = HistProducerFileTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds)
        for n_branch, (sample_name, prod_br, var, need_cache)  in histProducerFile_map.items():
            if sample_name not in samples_to_consider: continue
            if sample_name not in all_samples:
                all_samples[sample_name] = {}
            if var not in all_samples[sample_name]:
                all_samples[sample_name][var]=[]
            all_samples[sample_name][var].append(n_branch)
        k = 0
        for sample_name, sample_entry in all_samples.items():
            for var, branch_idx_list in sample_entry.items():
                branches[k] = (sample_name, branch_idx_list, var)
                k += 1
        return branches

    def output(self):
        sample_name, idx_list, var  = self.branch_data
        local_files_target = []
        outFileName_histProdSample = f'{var}.root'
        output_path = os.path.join(self.period, sample_name, self.version, var, outFileName_histProdSample)
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        sample_name, idx_list, var  = self.branch_data
        HistProducerSample = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample.py')
        with contextlib.ExitStack() as stack:
            #print(self.input())
            local_inputs = [stack.enter_context(inp.localize('r')).path for inp in self.input()]
            with self.output().localize("w") as tmp_local_file:
                HistProducerSample_cmd = ['python3', HistProducerSample,'--outFile', tmp_local_file.path]#, '--remove-files', 'True']
                HistProducerSample_cmd.extend(local_inputs)
                ps_call(HistProducerSample_cmd,verbose=1)

class MergeTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)

    def workflow_requires(self):
        histProducerSample_map = HistProducerSampleTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        branches = {}
        for br_idx, (smpl_name, idx_list, var) in histProducerSample_map.items():
            if var not in all_samples:
                all_samples[var] = []
            all_samples[var].append(br_idx)
        workflow_dict = {}
        n=0
        for var in all_samples.keys():
            workflow_dict[var] = {
                n: HistProducerSampleTask.req(self, branches=tuple((idx,) for idx in all_samples[var]))
            }
            n+=1
        return workflow_dict

    def requires(self):
        uncName, var, branches_idx = self.branch_data
        deps = [HistProducerSampleTask.req(self, max_runtime=HistProducerSampleTask.max_runtime._default, branch=prod_br) for prod_br in branches_idx ]
        return deps

    def create_branch_map(self):
        histProducerSample_map = HistProducerSampleTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        branches = {}
        for br_idx, (smpl_name, idx_list, var) in histProducerSample_map.items():
            if var not in all_samples:
                all_samples[var] = []
            all_samples[var].append(br_idx)
        k=0
        uncNames = ['Central']
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        unc_cfg_dict = load_unc_config(unc_config)
        uncNames.extend(list(unc_cfg_dict['norm'].keys()))
        uncNames.extend([unc for unc in unc_cfg_dict['shape']])
        for uncName in uncNames:
            if uncName in uncs_to_exclude[self.period]: continue
            for n, key in enumerate(all_samples.items()):
                var, branches_idx = key
                branches[k] = (uncName, var, branches_idx)
                k+=1
        return branches

    def output(self):
        if len(self.branch_data) == 0:
            return self.local_target('dummy.txt')
        uncName, var, branches_idx = self.branch_data
        outDir_MergeHists = os.path.join(self.period, 'all_histograms', self.version, var)
        outFileName_MergeHists = f'all_histograms_{var}_{uncName}.root'
        output_path = os.path.join(outDir_MergeHists, outFileName_MergeHists)
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        uncName, var, branches_idx = self.branch_data
        sample_config = os.path.join(self.ana_path(), 'config',self.period, f'samples.yaml')
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        global_config = os.path.join(self.ana_path(), 'config','HH_bbtautau', f'global.yaml')
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger.py')
        all_inputs = []
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds)
        for sample_name in self.samples.keys():
            if sample_name not in samples_to_consider: continue
            output_path_hist_prod_sample = os.path.join(self.period, sample_name, self.version, var, f'{var}.root')
            all_inputs.append((self.remote_target(output_path_hist_prod_sample, fs=self.fs_histograms),sample_name))
        output_path_hist_prod_sample_data = os.path.join(self.period, 'data', self.version, var, f'{var}.root')
        all_inputs.append((self.remote_target(output_path_hist_prod_sample_data, fs=self.fs_histograms),'data'))
        all_datasets=[]
        with contextlib.ExitStack() as stack:
            local_inputs = []
            for inp, smpl in all_inputs:
                local_inputs.append(stack.enter_context(inp.localize('r')).path)
                all_datasets.append(smpl)
            with self.output().localize("w") as tmp_local_file:
                tmpFile = tmp_local_file.path
                dataset_names = ','.join(smpl for smpl in all_datasets)
                MergerProducer_cmd = ['python3', MergerProducer,'--outFile', tmpFile, '--var', var, '--uncSource', uncName, '--uncConfig', unc_config, '--sampleConfig', sample_config, '--datasetFile', dataset_names,  '--year', getYear(self.period) , '--globalConfig', global_config]#, '--remove-files', 'True']
                MergerProducer_cmd.extend(local_inputs)
                ps_call(MergerProducer_cmd,verbose=1)


class HaddMergedTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)

    def workflow_requires(self):
        return { "Merge" : MergeTask.req(self, branches=()) }

    def requires(self):
        return [MergeTask.req(self, branches=())]

    def create_branch_map(self):
        n = 0
        branches = {}
        for var_entry in self.global_params['vars_to_plot'] :
            var_name, need_cache = parseVarEntry(var_entry)
            branches[n] = var_name
            n += 1
        return branches

    def output(self):
        var = self.branch_data
        output_path = os.path.join(self.period, 'all_histograms', self.version, var, f'all_histograms_{var}_HAdded.root')
        return self.remote_target(output_path, fs=self.fs_histograms)


    def run(self):
        var = self.branch_data
        sample_config = os.path.join(self.ana_path(), 'config',self.period, f'samples.yaml')
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        unc_cfg_dict = load_unc_config(unc_config)
        inDir_allHistograms = os.path.join(self.period, 'all_histograms', self.version, var)
        uncNames = []
        uncNames.extend(list(unc_cfg_dict['norm'].keys()))
        uncNames.extend([unc for unc in unc_cfg_dict['shape']])
        inFileNameCentral = f'all_histograms_{var}_Central.root'
        inFileCentral = os.path.join(inDir_allHistograms,inFileNameCentral)
        all_inputs = []
        all_uncertainties = ['Central']
        for uncName in ['Central']+uncNames :
            if uncName in uncs_to_exclude[self.period]: continue
            inFileName =  f'all_histograms_{var}_{uncName}.root'
            #print(inFileName)
            all_inputs.append(self.remote_target(os.path.join(inDir_allHistograms,inFileName), fs=self.fs_histograms))
            all_uncertainties.append(uncName)
        tmp_outFileName =  f'all_histograms_{var}_tmp.root'

        tmp_outFile = self.remote_target(os.path.join(inDir_allHistograms,tmp_outFileName), fs=self.fs_histograms)
        all_uncertainties_string = ','.join(unc for unc in all_uncertainties)
        HaddMergedHistsProducer = os.path.join(self.ana_path(), 'Analysis', 'hadd_merged_hists.py')
        RenameHistsProducer = os.path.join(self.ana_path(), 'Analysis', 'renameHists.py')
        #ShapeOrLogNormalProducer = os.path.join(self.ana_path(), 'Analysis', 'ShapeOrLogNormal.py')
        #jsonFile_name = os.path.join('jsonFiles', self.period, self.version, 'fitResults',f'slopeInfo_{var}.yaml')
        with contextlib.ExitStack() as stack:
            local_inputs = []
            for inp in all_inputs:
                local_inputs.append(stack.enter_context(inp.localize('r')).path)
            with tmp_outFile.localize("w") as tmpFile:
                HaddMergedHistsProducer_cmd = ['python3', HaddMergedHistsProducer,'--outFile', tmpFile.path, '--var', var]
                HaddMergedHistsProducer_cmd.extend(local_inputs)
                ps_call(HaddMergedHistsProducer_cmd,verbose=1)

        with tmp_outFile.localize("r") as tmpFile, self.output().localize("w") as outFile:
            RenameHistsProducer_cmd = ['python3', RenameHistsProducer,'--inFile', tmpFile.path, '--outFile', outFile.path, '--var', var, '--year', getYear(self.period)]
            ps_call(RenameHistsProducer_cmd,verbose=1)
