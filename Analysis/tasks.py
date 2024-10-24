import law
import os
import yaml
import contextlib

from RunKit.run_tools import ps_call
from RunKit.crabLaw import cond as kInit_cond, update_kinit_thread
from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param
from AnaProd.tasks import AnaTupleTask, DataMergeTask, AnaCacheTupleTask, DataCacheMergeTask

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
        'Run3_2022':'2022',
        'Run3_2022EE':'2022EE',
        'Run3_2023':'2023',
        'Run3_2023BPix':'2023BPix',
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
        need_data = False
        need_data_cache = False
        branch_set = set()
        branch_set_cache = set()
        for idx, (sample, br, var, need_cache) in self.branch_map.items():
            if sample == 'data':
                need_data = True
                if need_cache:
                    need_data_cache = True
            else:
                branch_set.add(br)
                if need_cache:
                    branch_set_cache.add(br)
        reqs = {}
        if len(branch_set) > 0:
            reqs['anaTuple'] = AnaTupleTask.req(self, branches=tuple(branch_set))
        if len(branch_set_cache) > 0:
            reqs['anaCacheTuple'] = AnaCacheTupleTask.req(self, branches=tuple(branch_set_cache))
        if need_data:
            reqs['dataMergeTuple'] = DataMergeTask.req(self, branches=())
        if need_data_cache:
            reqs['dataCacheMergeTuple'] = DataCacheMergeTask.req(self, branches=())
        return reqs

    def requires(self):
        sample_name, prod_br, var, need_cache = self.branch_data
        deps = []
        if sample_name =='data':
            deps.append(DataMergeTask.req(self, max_runtime=DataMergeTask.max_runtime._default, branch=prod_br,
                                          branches=(prod_br,)))
            if need_cache:
                deps.append(DataCacheMergeTask.req(self, max_runtime=DataCacheMergeTask.max_runtime._default,
                                                   branch=prod_br, branches=(prod_br,)))
        else:
            deps.append(AnaTupleTask.req(self, max_runtime=AnaTupleTask.max_runtime._default, branch=prod_br,
                                         branches=(prod_br,)))
            if need_cache:
                deps.append(AnaCacheTupleTask.req(self, max_runtime=AnaCacheTupleTask.max_runtime._default,
                                                  branch=prod_br, branches=(prod_br,)))
        return deps

    def create_branch_map(self):
        n = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds,self.global_params['signal_types'] )
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
        sample_name, prod_br, var, need_cache = self.branch_data
        outFileName = os.path.basename(self.input()[0].path)
        output_path = os.path.join(self.version, self.period, 'prod', var, f'{sample_name}_{outFileName}')
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        sample_name, prod_br, var, need_cache = self.branch_data
        input_file = self.input()[0]
        print(f'input file is {input_file.path}')
        global_config = os.path.join(self.ana_path(), 'config','HH_bbtautau', f'global.yaml')
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        sample_type = self.samples[sample_name]['sampleType'] if sample_name != 'data' else 'data'
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistProducerFile.py')
        print(f'output file is {self.output().path}')
        with input_file.localize("r") as local_input, self.output().localize("w") as local_output:
            print(local_output.path)

            HistProducerFile_cmd = [ 'python3', HistProducerFile,
                                    '--inFile', local_input.path, '--outFileName',local_output.path,
                                    '--dataset', sample_name, '--uncConfig', unc_config,
                                    '--histConfig', self.setup.hist_config_path, '--sampleType', sample_type,
                                    '--globalConfig', global_config, '--var', var, '--period', self.period  ]
            if self.global_params['compute_unc_histograms'] or var == 'kinFit_m':
                HistProducerFile_cmd.extend(['--compute_rel_weights', 'True', '--compute_unc_variations', 'True'])
            if 'deepTau2p5' in self.version.split('_'):
                print("deepTau2p5 in use")
                HistProducerFile_cmd.extend([ '--deepTauVersion', 'v2p5'])
            if need_cache:
                anaCache_file = self.input()[1]
                with anaCache_file.localize("r") as local_anacache:
                    HistProducerFile_cmd.extend(['--cacheFile', local_anacache.path])
                    ps_call(HistProducerFile_cmd, verbose=1)
            else:
                ps_call(HistProducerFile_cmd, verbose=1)

class HistProducerSampleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 2.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)

    def workflow_requires(self):
        branch_set = set()
        for br_idx, (sample_name, dep_br_list, var) in self.branch_map.items():
            branch_set.update(dep_br_list)
        branches = tuple(branch_set)
        return { "HistProducerFileTask": HistProducerFileTask.req(self, branches=branches) }

    def requires(self):
        sample_name, dep_br_list, var = self.branch_data
        return [
            HistProducerFileTask.req(self, max_runtime=HistProducerFileTask.max_runtime._default,
                                                 branch=dep_br, branches=(dep_br,))
            for dep_br in dep_br_list
        ]

    def create_branch_map(self):
        branches = {}
        histProducerFile_map = HistProducerFileTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds,self.global_params['signal_types'] )
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
        output_path = os.path.join(self.version, self.period, 'split', var, f'{sample_name}.root')
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        sample_name, idx_list, var  = self.branch_data
        HistProducerSample = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample.py')
        with contextlib.ExitStack() as stack:
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
        var, branches_idx = self.branch_data
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
        for n, key in enumerate(all_samples.items()):
            var, branches_idx = key
            branches[k] = (var, branches_idx)
            k+=1
        return branches

    def output(self):
        var, branches_idx = self.branch_data
        output_path = os.path.join(self.version, self.period, 'merged', var, f'{var}.root')
        return self.remote_target(output_path,  fs=self.fs_histograms)

    def run(self):
        var, branches_idx = self.branch_data
        sample_config = os.path.join(self.ana_path(), 'config',self.period, f'samples.yaml')
        global_config = os.path.join(self.ana_path(), 'config','HH_bbtautau', f'global.yaml')
        unc_config = os.path.join(self.ana_path(), 'config',self.period, f'weights.yaml')
        uncNames = ['Central']
        unc_cfg_dict = load_unc_config(unc_config)
        uncs_to_exclude = self.global_params['uncs_to_exclude'][self.period]
        print(uncs_to_exclude)
        if self.global_params['compute_unc_histograms']:
            for uncName in list(unc_cfg_dict['norm'].keys())+unc_cfg_dict['shape']:
                if uncName in uncs_to_exclude: continue
                uncNames.append(uncName)
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger.py')
        HaddMergedHistsProducer = os.path.join(self.ana_path(), 'Analysis', 'hadd_merged_hists.py')
        RenameHistsProducer = os.path.join(self.ana_path(), 'Analysis', 'renameHists.py')

        output_path_hist_prod_sample_data = os.path.join(self.version, self.period, 'split', var, f'data.root')
        all_inputs = [(self.remote_target(output_path_hist_prod_sample_data, fs=self.fs_histograms),'data')]
        samples_to_consider = GetSamples(self.samples, self.setup.backgrounds,self.global_params['signal_types'] )
        print(samples_to_consider)
        for sample_name in self.samples.keys():
            if sample_name not in samples_to_consider: continue
            print(sample_name)
            output_path_hist_prod_sample = os.path.join(self.version, self.period, 'split', var, f'{sample_name}.root')
            all_inputs.append((self.remote_target(output_path_hist_prod_sample, fs=self.fs_histograms),sample_name))
        print(all_inputs)
        all_datasets=[]
        all_outputs_merged = []

        outdir_histograms = os.path.join(self.version, self.period, 'merged', var, 'tmp')

        with contextlib.ExitStack() as stack:
            local_inputs = []
            for inp, smpl in all_inputs:
                local_inputs.append(stack.enter_context(inp.localize('r')).path)
                all_datasets.append(smpl)
            for uncName in uncNames:
                final_histname = f'all_histograms_{var}_{uncName}.root'
                tmp_outfile_merge = os.path.join(outdir_histograms,final_histname)
                tmp_outfile_merge_remote = self.remote_target(tmp_outfile_merge, fs=self.fs_histograms)
                all_outputs_merged.append(tmp_outfile_merge_remote)
                dataset_names = ','.join(smpl for smpl in all_datasets)
                with tmp_outfile_merge_remote.localize("w") as tmp_outfile_merge_unc:
                    MergerProducer_cmd = ['python3', MergerProducer,'--outFile', tmp_outfile_merge_unc.path, '--var', var, '--uncSource', uncName, '--uncConfig', unc_config, '--sampleConfig', sample_config, '--datasetFile', dataset_names,  '--year', getYear(self.period) , '--globalConfig', global_config]#, '--remove-files', 'True']
                    MergerProducer_cmd.extend(local_inputs)
                    ps_call(MergerProducer_cmd,verbose=1)
        all_uncertainties_string = ','.join(unc for unc in uncNames)
        tmp_outFile = self.remote_target( os.path.join(outdir_histograms,f'all_histograms_{var}_hadded.root'), fs=self.fs_histograms)
        print(all_outputs_merged)
        with contextlib.ExitStack() as stack:
            local_merged_files = []
            for infile_merged in all_outputs_merged:
                local_merged_files.append(stack.enter_context(infile_merged.localize('r')).path)
            with tmp_outFile.localize("w") as tmpFile:
                HaddMergedHistsProducer_cmd = ['python3', HaddMergedHistsProducer,'--outFile', tmpFile.path, '--var', var]
                HaddMergedHistsProducer_cmd.extend(local_merged_files)
                ps_call(HaddMergedHistsProducer_cmd,verbose=1)
        with tmp_outFile.localize("r") as tmpFile, self.output().localize("w") as outFile:
            RenameHistsProducer_cmd = ['python3', RenameHistsProducer,'--inFile', tmpFile.path, '--outFile', outFile.path, '--var', var, '--year', getYear(self.period)]
            ps_call(RenameHistsProducer_cmd,verbose=1)


