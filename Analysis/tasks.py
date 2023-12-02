import law
import luigi
import os
import shutil
import time
import yaml
from RunKit.sh_tools import sh_call
from RunKit.checkRootFile import checkRootFileSafe

from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param, get_param_value
from AnaProd.tasks import AnaTupleTask, DataMergeTask


hists = None
def load_hist_config(hist_config):
    global hists
    with open(hist_config, 'r') as f:
        hists = yaml.safe_load(f)
    return hists
unc_cfg_dict = None
def load_unc_config(unc_cfg):
    global unc_cfg_dict
    with open(unc_cfg, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    return unc_cfg_dict

def get2DOutFileName(var, sample_name, central_Histograms_path, btag_dir, suffix=''):
    outDir = os.path.join(central_Histograms_path, sample_name,btag_dir)
    fileName = f'{var}2D{suffix}.root'
    outFile = os.path.join(outDir,fileName)
    return outFile

def getOutFileName(var, sample_name, central_Histograms_path, btag_dir, suffix=''):
    outDir = os.path.join(central_Histograms_path, var, sample_name,btag_dir)
    fileName = f'{var}{suffix}.root'
    outFile = os.path.join(outDir,fileName)
    return outFile

# **************** 1D HISTOGRAMS *******************
class HistProducerFileTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branch=br, branches=())
            for idx, (sample, br,var) in branch_map.items() if sample !='data' and var=='tau1_pt'
        }
        workflow_dict["dataMergeTuple"] = {
            idx: DataMergeTask.req(self, branch=0, branches=())
            for idx, (sample, br,var) in branch_map.items() if sample =='data'
        }
        return workflow_dict

    def requires(self):
        sample_name, prod_br,var = self.branch_data
        if sample_name =='data':
            return [ DataMergeTask.req(self,max_runtime=DataMergeTask.max_runtime._default, branch=prod_br, branches=())]
        return [ AnaTupleTask.req(self, branch=prod_br, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        k = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        vars_to_plot = ['bbtautau_mass']#list(hists.keys())# ['tau1_pt','tau2_pt','tau1_eta','tau2_eta','SelectedFatJet_pt_boosted','SelectedFatJet_eta_boosted'] #
        #vars_to_plot = ['bbtautau_mass']#['tau1_pt','tau2_pt','tau1_eta','tau2_eta','SelectedFatJet_pt_boosted','SelectedFatJet_eta_boosted'] #
        for var in vars_to_plot:
            for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
                if sample_type =='data' or 'QCD' in sample_name:
                    continue
                branches[n] = (sample_name, prod_br,var)
                n+=1
            branches[n] = ('data', 0, var)
            n+=1
        return branches


    def output(self):
        sample_name, prod_br,var = self.branch_data
        input_file = self.input()[0].path
        fileName  = os.path.basename(input_file)
        vars_to_plot = list(hists.keys())
        local_files_target = []
        outFile_histProdSample = get2DOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir(), '_onlyCentral')
        #if os.path.exists(outFile_histProdSample):
        #    return law.LocalFileTarget(outFile_histProdSample)
        outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp_onlyCentral', var, self.GetBTagDir())
        if not os.path.isdir(outDir):
            os.makedirs(outDir)
        outFile = os.path.join(outDir,fileName)
        return law.LocalFileTarget(outFile)

    def run(self):
        sample_name, prod_br,var = self.branch_data
        if len(self.input()) > 1:
            raise RuntimeError(f"multple input files!! {' '.join(f.path for f in self.input())}")
        input_file = self.input()[0].path
        outFileName = os.path.basename(input_file)
        hist_config = self.hist_config
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistProducerFile.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp')
        HistProducerFile_cmd = ['python3', HistProducerFile,'--inFile', input_file, '--outFileName',outFileName, '--dataset', sample_name, '--outDir', outDir, '--compute_unc_variations', 'True', '--compute_rel_weights', 'True', '--uncConfig', unc_config, '--histConfig', hist_config,'--sampleConfig', sample_config, '--var', var]

        if self.version == 'v7_deepTau2p5':
            HistProducerFile_cmd.extend([ '--deepTauVersion', '2p5'])
        if self.wantBTag:
            HistProducerFile_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        sh_call(HistProducerFile_cmd,verbose=1)


class HistProducerSampleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        self_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["histProducerFile"] = {
            n: HistProducerFileTask.req(self, branches=tuple((idx,) for idx in idx_list))
            for n, (sample_name, idx_list) in self_map.items()
        }
        return workflow_dict

    def requires(self):
        sample_name, idx_list  = self.branch_data
        deps = [HistProducerFileTask.req(self, max_runtime=HistProducerFileTask.max_runtime._default, branch=idx) for idx in idx_list ]
        return deps


    def create_branch_map(self):
        branches = {}
        histProducerFile_map = HistProducerFileTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        for br_idx, (sample_name, prod_br) in histProducerFile_map.items():
            if sample_name not in all_samples:
                all_samples[sample_name] = []
            all_samples[sample_name].append(br_idx)
        for n, (sample_name, idx_list) in enumerate(all_samples.items()):
            branches[n] = (sample_name, idx_list)
        return branches

    def output(self):
        sample_name, idx_list = self.branch_data
        vars_to_plot = list(hists.keys())
        local_files_target = []
        for var in vars_to_plot:
            outFile = getOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir())
            local_files_target.append(law.LocalFileTarget(outFile))
        return local_files_target

    def run(self):
        sample_name, idx_list = self.branch_data
        #print(histProducerFile_map)
        files_idx = []
        vars_to_plot = list(hists.keys())
        hists_str = ','.join(var for var in vars_to_plot)
        file_ids_str = ''
        file_name_pattern = 'nano'
        if(len(idx_list)>1):
            file_name_pattern +="_{id}"
            file_ids_str = f"0-{len(idx_list)}"

        file_name_pattern += ".root"
        HistProducerSample = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name)
        histDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp')
        HistProducerSample_cmd = ['python3', HistProducerSample,'--histDir', histDir, '--outDir', outDir, '--hists', hists_str, '--file-name-pattern', file_name_pattern, '--remove-files', 'True']
        if self.wantBTag:
            HistProducerSample_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        if(len(idx_list)>1):
            HistProducerSample_cmd.extend(['--file-ids', file_ids_str])
        #print(HistProducerSample_cmd)
        sh_call(HistProducerSample_cmd,verbose=1)


class MergeTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)
    unc_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'weight_definition.yaml')
    unc_cfg_dict = load_unc_config(unc_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        prod_branches = HistProducerSampleTask.req(self, branch=-1, branches=()).create_branch_map()
        workflow_dict = {}
        workflow_dict["histProducerSample"] = {
            0: HistProducerSampleTask.req(self, branches=tuple((br,) for br in prod_branches))
        }
        return workflow_dict

    def requires(self):
        prod_branches = HistProducerSampleTask.req(self, branch=-1, branches=()).create_branch_map()
        deps = [HistProducerSampleTask.req(self, max_runtime=HistProducerSampleTask.max_runtime._default, branch=prod_br) for prod_br in prod_branches ]
        return deps


    def create_branch_map(self):
        uncNames = ['Central']
        uncNames.extend(list(unc_cfg_dict['norm'].keys()))
        uncNames.extend([unc for unc in unc_cfg_dict['shape']])
        vars_to_plot = list(hists.keys())
        n = 0
        branches = {}
        for var in vars_to_plot :
            for uncName in uncNames:
                branches[n] = (var, uncName)
                n += 1
        return branches

    def output(self):
        var, uncName = self.branch_data
        outFile_haddMergedFiles = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir(),f'all_histograms_{var}.root')
        if os.path.exists(outFile_haddMergedFiles):
            return law.LocalFileTarget((outFile_haddMergedFiles))
        local_file_target = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir(),f'all_histograms_{var}_{uncName}.root')
        return law.LocalFileTarget(local_file_target)

    def run(self):
        var, uncName = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        vars_to_plot = list(hists.keys())
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger.py')
        MergerProducer_cmd = ['python3', MergerProducer,'--histDir', self.central_Histograms_path(), '--sampleConfig', sample_config, '--hists', var,'--uncConfig', unc_config, '--uncSource', uncName]
        if self.wantBTag:
            MergerProducer_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        sh_call(MergerProducer_cmd,verbose=1)

# ************* CENTRAL ONLY **********************

class HistSampleTaskCentral(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)


    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branches=tuple((br,) for br in branches))
            for idx, (sample, branches,var) in branch_map.items() if sample !='data' and var=='tau1_pt'
        }
        workflow_dict["dataMergeTuple"] = {
            idx: DataMergeTask.req(self, branch=0, branches=())
            for idx, (sample, branches,var) in branch_map.items() if sample =='data' and var=='tau1_pt'
        }
        return workflow_dict

    def requires(self):
        sample, prod_branches,var = self.branch_data
        if sample =='data':
            return [ DataMergeTask.req(self,max_runtime=DataMergeTask.max_runtime._default, branch=0, branches=())]
        return [AnaTupleTask.req(self, max_runtime=AnaTupleTask.max_runtime._default, branch=prod_br) for prod_br in prod_branches ]

    def create_branch_map(self):
        n=0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        vars_to_plot = list(hists.keys())# ['bbtautau_mass']
        for var in vars_to_plot:
            all_samples = {}
            for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
                if sample_type=='data' or 'QCD' in sample_type :continue
                if sample_name not in all_samples:
                    all_samples[sample_name] = {}
                if var not in all_samples[sample_name]:
                    all_samples[sample_name][var] = []
                all_samples[sample_name][var].append(prod_br)
            for sample_name,all_samples_sample in all_samples.items():
                for var, idx_list in all_samples_sample.items():
                    branches[n] = (sample_name, idx_list, var)
                n+=1
            branches[n] = ('data',[0], var)
            n+=1
        return branches


    def output(self):
        sample_name, idx_list,var = self.branch_data
        outDir = os.path.join(self.central_Histograms_path(), sample_name, var, self.GetBTagDir())
        fileName = f'{var}_onlyCentral.root'
        outFile = os.path.join(outDir, fileName)
        return law.LocalFileTarget(outFile)

    def run(self):
        sample_name, prod_branches,var = self.branch_data
        inDir = os.path.join(self.central_anaTuples_path(), sample_name)
        hist_config = self.hist_config
        sample_config = self.sample_config
        outDir = os.path.join(self.central_Histograms_path(), sample_name)
        cacheDir = f'/eos/home-k/kandroso/cms-hh-bbtautau/anaCache/Run2_2018/{sample_name}/{self.version}'
        if sample_name=='data':
            cacheDir+='/data/'
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistSampleCentral.py')
        HistProducerFile_cmd = ['python3', HistProducerFile,'--inDir', inDir, '--cacheDir', cacheDir, '--outDir', outDir, '--dataset', sample_name, '--histConfig', hist_config,'--sampleConfig', sample_config, '--var', var]
        if self.version == 'v7_deepTau2p5':
            HistProducerFile_cmd.extend([ '--deepTauVersion', 'v2p5'])
        if self.wantBTag:
            HistProducerFile_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        sh_call(HistProducerFile_cmd,verbose=1)


class MergeTaskCentral(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)
    unc_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'weight_definition.yaml')
    unc_cfg_dict = load_unc_config(unc_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        workflow_dict = {}
        workflow_dict["histProducerSample"] = {
            0: HistSampleTaskCentral.req(self, branches=())
        }
        return workflow_dict

    def requires(self):
        deps = [HistSampleTaskCentral.req(self, branches=()) ]
        return deps

    def create_branch_map(self):
        vars_to_plot = list(hists.keys())
        n = 0
        branches = {}
        for var in vars_to_plot :
            branches[n] = var
            n += 1
        return branches

    def output(self):
        var = self.branch_data
        outDir = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir())
        outFile = f'all_histograms_{var}_onlyCentral.root'
        json_dir= os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version)
        json_file = f'all_rations_{var}_onlyCentral.json'
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        if not os.path.exists(json_dir):
            os.makedirs(json_dir)
        local_files_target = []
        local_files_target.append(law.LocalFileTarget(os.path.join(outDir,outFile)))
        local_files_target.append(law.LocalFileTarget(os.path.join(json_dir,json_file)))
        return local_files_target

    def run(self):
        var = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        vars_to_plot = list(hists.keys())
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger.py')
        json_dir= os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version)
        MergerProducer_cmd = ['python3', MergerProducer,'--histDir', self.central_Histograms_path(), '--sampleConfig', sample_config, '--var', var,'--uncConfig', unc_config, '--jsonDir', json_dir, '--suffix','_onlyCentral']
        if self.wantBTag:
            MergerProducer_cmd.extend(['--wantBTag','True' ])
        sh_call(MergerProducer_cmd,verbose=1)

class HistSample2DTaskCentral(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)


    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branches=tuple((br,) for br in branches))
            for idx, (sample, branches,var) in branch_map.items() if sample !='data' and var=='tau1_pt'
        }
        workflow_dict["dataMergeTuple"] = {
            idx: DataMergeTask.req(self, branch=0, branches=())
            for idx, (sample, branches,var) in branch_map.items() if sample =='data' and var=='tau1_pt'
        }
        return workflow_dict

    def requires(self):
        sample, prod_branches,var = self.branch_data
        if sample =='data':
            return [ DataMergeTask.req(self,max_runtime=DataMergeTask.max_runtime._default, branch=0, branches=())]
        return [AnaTupleTask.req(self, max_runtime=AnaTupleTask.max_runtime._default, branch=prod_br) for prod_br in prod_branches ]

    def create_branch_map(self):
        n=0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        vars_to_plot = list(hists.keys())# ['bbtautau_mass']
        for var in vars_to_plot:
            all_samples = {}
            for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
                if sample_type=='data' or 'QCD' in sample_type :continue
                if sample_name not in all_samples:
                    all_samples[sample_name] = {}
                if var not in all_samples[sample_name]:
                    all_samples[sample_name][var] = []
                all_samples[sample_name][var].append(prod_br)
            for sample_name,all_samples_sample in all_samples.items():
                for var, idx_list in all_samples_sample.items():
                    branches[n] = (sample_name, idx_list, var)
                n+=1
            branches[n] = ('data',[0], var)
            n+=1
        return branches


    def output(self):
        sample_name, idx_list,var = self.branch_data
        outFile = getOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir(),'_onlyCentral')
        return law.LocalFileTarget(outFile)

    def run(self):
        sample_name, prod_branches,var = self.branch_data
        inDir = os.path.join(self.central_anaTuples_path(), sample_name)
        hist_config = self.hist_config
        sample_config = self.sample_config
        outDir = os.path.join(self.central_Histograms_path(), sample_name)

        cacheDir = f'/eos/home-k/kandroso/cms-hh-bbtautau/anaCache/Run2_2018/{sample_name}/{self.version}'
        if sample_name=='data':
            cacheDir+='/data/'
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistSampleCentral.py')
        HistProducerFile_cmd = ['python3', HistProducerFile,'--inDir', inDir, '--cacheDir', cacheDir, '--outDir', outDir, '--dataset', sample_name, '--histConfig', hist_config,'--sampleConfig', sample_config, '--var', var,'--want2D','True']
        if self.version == 'v7_deepTau2p5':
            HistProducerFile_cmd.extend([ '--deepTauVersion', 'v2p5'])
        sh_call(HistProducerFile_cmd,verbose=1)


class Merge2DTaskCentral(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)
    unc_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'weight_definition.yaml')
    unc_cfg_dict = load_unc_config(unc_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        workflow_dict = {}
        workflow_dict["histProducerSample"] = {
            0: HistSampleTaskCentral.req(self, branches=())
        }
        return workflow_dict

    def requires(self):
        deps = [HistSampleTaskCentral.req(self, branches=()) ]
        return deps

    def create_branch_map(self):
        vars_to_plot = list(hists.keys())
        n = 0
        branches = {}
        for var in vars_to_plot :
            branches[n] = var
            n += 1
        return branches

    def output(self):
        var = self.branch_data
        outDir = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir())
        outFile = f'all_histograms_{var}_onlyCentral.root'
        json_dir= os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version)
        json_file = f'all_rations_{var}_onlyCentral.json'
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        if not os.path.exists(json_dir):
            os.makedirs(json_dir)
        local_files_target = []
        local_files_target.append(law.LocalFileTarget(os.path.join(outDir,outFile)))
        local_files_target.append(law.LocalFileTarget(os.path.join(json_dir,json_file)))
        return local_files_target

    def run(self):
        var = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        vars_to_plot = list(hists.keys())
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger.py')
        json_dir= os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version)
        MergerProducer_cmd = ['python3', MergerProducer,'--histDir', self.central_Histograms_path(), '--sampleConfig', sample_config, '--var', var,'--uncConfig', unc_config, '--jsonDir', json_dir, '--suffix','_onlyCentral', '--want2D','True' ]
        sh_call(MergerProducer_cmd,verbose=1)

'''
class PlotterTaskCentral(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        return { "Merge2DTaskCentral" : Merge2DTaskCentral.req(self, branches=()) }

    def requires(self):
        return [Merge2DTaskCentral.req(self, branches=())]

    def create_branch_map(self):
        vars_to_plot = list(hists.keys())
        sample_cfg_dict = self.sample_config
        categories = list(self.global_params['categories'])
        QCD_region = 'OS_Iso'
        uncName = 'Central'
        channels = list(self.global_params['channelSelection'])
        n = 0
        branches = {}
        for var in vars_to_plot:
            for channel in channels:
                for category in categories:
                    branches[n] = (var,channel, QCD_region, category, uncName)
                    n+=1
        return branches

    def output(self):
        var,channel, QCD_region, category, uncName = self.branch_data
        final_directory = os.path.join(self.central_Histograms_path(), 'plots', var, self.GetBTagDir())
        final_fileName = f'{channel}_{QCD_region}_{category}_XMass{self.mass}.pdf'
        final_file= os.path.join(final_directory, final_fileName)
        return law.LocalFileTarget(final_file)

    def run(self):
        var,channel, QCD_region, category, uncName = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        PlotterProducer = os.path.join(self.ana_path(), 'Analysis', 'HistPlotter.py')
        outDir = os.path.join(self.central_Histograms_path(), 'plots')
        PlotterProducer_cmd = ['python3', PlotterProducer,'--mass', self.mass, '--histDir', self.central_Histograms_path(), '--outDir',outDir, '--inFileName', 'all_histograms', '--var', var, '--sampleConfig',sample_config, '--channel', channel, '--category', category, '--uncSource', uncName, '--suffix','_onlyCentral']
        sh_call(PlotterProducer_cmd,verbose=1)
        '''
# **************** 2D HISTOGRAMS *******************

class HistProducerFile2DTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branch=br, branches=())
            for idx, (sample, br,var) in branch_map.items() if sample !='data' and var=='tau1_pt'
        }
        workflow_dict["dataMergeTuple"] = {
            idx: DataMergeTask.req(self, branch=0, branches=())
            for idx, (sample, br,var) in branch_map.items() if sample =='data'
        }
        return workflow_dict

    def requires(self):
        sample_name, prod_br,var = self.branch_data
        if sample_name =='data':
            return [ DataMergeTask.req(self,max_runtime=DataMergeTask.max_runtime._default, branch=prod_br, branches=())]
        return [ AnaTupleTask.req(self, branch=prod_br, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        k = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        vars_to_plot = list(hists.keys())
        for var in vars_to_plot:
            for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
                if sample_type =='data' or 'QCD' in sample_name:
                    continue
                branches[n] = (sample_name, prod_br,var)
                n+=1
            branches[n] = ('data', 0, var)
            n+=1
        return branches

    def output(self):
        sample_name, prod_br,var = self.branch_data
        input_file = self.input()[0].path
        fileName_split = os.path.basename(input_file).split('.')
        if len(fileName_split)>2: print(f"fileName_split is {fileName_split}")
        fileName_split[0]+='2D'
        fileName=fileName_split[0]+'.'+fileName_split[1]
        outFile_histProdSample = get2DOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir())
        if os.path.exists(outFile_histProdSample):
            return law.LocalFileTarget(outFile_histProdSample)
        outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp2D', var, self.GetBTagDir())
        if not os.path.isdir(outDir):
            os.makedirs(outDir)
        outFile = os.path.join(outDir,fileName)
        return law.LocalFileTarget(outFile)

    def run(self):
        #sample_name, prod_br,var = self.branch_data
        #print(get2DOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir()))
        #print(os.path.exists(outFile_histProdSample))
        #print(f"outFile is {self.output().path}")
        sample_name, prod_br,var = self.branch_data
        if len(self.input()) > 1:
            raise RuntimeError(f"multple input files!! {' '.join(f.path for f in self.input())}")
        input_file = self.input()[0].path
        outFileName_split = os.path.basename(input_file).split('.')
        if len(outFileName_split)>2: print(f"outFileName_split is {outFileName_split}")
        outFileName_split[0]+='2D'
        outFileName=outFileName_split[0]+'.'+outFileName_split[1]
        hist_config = self.hist_config
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistProducerFile.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp2D')
        HistProducerFile_cmd = ['python3', HistProducerFile,'--inFile', input_file, '--outFileName',outFileName, '--dataset', sample_name, '--outDir', outDir, '--compute_unc_variations', 'True', '--compute_rel_weights', 'True', '--uncConfig', unc_config, '--histConfig', hist_config,'--sampleConfig', sample_config, '--var', var, '--want2D', 'True']
        if self.version == 'v7_deepTau2p5':
            HistProducerFile_cmd.extend([ '--deepTauVersion', '2p5'])
        if self.wantBTag:
            HistProducerFile_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        sh_call(HistProducerFile_cmd,verbose=1)


class HistProducerSample2DTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        self_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["histProducerFile"] = {
            n: HistProducerFile2DTask.req(self, branches=tuple((idx,) for idx in idx_list))
            for n, (sample_name, idx_list, var) in self_map.items()
        }
        return workflow_dict

    def requires(self):
        sample_name, idx_list,var = self.branch_data
        deps = [HistProducerFile2DTask.req(self, max_runtime=HistProducerFile2DTask.max_runtime._default, branch=idx) for idx in idx_list ]
        return deps


    def create_branch_map(self):
        branches = {}
        histProducerFile_map = HistProducerFile2DTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        n=0
        for br_idx, (sample_name, prod_br,var) in histProducerFile_map.items():
            if sample_name not in all_samples:
                all_samples[sample_name] = {}
            if var not in all_samples[sample_name]:
                all_samples[sample_name][var] = []
            all_samples[sample_name][var].append(br_idx)
        for sample_name,all_samples_sample in all_samples.items():
            for var, idx_list in all_samples_sample.items():
                branches[n] = (sample_name, idx_list, var)
                n+=1
        return branches

    def output(self):
        sample_name, idx_list,var = self.branch_data
        outFile = get2DOutFileName(var, sample_name, self.central_Histograms_path(), self.GetBTagDir())
        return law.LocalFileTarget(outFile)

    def run(self):
        sample_name, idx_list, var = self.branch_data
        #print(histProducerFile_map)
        files_idx = []
        file_ids_str = ''
        file_name_pattern = 'nano'
        if(len(idx_list)>1):
            file_name_pattern +="_{id}"
            file_ids_str = f"0-{len(idx_list)}"

        file_name_pattern += "2D.root"
        HistProducerSample = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample2D.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name)
        histDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp2D')
        HistProducerSample_cmd = ['python3', HistProducerSample,'--histDir', histDir, '--outDir', outDir, '--hists', var, '--file-name-pattern', file_name_pattern]
        if self.wantBTag:
            HistProducerSample_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        if(len(idx_list)>1):
            HistProducerSample_cmd.extend(['--file-ids', file_ids_str])
        #print(HistProducerSample_cmd)
        sh_call(HistProducerSample_cmd,verbose=1)


class Merge2DTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 30.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)
    unc_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'weight_definition.yaml')
    unc_cfg_dict = load_unc_config(unc_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        workflow_dict = {}
        workflow_dict["histProducerSample"] = {
            0: HistProducerSample2DTask.req(self, branches=())
        }
        return workflow_dict

    def requires(self):
        deps = [HistProducerSample2DTask.req(self, branches=()) ]
        return deps


    def create_branch_map(self):
        uncNames = ['Central']
        uncNames.extend(list(unc_cfg_dict['norm'].keys()))
        uncNames.extend([unc for unc in unc_cfg_dict['shape']])
        vars_to_plot = list(hists.keys())
        n = 0
        branches = {}
        for var in vars_to_plot :
            for uncName in uncNames:
                branches[n] = (var, uncName)
                n += 1
        return branches

    def output(self):
        var, uncName = self.branch_data
        outFile_haddMergedFiles = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir(),f'all_histograms_{var}.root')
        if os.path.exists(outFile_haddMergedFiles):
            return law.LocalFileTarget((outFile_haddMergedFiles))
        local_file_target = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir(),f'all_histograms_{var}_{uncName}.root')
        return law.LocalFileTarget(local_file_target)

    def run(self):
        var, uncName = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        MergerProducer = os.path.join(self.ana_path(), 'Analysis', 'HistMerger2D.py')
        json_dir= os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version)
        MergerProducer_cmd = ['python3', MergerProducer,'--histDir', self.central_Histograms_path(), '--sampleConfig', sample_config, '--var', var,'--uncConfig', unc_config, '--jsonDir', json_dir, '--uncSource', uncName ]
        if self.wantBTag:
            MergerProducer_cmd.extend([ '--wantBTag', f'{self.wantBTag}'])
        #if var in ['tau2_pt', 'bbtautau_mass', 'b2_pt', 'tau1_eta', 'tau2_eta', 'bb_m_vis', 'tau1_pt', 'b1_pt','bbtautau_mass']:
        sh_call(MergerProducer_cmd,verbose=1)


# ***************************************

class HaddMergedTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        return { "Merge" : MergeTask.req(self, branches=()) }

    def requires(self):
        return [MergeTask.req(self, branches=())]

    def create_branch_map(self):
        vars_to_plot = list(hists.keys())
        n = 0
        branches = {}
        for var in vars_to_plot :
            branches[n] = var
            n += 1
        return branches

    def output(self):
        var = self.branch_data
        local_file_target = os.path.join(self.central_Histograms_path(), 'all_histograms',var,self.GetBTagDir(),f'all_histograms_{var}.root')
        return law.LocalFileTarget(local_file_target)

    def run(self):
        var = self.branch_data
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        outDir_json = os.path.join(self.valeos_path(), 'jsonFiles', self.period, self.version,'fitResults')
        histDir = os.path.join(self.central_Histograms_path(),'all_histograms',var, self.GetBTagDir())
        ShapeOrLogNormalProducer = os.path.join(self.ana_path(), 'Analysis', 'ShapeOrLogNormal.py')
        ShapeOrLogNormal_cmd = ['python3', ShapeOrLogNormalProducer,'--mass', self.mass, '--histDir', histDir, '--inFileName', 'all_histograms','--sampleConfig', self.sample_config,'--uncConfig', unc_config,'--var', var]
        sh_call(HaddMergedHistsProducer_cmd,verbose=1)
        final_json_file = os.path.join(outDir_json, var, self.GetBTagDir(), 'slopeInfo.json')
        HaddMergedHistsProducer = os.path.join(self.ana_path(), 'Analysis', 'hadd_merged_hists.py')
        HaddMergedHistsProducer_cmd = ['python3', HaddMergedHistsProducer,'--histDir', histDir, '--file-name-pattern', 'all_histograms', '--hists', var,
                            '--uncConfig', unc_config]
        if os.path.exists(final_json_file):
            HaddMergedHistsProducer_cmd.extend(['--remove-files', 'True'])
        sh_call(HaddMergedHistsProducer_cmd,verbose=1)

'''
class PlotterTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def GetBTagDir(self):
        return "bTag_WP" if self.wantBTag else "bTag_shape"


    def workflow_requires(self):
        return { "HaddMerged" : HaddMergedTask.req(self, branches=()) }

    def requires(self):
        return [HaddMergedTask.req(self, branches=())]

    def create_branch_map(self):
        uncNames = ['Central']
        uncNames.extend(list(unc_cfg_dict['norm'].keys()))
        uncNames.extend([unc for unc in unc_cfg_dict['shape']])
        sample_cfg_dict = self.sample_config
        categories = list(self.global_params['categories'])
        QCD_region = 'OS_Iso'
        channels = list(self.global_params['channelSelection'])
        n = 0
        branches = {}
        for uncName in uncNames:
            for channel in channels:
                for category in categories:
                    branches[n] = (channel, QCD_region, category, uncName)
                    n+=1
        return branches

    def output(self):
        channel, QCD_region, category, uncName = self.branch_data
        vars_to_plot = list(hists.keys())
        final_files = []
        for var in vars_to_plot:
            for uncScale in ['Central','Up','Down']:
                if uncName == 'Central' and uncScale != 'Central': continue
                if uncName != 'Central' and uncScale == 'Central': continue
                final_directory = os.path.join(self.central_Histograms_path(), 'plots', var)
                final_fileName = f'{channel}_{QCD_region}_{category}_{uncName}_{uncScale}_XMass{self.mass}.pdf'
                final_files.append(law.LocalFileTarget(os.path.join(final_directory, final_fileName)))
        return law.LocalDirectoryTarget(final_files)

    def run(self):
        channel, QCD_region, category, uncName = self.branch_data
        print(channel, QCD_region, category, uncName)
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        vars_to_plot = list(hists.keys())
        hists_str = ','.join(var for var in vars_to_plot)
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        PlotterProducer = os.path.join(self.ana_path(), 'Analysis', 'HistPlotter.py')
        for var in vars_to_plot:
            outDir = os.path.join(self.central_Histograms_path(), 'plots')
            PlotterProducer_cmd = ['python3', PlotterProducer,'--mass', self.mass, '--histDir', self.central_Histograms_path(),
                                    '--outDir',outDir, '--inFileName', 'all_histograms', '--hists', var, '--sampleConfig',sample_config,
                                    '--channel', channel, '--category', category, '--uncSource', uncName]
            sh_call(PlotterProducer_cmd,verbose=1)
        '''