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


def getOutFileName(var, sample_name, central_Histograms_path):
    outDir = os.path.join(central_Histograms_path, sample_name)
    fileName = f'{var}.root'
    outFile = os.path.join(outDir,fileName)
    return outFile

class HistProducerFileTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 10.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

    def workflow_requires(self):
        branch_map = self.create_branch_map()
        workflow_dict = {}
        workflow_dict["anaTuple"] = {
            idx: AnaTupleTask.req(self, branch=br, branches=())
            for idx, (sample, br) in branch_map.items() if sample !='data'
        }
        workflow_dict["dataMergeTuple"] = {
            idx: DataMergeTask.req(self, branch=br, branches=())
            for idx, (sample, br) in branch_map.items() if sample =='data'
        }

        return workflow_dict

    def requires(self):
        sample_name, prod_br = self.branch_data
        if sample_name =='data':
            return [ DataMergeTask.req(self,max_runtime=DataMergeTask.max_runtime._default, branch=prod_br, branches=())]
        return [ AnaTupleTask.req(self, branch=prod_br, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        sample_id_data = 0
        for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
            if sample_type =='data' or 'QCD' in sample_name:
                continue
            branches[n] = (sample_name, prod_br)
            n+=1
        branches[n+1] = ('data', 0)
        return branches

    def output(self):
        sample_name, prod_br = self.branch_data
        input_file = self.input()[0].path
        fileName = outFileName = os.path.basename(input_file)
        vars_to_plot = list(hists.keys())
        local_files_target = []
        for var in vars_to_plot:
            outFile_histProdSample = getOutFileName(var, sample_name, self.central_Histograms_path())
            if os.path.exists(outFile_histProdSample):
                local_files_target.append(law.LocalFileTarget(outFile_histProdSample))
                continue
            outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp', var)
            if not os.path.isdir(outDir):
                os.makedirs(outDir)
            outFile = os.path.join(outDir,fileName)
            local_files_target.append(law.LocalFileTarget(outFile))
        return local_files_target

    def run(self):
        sample_name, prod_br = self.branch_data
        if len(self.input()) > 1:
            raise RuntimeError(f"multple input files!! {' '.join(f.path for f in self.input())}")
        input_file = self.input()[0].path
        outFileName = outFileName = os.path.basename(input_file)
        hist_config = self.hist_config
        unc_config = os.path.join(self.ana_path(), 'config', 'weight_definition.yaml')
        sample_config = self.sample_config
        HistProducerFile = os.path.join(self.ana_path(), 'Analysis', 'HistProducerFile.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp')
        HistProducerFile_cmd = ['python3', HistProducerFile,'--inFile', input_file, '--outFileName',outFileName, '--dataset', sample_name, '--outDir', outDir,
                            '--compute_unc_variations', 'True', '--compute_rel_weights', 'True',
                            '--uncConfig', unc_config, '--histConfig', hist_config,'--sampleConfig', sample_config]
        sh_call(HistProducerFile_cmd,verbose=1)


class HistProducerSampleTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 1.0)
    hist_config = os.path.join(os.getenv("ANALYSIS_PATH"), 'config', 'plot','histograms.yaml')
    hists = load_hist_config(hist_config)

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
        histProducerFile_map = HistProducerFileTask.req(self,branch=-1, branches=()).create_branch_map()
        all_samples = {}
        for br_idx, (sample_name, prod_br) in histProducerFile_map.items():
            if sample_name not in all_samples:
                all_samples[sample_name] = []
            all_samples[sample_name].append(br_idx)
        return { n : (sample_name, idx_list) for n, (sample_name, idx_list) in enumerate(all_samples.items()) }

    def output(self):
        sample_name, idx_list = self.branch_data
        vars_to_plot = list(hists.keys())
        local_files_target = []
        for var in vars_to_plot:
            outFile = getOutFileName(var, sample_name, self.central_Histograms_path())
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
        HistProducerSample_cmd = ['python3', HistProducerSample,'--histDir', histDir, '--outDir', outDir, '--hists', hists_str,
                            '--file-name-pattern', file_name_pattern, '--remove-files', 'True']
        if(len(idx_list)>1):
            HistProducerSample_cmd.extend(['--file-ids', file_ids_str])
        #print(HistProducerSample_cmd)
        sh_call(HistProducerSample_cmd,verbose=1)
