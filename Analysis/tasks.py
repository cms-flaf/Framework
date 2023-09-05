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

class HistProducerFileTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 10.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 1)

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
        vars_to_plot = list(self.hists.keys())
        local_files_target = []

        histProducerSample_map = HistProducerSampleTask.req(self, branch=-1, branches=()).create_branch_map()
        branch = 0
        for br_idx, (smpl_name, idx_list) in histProducerSample_map:
            if smpl_name != sample_name: continue
            branch = br_idx
        output_HistProducerSampleTask = HistProducerSampleTask.req(self, branch=branch, branches=()).output()
        for foutput in output_HistProducerSampleTask:
            if os.path.exists(foutput):
                local_files_target.append(foutput)
        if local_files_target: return local_files_target
        for var in vars_to_plot:
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

    def workflow_requires(self):
        return { "histProducerFile" : HistProducerFileTask.req(self, branches=())}

    def requires(self):
        histProducerFile_map = HistProducerFileTask.req(self, branches=()).create_branch_map()
        sample_name = self.branch_data
        reqs = []
        for br_idx, (smpl_name, prod_br) in histProducerFile_map.items():
            if smpl_name == sample_name:
                reqs.append( HistProducerFileTask.req(self, branch=br_idx, max_runtime=HistProducerFileTask.max_runtime._default, branches=()))
        return reqs


    def create_branch_map(self):
        n = 0
        branches = {}
        already_considered_samples = []
        histProducerFile_map = HistProducerFileTask.req(self, branches=()).create_branch_map()
        samples = {}
        for br_idx, (sample_name, prod_br) in histProducerFile_map.items():
            if sample_name not in samples:
                samples[sample_name] = []
            samples[sample_name].append(br_idx)
        return { n : (sample_name, idx_list) for n, (sample_name, idx_list) in enumerate(samples.items()) }

    def output(self):
        sample_name, idx_list = self.branch_data
        vars_to_plot = list(self.hists.keys())
        local_files_target = []
        for var in vars_to_plot:
            fileName = f'{var}.root'
            outDir = os.path.join(self.central_Histograms_path(), sample_name)
            if not os.path.isdir(outDir):
                os.makedirs(outDir)
            outFile = os.path.join(outDir,fileName)
            local_files_target.append(law.LocalFileTarget(outFile))
        return local_files_target

    def run(self):
        sample_name, idx_list = self.branch_data
        histProducerFile_map = HistProducerFileTask.req(self, branches=()).branch_data
        files_idx = []
        vars_to_plot = list(self.hists.keys())
        hists_str = ','.join(var for var in vars_to_plot)
        file_ids_str = ''
        file_name_pattern = 'nano'
        for br_idx, (smpl_name, prod_br) in histProducerFile_map.items():
            if smpl_name == sample_name:
                files_idx.append(br_idx)
        if(len(files_idx)>1):
            file_name_pattern +="_{id}"
            file_ids_str = f"{files_idx[0]}-{files_idx[-1]}"

        file_name_pattern += ".root"
        HistProducerSample = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample.py')
        outDir = os.path.join(self.central_Histograms_path(), sample_name)
        histDir = os.path.join(self.central_Histograms_path(), sample_name, 'tmp')
        HistProducerSample_cmd = ['python3', HistProducerSample,'--histDir', histDir, '--outDir', outDir, '--hists', hists_str,
                            '--file-name-pattern', file_name_pattern, '--remove-files', 'True']
        if(len(files_idx)>1):
            HistProducerSample_cmd.extend(['--file-ids', file_ids_str])
        sh_call(HistProducerSample_cmd,verbose=1)

