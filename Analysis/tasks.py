import law
import luigi
import os
import shutil
import time
import yaml
from RunKit.sh_tools import sh_call
from RunKit.checkRootFile import checkRootFileSafe

from run_tools.law_customizations import Task, HTCondorWorkflow, copy_param, get_param_value

class HistProducerTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 10.0)

    def workflow_requires(self):
        return { "anaTuple" : AnaTupleTask.req(self, branches=())}

    def requires(self):
        sample_name, sample_id = self.branch_data
        return [ AnaTupleTask.req(self, branch=sample_id, max_runtime=AnaTupleTask.max_runtime._default, branches=())]

    def create_branch_map(self):
        n = 0
        branches = {}
        anaProd_branch_map = AnaTupleTask.req(self, branch=-1, branches=()).create_branch_map()
        already_considered_samples = []
        for prod_br, (sample_id, sample_name, sample_type, input_file) in anaProd_branch_map.items():
            if sample_type =='data' or 'QCD' in sample_name: continue
            if sample_name not in already_considered_samples:
                already_considered_samples.append(sample_name)
            else: continue
            branches[n] =  (sample_name, sample_id)
            n+=1
        branches[n+1] = ('data', 0)
        #print(branches)
        return branches

    def output(self):
        if not os.path.isdir(self.central_Histograms_path()):
            os.makedirs(self.central_Histograms_path())
        sample_name, sample_id = self.branch_data
        hist_config = os.path.join(self.ana_path(), 'config', 'plot','histograms.yaml')
        with open(hist_config, 'r') as f:
            hist_cfg_dict = yaml.safe_load(f)
        vars_to_plot = list(hist_cfg_dict.keys())
        fileName = f'{sample_name}.root'
        local_files_target = []
        for var in vars_to_plot:
            if not os.path.isdir( os.path.join(self.central_Histograms_path(),var)):
                os.makedirs(os.path.join(self.central_Histograms_path(),var))
            outFile = os.path.join(self.central_Histograms_path(),var,fileName)
            local_files_target.append(law.LocalFileTarget(outFile))
        return local_files_target

    def run(self):
        sample_name, sample_id = self.branch_data
        print(sample_name)
        hist_config = os.path.join(self.ana_path(), 'config', 'plot','histograms.yaml')
        sample_config = self.sample_config
        with open(hist_config, 'r') as f:
            hist_cfg_dict = yaml.safe_load(f)
        vars_to_plot = list(hist_cfg_dict.keys())
        HistProducer = os.path.join(self.ana_path(), 'Analysis', 'HistProducerSample.py')
        print(' '.join(x.path for x in self.output()))

        HistProducer_cmd = ['python3', HistProducer,'--inputDir', self.central_anaTuples_path(), '--dataset', sample_name, '--histDir', self.central_Histograms_path() ,
                            '--compute_unc_variations', 'True', '--compute_rel_weights', 'True','--histConfig', hist_config,'--sampleConfig', sample_config]
        sh_call(HistProducer_cmd,verbose=1)