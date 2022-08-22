import json
import law
import luigi
import os
import shutil
import yaml

from RunKit.grid_helper_tasks import CreateVomsProxy
from RunKit.sh_tools import sh_call, xrd_copy
from run_tools.law_customizations import Task, HTCondorWorkflow

class BaseTask(Task):
    dataset_tier = luigi.Parameter(default='nanoAOD')
    ignore_missing_samples = luigi.BoolParameter(default=False)

    def create_branch_map(self):
        self.load_sample_configs()
        n = 0
        branches = {}
        missing_samples = []
        for period, samples in self.samples.items():
            for sample_name in sorted(samples.keys()):
                das_dataset = samples[sample_name].get(self.dataset_tier, None)
                if das_dataset is None or len(das_dataset) == 0:
                    missing_samples.append(f'{period}:{sample_name}')
                else:
                    branches[n] = (sample_name, period, das_dataset)
                n += 1
        if len(missing_samples) > 0:
            self.publish_message("Missing samples: {}".format(', '.join(missing_samples)))
            if not self.ignore_missing_samples:
                raise RuntimeError("Missing samples has been detected.")
        return branches

class CreateDatasetInfos(BaseTask, law.LocalWorkflow):
    def workflow_requires(self):
        return { "proxy": CreateVomsProxy.req(self) }

    def requires(self):
        return self.workflow_requires()

    def output(self):
        sample_name, period, das_dataset = self.branch_data
        sample_out = os.path.join(self.central_path(), self.version, f'inputs_{self.dataset_tier}', period,
                                  sample_name + '.json' )
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name, period, das_dataset = self.branch_data
        self.publish_message(f'sameple_name={sample_name}, period={period}, das_dataset={das_dataset}')
        result = {}
        result['das_dataset'] = das_dataset
        result['size'] = 0
        result['nevents'] = 0
        result['files'] = []
        _, output = sh_call(['dasgoclient', '-json', '-query', f'file dataset={das_dataset}'], catch_stdout=True)
        file_entries = json.loads(output)
        for file_entry in file_entries:
            if len(file_entry['file']) != 1:
                raise RuntimeError("Invalid file entry")
            out_entry = {
                'name': file_entry['file'][0]['name'],
                'size': file_entry['file'][0]['size'],
                'nevents': file_entry['file'][0]['nevents'],
                'adler32': file_entry['file'][0]['adler32'],
            }
            result['size'] += out_entry['size']
            result['nevents'] += out_entry['nevents']
            result['files'].append(out_entry)
        self.output().dump(result, indent=2)

class CreateNanoSkims(BaseTask, HTCondorWorkflow, law.LocalWorkflow):

    def workflow_requires(self):
        return {"proxy" : CreateVomsProxy.req(self), "dataset_info": CreateDatasetInfos.req(self, workflow='local') }

    def requires(self):
        return self.workflow_requires()

    def output(self):
        sample_name, period, das_dataset = self.branch_data
        sample_out = os.path.join(self.central_path(), self.version, 'nanoAOD', period, sample_name + '.root' )
        return law.LocalFileTarget(sample_out)

    def run(self):
        sample_name, period, das_dataset = self.branch_data
        sample_config_path = os.path.join(self.central_path(), self.version, f'inputs_{self.dataset_tier}', period,
                                          sample_name + '.json' )
        with open(sample_config_path, 'r') as f:
            sample_config = json.load(f)

        skim_config_path = os.path.join(self.ana_path(), 'config', 'skims.yaml')
        with open(skim_config_path, 'r') as f:
            skim_config = yaml.safe_load(f)
        exclude_columns = ','.join(skim_config['common']['exclude_columns'])
        output_files = []
        os.makedirs(self.local_central_path(), exist_ok=True)
        for n, input_file_entry in enumerate(sample_config['files']):
            input_file_remote = input_file_entry['name']
            adler32 = input_file_entry.get('adler32', None)
            if adler32 is not None:
                adler32 = int(adler32, 16)
            input_file_local = self.local_central_path(f'{self.branch}_{n}_in.root')
            output_file = self.local_central_path(f'{self.branch}_{n}_out.root')
            xrd_copy(input_file_remote, input_file_local, expected_adler32sum=adler32, silent=False)
            sh_call(['skim_tree.py', '--input', input_file_local, '--output', output_file, '--input-tree', 'Events',
                     '--other-trees', 'LuminosityBlocks,Runs',
                     '--exclude-columns', exclude_columns, '--verbose', '1'], verbose=1)
            os.remove(input_file_local)
            output_files.append(output_file)
        output_path = self.output().path
        os.makedirs(self.output().dirname, exist_ok=True)
        if len(output_files) > 1:
            output_tmp = output_path + '.tmp.root'
            sh_call(['haddnano.py', output_tmp] + output_files, verbose=1)
            for f in output_files:
                os.remove(f)
        elif len(output_files) == 1:
            output_tmp = output_files[0]
        else:
            raise RuntimeError("No output files found.")
        shutil.move(output_tmp, output_path)
