import datetime
import json
import os
import sys
import yaml
import ROOT
import importlib



if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

def computeAnaCache(file_lists, global_params, generator, sampleType, stitch_config=None, range=None):
    from Corrections.Corrections import Corrections
    from Corrections.CorrectionsCore import central, getScales, getSystName
    from Corrections.pu import puWeightProducer

    start_time = datetime.datetime.now()
    Corrections.initializeGlobal(global_params, sample_name=None, isData=False, load_corr_lib=True)

    stitch_name = f"{sampleType}_{generator}"
    do_stitching = stitch_config['do_stitching'] if stitch_config and (stitch_name in stitch_config) else False

    anaCache = { 'denominator': {}, 'differential_denominator': {} } if do_stitching else { 'denominator': {} }
    sources = [ central ]
    if 'pu' in Corrections.getGlobal().to_apply:
        sources += puWeightProducer.uncSource


    for tree, file_list in file_lists.items():
        df = ROOT.RDataFrame(tree, file_list)
        # We have opened tree here, so we want to only read once
        # This means we need to do our binned 'stitching' cache in this loop too
        # We want to pass a 'module' or python file for which stitching code to load and use
        # Started a sub folder DatasetStitching, will start with DY stitching
        # importlib.import(SpecialName)


        if do_stitching:
            module_subfolder = "DatasetStitching"
            module_name = "DY_Stitching"
            full_module_name = ".".join([module_subfolder, module_name])
            stitching_module = importlib.import_module(full_module_name)

        if range is not None:
            df = df.Range(range)
        df, syst_names = Corrections.getGlobal().getDenominator(df, sources, generator)
        for source in sources:
            if source not in anaCache['denominator']:
                anaCache['denominator'][source]={}
            for scale in getScales(source):
                syst_name = getSystName(source, scale)
                anaCache['denominator'][source][scale] = anaCache['denominator'][source].get(scale, 0.) \
                    + df.Sum(f'weight_denom_{syst_name}').GetValue()
        anaCache['denominator']['sum_genWeight'] = anaCache['denominator'].get('sum_genWeight', 0.) + df.Sum(f'genWeight').GetValue()
        df = df.Define('genWeight2', '(genWeight)*(genWeight)')
        anaCache['denominator']['sum_genWeight2'] = anaCache['denominator'].get('sum_genWeight2', 0.) + df.Sum(f'genWeight2').GetValue()
        


        # Now we repeat this "anaCache['denominator']" dict loop with our new dict
        # "anaCache['differential_denominator']"
        # and each bin will have its own copy of the central/systs values
        if do_stitching:
            this_stitch_config = stitch_config[stitch_name]
            anaCache = stitching_module.GetStitchDict(df, anaCache, this_stitch_config, sources)
            #The config will have different binning for different generators (only use amcatnlo for now)

            #Then finally the cache dict should also have information on what the bins were (human readable) using LHE_Vpt for pT and LHE_NpNLO for nJets (0, 1, 2+)
    


    end_time = datetime.datetime.now()
    anaCache['runtime'] = (end_time - start_time).total_seconds()
    return anaCache


def create_filelists(input_files, keys=['Events', 'EventsNotSelected']):
    file_lists = {}
    for input_file in input_files:
        with ROOT.TFile.Open(input_file) as tmp_file:
            for key in keys:
                if key in tmp_file.GetListOfKeys():
                    if key not in file_lists:
                        file_lists[key] = []
                    file_lists[key].append(input_file)
    return file_lists


def addAnaCaches(*anaCaches):
    #anaCacheSum = { 'denominator': {}, 'runtime': 0.}

    do_stitching = 'differential_denominator' in anaCaches[0] #If the stitching dict exists in the first cache, it should exist in all caches
    anaCacheSum = { 'denominator': {}, 'differential_denominator': {}, 'runtime': 0. } if do_stitching else { 'denominator': {}, 'runtime': 0. }
    for idx, anaCache in enumerate(anaCaches):
        for source, source_entry in anaCache['denominator'].items():
            if source in ["sum_genWeight", "sum_genWeight2"]:
                if source not in anaCacheSum['denominator']:
                    if idx == 0:
                        anaCacheSum['denominator'][source] = 0.
                    else:
                        raise RuntimeError(f"addAnaCaches: {source} not found in first cache")
                print(f"What is source_entry? {source}/{source_entry}", file=sys.stderr)
                anaCacheSum['denominator'][source] += source_entry
                continue #Since the genWeights are not dicts, the .items() logic would break, this is a catch for those two now

            if source not in anaCacheSum['denominator']:
                if idx == 0:
                    anaCacheSum['denominator'][source] = {}
                else:
                    raise RuntimeError(f"addAnaCaches: source {source} not found in first cache")

            for scale, value in source_entry.items():
                if scale not in anaCacheSum['denominator'][source]:
                    if idx == 0:
                        anaCacheSum['denominator'][source][scale] = 0.
                    else:
                        raise RuntimeError(f"addAnaCaches: {source}/{scale} not found in first cache")
                anaCacheSum['denominator'][source][scale] += value

        if do_stitching:
            if 'differential_denominator' not in anaCache:
                raise RuntmieError(f"addAnaCaches: differential denominator not found in later cache even after being found in first cache")
            for thisBin in anaCache['differential_denominator']:
                if thisBin not in anaCacheSum['differential_denominator']:
                    anaCacheSum['differential_denominator'][thisBin] = {}
                for source, source_entry in anaCache['differential_denominator'][thisBin].items():
                    if source in ["sum_genWeight", "sum_genWeight2"]:
                        if source not in anaCacheSum['differential_denominator']:
                            if idx == 0:
                                anaCacheSum['differential_denominator'][source] = 0.
                            else:
                                raise RuntimeError(f"addAnaCaches: {source} not found in first cache")
                        anaCacheSum['differential_denominator'][source] += source_entry
                        continue #Since the genWeights are not dicts, the .items() logic would break, this is a catch for those two now

                    if source not in anaCacheSum['differential_denominator'][thisBin]:
                        if idx == 0:
                            anaCacheSum['differential_denominator'][thisBin][source] = {}
                        else:
                            raise RuntimeError(f"addAnaCaches: source {thisBin}/{source} not found in first cache")

                    for scale, value in source_entry.items():
                        if scale not in anaCacheSum['differential_denominator'][thisBin][source]:
                            if idx == 0:
                                anaCacheSum['differential_denominator'][thisBin][source][scale] = 0.
                            else:
                                raise RuntimeError(f"addAnaCaches: {thisBin}/{source}/{scale} not found in first cache")
                        anaCacheSum['differential_denominator'][thisBin][source][scale] += value
                

        anaCacheSum['runtime'] += anaCache['runtime']
    return anaCacheSum

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-files', required=True, type=str)
    parser.add_argument('--output', required=False, default=None, type=str)
    parser.add_argument('--global-params', required=True, type=str)
    parser.add_argument('--generator-name', required=True, type=str)
    parser.add_argument('--sample-type', required=True, type=str)
    parser.add_argument('--n-events', type=int, default=None)
    parser.add_argument('--stitch-config-path', required=False, default=None, type=str)
    parser.add_argument('--verbose', type=int, default=1)
    args = parser.parse_args()

    from Common.Utilities import DeserializeObjectFromString
    input_files = args.input_files.split(',')
    global_params = DeserializeObjectFromString(args.global_params)

    stitch_config = None
    with open(args.stitch_config_path) as f:
        stitch_config = yaml.safe_load(f)

    file_lists = create_filelists(input_files)
    anaCache = computeAnaCache(file_lists, global_params, args.generator_name, args.sample_type, stitch_config, range=args.n_events)
    if args.verbose > 0:
        print(json.dumps(anaCache))

    if args.output is not None:
        if os.path.exists(args.output):
            print(f"{args.output} already exist, removing it")
            os.remove(args.output)
        with open(args.output, 'w') as file:
            yaml.dump(anaCache, file)


