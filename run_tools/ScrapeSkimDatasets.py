import os
import yaml
import json
import sys


def ScrapeSkimDatasets(input_dir, output):
  #skim_storage_dir = "/eos/cms/store/group/phys_higgs/HLepRare/skim_2024_v1/Run3_2022"
  skim_storage_dir = input_dir
  yaml_filename = output
  xsec_filename = '_xsec.'.join(output.split('.'))

  datasets = os.listdir(skim_storage_dir)

  dict_for_yaml = {}
  bad_cases = []

  dict_for_xsec = {}

  for dataset in datasets:
      print(dataset)
      sampleType = 'empty'
      crossSection = dataset.split('_ext')[0]

      xSec = '1.0'
      ref = 'fill me!'
      unc = ''


      #For now we can skip signal and data
      if dataset.startswith('DoubleMuon') or dataset.startswith('Muon') or dataset.startswith('SingleMuon') or dataset.startswith('EGamma') or dataset.startswith('MuonEG') or dataset.startswith('JetHT') or dataset.startswith('JetMET') or dataset.startswith('MET') or dataset.startswith('Tau'):
        continue
      if dataset.startswith('GluGlutoRadion') or dataset.startswith('GluGlutoBulkGraviton'):
        continue
      if dataset.startswith('GluGlutoHH') or dataset.startswith('VBFHH'):
        continue

      if dataset.startswith('DY'):
        sampleType = 'DY'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''


      if dataset.startswith('ST'):
        sampleType = 'ST'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''
        if dataset.startswith('ST_s-channel_antitop'):
          crossSection = 'ST_s-channel_antitop'
          xSec = '0.0'
          unc = ''
          ref = 'Take this from XSDB for now'
        if dataset.startswith('ST_s-channel_top'):
          crossSection = 'ST_s-channel_top'
          xSec = '0.0'
          unc = ''
          ref = 'Take this from XSDB for now'
        if dataset.startswith('ST_t-channel_antitop'):
          crossSection = 'ST_t-channel_antitop'
          xSec = '87.2'
          unc = '+0.9 -0.8 (scale) +1.5 -1.3(PDF+alphaS) +1.8 -1.5(Total) +0.6 -0.7(mass) +0.2 -0.2 (Ebeam) +/-0.1 (Integration)'
          ref = 'https://twiki.cern.ch/twiki/bin/view/LHCPhysics/SingleTopNNLORef'
        if dataset.startswith('ST_t-channel_top'):
          crossSection = 'ST_t-channel_top'
          xSec = '145.0'
          unc = '+1.7 -1.1 (scale) +2.3 -1.5(PDF+alphaS) +2.8 -1.9(Total) +1.3 -0.9(mass) +0.4 -0.3 (Ebeam) +/-0.1 (Integration)'
          ref = 'https://twiki.cern.ch/twiki/bin/view/LHCPhysics/SingleTopNNLORef'
        if dataset.startswith('ST-tW_antitop'):
          crossSection = 'ST-tW_antitop'
          xSec = '0.0'
          unc = ''
          ref = 'Take this from XSDB for now'
        if dataset.startswith('ST-tW_top'):
          crossSection = 'ST-tW_top'
          xSec = '0.0'
          unc = ''
          ref = 'Take this from XSDB for now'


      if dataset.startswith('TT'):
        sampleType = 'TT'
        xSec = '923.6'
        ref = 'https://twiki.cern.ch/twiki/bin/view/LHCPhysics/TtbarNNLO#Updated_reference_cross_sections'
        unc = '+22.6 -33.4 (scale) +/-  22.8 (PDF alphas) -24.6 +25.4 (mass) (to be multiplied by the BR)'
        if dataset.startswith('TTto2L2Nu'):
          xSec = xSec + ' * (1 - 0.6741) * (1 - 0.6741)'
        if dataset.startswith('TTtoLNu2Q'):
          xSec = xSec + ' * 0.6741 * (1 - 0.6741) * 2'
        if dataset.startswith('TTto4Q'):
          xSec = xSec + ' * 0.6741 * 0.6741'

      if dataset.startswith('TTZToQQ'):
        sampleType = 'TTV'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''
      if dataset.startswith('TTWH') or dataset.startswith('TTWW') or dataset.startswith('TTZH') or dataset.startswith('TTZZ'):
        sampleType = 'TTVV'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('Wto') or dataset.startswith('WTo'):
        sampleType = 'W'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''
        if dataset.startswith('WtoLNu_amcatnloFXFX') or dataset.startswith('WtoLNu_madgraphMLM'):
          xSec = '3*(9013.3 + 12128.4)'
          ref = 'https://twiki.cern.ch/twiki/bin/viewauth/CMS/MATRIXCrossSectionsat13p6TeV'
          unc = '+1.2%% -1.1%% +-0.8%% (e-nu) +1.1%% -1.4%% +-0.7%% (e+nu)'

      if dataset.startswith('Zto2Nu'):
        sampleType = 'Zto2Nu'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('Zto2Q'):
        sampleType = 'ZQQ'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('WW') or dataset.startswith('WZ') or dataset.startswith('ZZ'):
        sampleType = 'VV'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''
        if dataset.startswith('WW'):
          crossSection = 'WW'
          xSec = '0.0'
          unc = ''
          ref = 'fill me!'

          if dataset.startswith('WWto2L2Nu'):
            crossSection = 'WWto2L2Nu'
            xSec = '0.0'
            unc = ''
            ref = 'fill me!'

          if dataset.startswith('WWto4Q'):
            crossSection = 'WW'
            xSec = '0.0'
            unc = ''
            ref = 'fill me!'

          if dataset.startswith('WWtoLNu2Q'):
            crossSection = 'WW'
            xSec = '0.0'
            unc = ''
            ref = 'fill me!'




      if dataset.startswith('WWW') or dataset.startswith('WWZ') or dataset.startswith('WZZ'):
        sampleType = 'VVV'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('WminusH') or dataset.startswith('WplusH') or dataset.startswith('ZH'):
        sampleType = 'VH'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''
        if dataset.startswith('ZH_Hbb_Zll'):
          xSec = '9.439e-1 * 0.5824 * 3 * 0.033658'
          ref = 'https://twiki.cern.ch/twiki/bin/view/LHCPhysics/LHCHWG136TeVxsec_extrap'
          unc = '+3.7 -3.2 (qcd scale) +-1.6 (pdf alpha) +-1.3 (pdf) +- 0.9 (alpha)'
        if dataset.startswith('ZH_Hbb_Zqq'):
          xSec = '9.439e-1 * 0.5824 * 0.69911'
          ref = 'https://twiki.cern.ch/twiki/bin/view/LHCPhysics/LHCHWG136TeVxsec_extrap'
          unc = '+3.7 -3.2 (qcd scale) +-1.6 (pdf alpha) +-1.3 (pdf) +- 0.9 (alpha)'



      if dataset.startswith('ttH'):
        sampleType = 'ttH'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('QCD'):
        sampleType = 'QCD'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if dataset.startswith('GluGluHto') or dataset.startswith('GluGluHTo') or dataset.startswith('VBFHTo'):
        sampleType = 'H'
        xSec = '1.0'
        ref = 'fill me!'
        unc = ''

      if sampleType == 'test':
        bad_cases.append(dataset)



      prodReport = open(os.path.join(skim_storage_dir, dataset, 'prodReport_nano.json'))
      prodReport_data = json.load(prodReport)
      dataset_name = prodReport_data['inputDataset']
      generator = ((dataset_name.split('/')[1]).split('_')[-1]).split('-')[0]



      if dataset.startswith('DY'):
        dict_for_yaml[dataset] = {'sampleType': sampleType, 'crossSectionStitch': crossSection, 'generator': generator, 'miniAOD': dataset_name}
      else:
        dict_for_yaml[dataset] = {'sampleType': sampleType, 'crossSection': crossSection, 'generator': generator, 'miniAOD': dataset_name}

      dict_for_xsec[crossSection] = {'crossSec': xSec, 'reference': ref, 'unc': unc}


  #f = open('temp_Run3_2022.yaml', 'w+')
  f = open(yaml_filename, 'w+')
  yaml.dump(dict_for_yaml, f, allow_unicode=True)
  #t = open('temp_Run3_2022_xsec.yaml', 'w+')
  t = open(xsec_filename, 'w+')
  yaml.dump(dict_for_xsec, t, allow_unicode=True)


  print("Could not find a match for these cases")
  print(bad_cases)




if __name__ == '__main__':
        file_dir = os.path.dirname(os.path.abspath(__file__))
        pkg_dir = os.path.dirname(file_dir)
        base_dir = os.path.dirname(pkg_dir)
        pkg_dir_name = os.path.split(pkg_dir)[1]
        if base_dir not in sys.path:
            sys.path.append(base_dir)
        __package__ = pkg_dir_name

        import argparse
        parser = argparse.ArgumentParser(description='Create yaml from a skimmed dataset directory')
        parser.add_argument('--input-dir', required=True, type=str, help="input directory of skimmed datasets")
        parser.add_argument('--output', required=True, type=str, help="output yaml file")

        args = parser.parse_args()
        input_dir = args.input_dir
        output = args.output

        ScrapeSkimDatasets(input_dir, output)
