import ROOT
import Common.Utilities as Utilities
import os
import shutil
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

def make_df(inputFileCentral,inputFileShifted,output):
  if os.path.exists(output):
    shutil.rmtree(output)
  os.makedirs(output, exist_ok=True)
  df_in = ROOT.RDataFrame('Events', inputFileCentral)

  df_out = ROOT.RDataFrame('Events', inputFileShifted)
  colNames = [str(c) for c in df_in.GetColumnNames()]
  entryIndexIdx = colNames.index("entryIndex")
  colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
  col_types = [str(df_in.GetColumnType(c)) for c in colNames]
  col_types = col_types[:3]
  colNames = colNames[:3]

  nEvents = 2
  df_in = df_in.Range(nEvents)
  print(','.join(f'"{c}"' for c in colNames))
  df_out = df_out.Range(nEvents)
  print(df_out.Count().GetValue())
  colNames_v = Utilities.ListToVector(colNames)

  tuple_maker = ROOT.analysis.TupleMaker(*col_types)(4)
  df_out = tuple_maker.process(ROOT.RDF.AsRNode(df_in), ROOT.RDF.AsRNode(df_out), colNames_v)
  '''
    if step_idx == 0:
      nTau_out = batch_size * n_batches
      df_out = ROOT.RDataFrame(nTau_out)
    else:
      output_prev_step = os.path.join(output, f'step_{step_idx}.root')
      df_out = ROOT.RDataFrame(cfg['tree_name'], output_prev_step)

    tuple_maker = ROOT.analysis.TupleMaker(*column_types)(100, nTau_in)
    df_out = tuple_maker.process(ROOT.RDF.AsRNode(df_in), ROOT.RDF.AsRNode(df_out), columns_in_v,
                                 step.start_idx, step.stop_idx, batch_size)

    define_fn_name = 'Define' if step_idx == 0 else 'Redefine'
    for column_idx in range(1, len(columns_in)):
      column_type = column_types[column_idx]
      {'ROOT::VecOps::RVec<int>', 'ROOT::VecOps::RVec<float>', 'ROOT::VecOps::RVec<ROOT::VecOps::RVec<int> >'}
      if column_type in ['ROOT::VecOps::RVec<int>', 'ROOT::VecOps::RVec<Int_t>' ]:
        default_value = '0'
        entry_col = 'int_values'
      elif column_type in [ 'ROOT::VecOps::RVec<float>', 'ROOT::VecOps::RVec<Float_t>']:
        default_value = '0.f'
        entry_col = 'float_values'
      elif column_type in [ 'ROOT::VecOps::RVec<ROOT::VecOps::RVec<int> >', 'ROOT::VecOps::RVec<vector<int> >']:
        default_value = 'RVecI()'
        entry_col = 'vint_values'
      elif column_type == 'ROOT::VecOps::RVec<ROOT::VecOps::RVec<float> >':
        default_value = 'RVecF()'
        entry_col = 'vfloat_values'
      else:
        raise Exception(f'Unknown column type {column_type}')

      if step_idx != 0:
        default_value = f'{columns_out[column_idx-1]}'
      define_str = f'_entry.valid ? _entry.{entry_col}.at({column_idx - 1}) : {default_value}'
      df_out = getattr(df_out, define_fn_name)(columns_out[column_idx - 1], define_str)

    if step_idx == 0:
      df_out = df_out.Define('is_valid', '_entry.valid')
      df_out = df_out.Define('step_idx', f'int({step_idx + 1})')
    else:
      df_out = df_out.Redefine('is_valid', '_entry.valid || is_valid')
      df_out = df_out.Redefine('step_idx', f'_entry.valid ? int({step_idx + 1}) : step_idx')
    columns_out.extend(['is_valid', 'step_idx'])
    '''

if __name__ == "__main__":
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--inFileCentral', required=True, type=str)
  parser.add_argument('--inFileShifted', required=True, type=str)
  parser.add_argument('--outFile', required=True, type=str)
  args = parser.parse_args()
  headers_dir = os.path.dirname(os.path.abspath(__file__))
  ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
  header_path_Skimmer = os.path.join(headers_dir, "SystSkimmer.h")
  ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')
  make_df(args.inFileCentral,args.inFileShifted,args.outFile)
  #df_in = ROOT.RDataFrame("Events", "output/tmp_ggR_320/Events.root")
  #df_out = ROOT.RDataFrame("Events", "output/tmp_ggR_320/Events_TotalUp.root")