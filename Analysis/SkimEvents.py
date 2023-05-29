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