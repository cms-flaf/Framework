import ROOT
import Common.Utilities as Utilities
import os
import shutil
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

ROOT.gInterpreter.Declare(
  """
    ROOT::RDF::RNode processExample(ROOT::RDF::RNode df_in, const std::vector<std::string>& var_names)
    {
      std::cout << "TupleMaker::process: foreach started." << std::endl;
      ROOT::RDF::RNode df = df_in;
      df.Foreach([&](unsigned long long i){ std::cout << i << std::endl;}, var_names);
      return df_in;
    }
  """)

col_type_dict = {
  'ULong64_t':'unsigned long long',
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'UInt_t':'unsigned int',
  'ULong_t':'unsigned long',
  'Long_t':'long',
  'Double_t':'double'
  }

def make_df(inputFileCentral,inputFileShifted,outFile):
  if os.path.exists(outFile):
    os.remove(outFile)
  #os.mkfile(outFile, exist_ok=True)
  df_in = ROOT.RDataFrame('Events', inputFileCentral)

  df_out = ROOT.RDataFrame('Events', inputFileShifted)
  colNames = [str(c) for c in df_in.GetColumnNames()]
  entryIndexIdx = colNames.index("entryIndex")
  colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
  col_types = [str(df_in.GetColumnType(c)) for c in colNames]



  colNames_shifted = [str(c) for c in df_out.GetColumnNames()]
  entryIndexIdx_shifted = colNames_shifted.index("entryIndex")

  colNames_shifted[entryIndexIdx_shifted], colNames_shifted[0] = colNames_shifted[0], colNames_shifted[entryIndexIdx_shifted]
  col_types_shifted = [str(df_out.GetColumnType(c)) for c in colNames_shifted]
  common_columns = []
  for col, col_shifted in zip(colNames, colNames_shifted):
    if col != col_shifted:
      #print(f"col name = {col}, col name shifted = {col_shifted}")
      continue
    common_columns.append(col)
    '''
  for var_idx,var_name in enumerate(common_columns):
    if(var_idx>2):
      continue
    print(var_name)
    print(colNames_shifted[var_idx], colNames[var_idx])
    df_in.Display({"entryIndex"}).Print()
    df_out.Display({"entryIndex"}).Print()
    '''

  #print(list(dict.fromkeys(col_types)))
  #col_types = col_types[:100]
  #colNames = colNames[:100]

  #nEvents = 100
  #df_in = df_in.Range(nEvents)
  #print(','.join(f'"{c}"' for c in colNames))
  #df_out = df_out.Range(nEvents)
  #print(df_in.Describe())
  #print(df_out.Count().GetValue())
  colNames_v = Utilities.ListToVector(colNames)
  #print(colNames, col_types)
  tuple_maker = ROOT.analysis.TupleMaker(*col_types)(4)

  df_out = tuple_maker.process(ROOT.RDF.AsRNode(df_in), ROOT.RDF.AsRNode(df_out), colNames_v)
  df_out = df_out.Define("isValyd", "_entryCentral.valid")
  #df_out.Display({"isValyd"}).Print()
  df_unique = df_out.Filter("!isValyd")
  #print(df_unique.Count().GetValue())
  df_out_valid = df_out.Filter('isValyd')
  for var_idx,var_name in enumerate(colNames_shifted):
    #if(var_idx>2):
    #  continue
    if var_name in common_columns:
      df_out_valid=df_out_valid.Define(f"diff_{var_name}", f"""_entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
      #df_out_valid.Display({f"diff_{var_name}"}).Print()
    #print(var_name)
    #print(colNames_shifted[var_idx], colNames[var_idx])
    #df_out= df_out.Filter('isValyd').Define(f"diff_{var_name}", f"""std::cout <<"isValyd \t entryCentral_idx = " << _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}) << "\t entryShifted = " << {var_name} << std::endl; return _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
    #print(df_out.Count().GetValue())
    #if(var_idx<2):

  tuple_maker.join()
  snaps = []
  print("start making screenshot")
  snapshotOptions = ROOT.RDF.RSnapshotOptions()
  snapshotOptions.fOverwriteIfExists=False
  snapshotOptions.fLazy=True
  snapshotOptions.fMode="RECREATE"
  snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + 'LZ4')
  snapshotOptions.fCompressionLevel = 5
  snaps.append(df_out_valid.Snapshot(f"Events", outFile, df_out_valid.GetColumnNames(), snapshotOptions))
  snapshotOptions.fMode="UPDATE"
  snaps.append(df_unique.Snapshot(f"Events_nonValid", outFile, df_unique.GetColumnNames(), snapshotOptions))

  ROOT.RDF.RunGraphs(snaps)




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