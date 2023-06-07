import ROOT
#import Common.Utilities as Utilities
import os
import shutil
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

ROOT.gInterpreter.Declare(
  """

  #include <iterator>
  #include <vector>
  #include <numeric>
  using RVecF = ROOT::VecOps::RVec<float>;
  using RVecI = ROOT::VecOps::RVec<int>;
  using RVecUL = ROOT::VecOps::RVec<unsigned long>;
  using RVecB = ROOT::VecOps::RVec<bool>;

    ROOT::RDF::RNode processExample(ROOT::RDF::RNode df_in, const std::vector<std::string>& var_names)
    {
      std::cout << "TupleMaker::process: foreach started." << std::endl;
      ROOT::RDF::RNode df = df_in;
      df.Foreach([&](unsigned long long i){ std::cout << i << std::endl;}, var_names);
      return df_in;
    }

    template<typename T>
    T GetDifference(const T& var_shifted, const T& var_central){
      using T_var_shifted = std::decay_t<decltype(var_shifted)>;
      if constexpr(std::is_same_v<T_var_shifted,RVecUL> || std::is_same_v<T_var_shifted,RVecI>
      || std::is_same_v<T_var_shifted,RVecF> || std::is_same_v<T_var_shifted,RVecB>){
        T diff ;
        T var_shifted_v = var_shifted;
        T var_central_v = var_central;
        std::set_difference(var_shifted_v.begin(), var_shifted_v.end(), var_central_v.begin(), var_central_v.end(), std::inserter(diff, diff.begin()));
        return diff;
      }
      else if constexpr(std::is_same_v<T_var_shifted,int> || std::is_same_v<T_var_shifted,float>
      || std::is_same_v<T_var_shifted,double> || std::is_same_v<T_var_shifted,bool>
      || std::is_same_v<T_var_shifted,unsigned long long> || std::is_same_v<T_var_shifted,long>
      || std::is_same_v<T_var_shifted,unsigned long> || std::is_same_v<T_var_shifted, unsigned int>){
        return var_shifted-var_central;
      }
      return var_shifted-var_central;
    }

    template<typename T>
    bool IsEqualToZero(const T& difference){
      using T_difference = std::decay_t<decltype(difference)>;
      if constexpr(std::is_same_v<T_difference,RVecUL> || std::is_same_v<T_difference,RVecI>
      || std::is_same_v<T_difference,RVecF> || std::is_same_v<T_difference,RVecB>){
        return accumulate(difference.begin(),difference.end(),0)==0;
      }
      else if constexpr(std::is_same_v<T_difference,int> || std::is_same_v<T_difference,float>
      || std::is_same_v<T_difference,double> || std::is_same_v<T_difference,bool>
      || std::is_same_v<T_difference,unsigned long long> || std::is_same_v<T_difference,long>
      || std::is_same_v<T_difference,unsigned long> || std::is_same_v<T_difference, unsigned int>){
        return difference==0;
      }
      return difference==0;
    }

    template<typename T>
    bool IsDifferentFromZero(const T& difference){
      using T_difference = std::decay_t<decltype(difference)>;
      if constexpr(std::is_same_v<T_difference,RVecUL> || std::is_same_v<T_difference,RVecI>
      || std::is_same_v<T_difference,RVecF> || std::is_same_v<T_difference,RVecB>){
        return accumulate(difference.begin(),difference.end(),0)!=0;
      }
      else if constexpr(std::is_same_v<T_difference,int> || std::is_same_v<T_difference,float>
      || std::is_same_v<T_difference,double> || std::is_same_v<T_difference,bool>
      || std::is_same_v<T_difference,unsigned long long> || std::is_same_v<T_difference,long>
      || std::is_same_v<T_difference,unsigned long> || std::is_same_v<T_difference, unsigned int>){
        return difference!=0;
      }
      return difference!=0;
    }
  """)
def ListToVector(list, type="string"):
	vec = ROOT.std.vector(type)()
	for item in list:
		vec.push_back(item)
	return vec
col_type_dict = {
  'ULong64_t':'unsigned long long',
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'UInt_t':'unsigned int',
  'ULong_t':'unsigned long',
  'Long_t':'long',
  'Double_t':'double',
  'ROOT::VecOps::RVec<float>':'ROOT::VecOps::RVec<float>',
  'ROOT::VecOps::RVec<int>':'ROOT::VecOps::RVec<int>',
  'ROOT::VecOps::RVec<bool>':'ROOT::VecOps::RVec<bool>',
  'ROOT::VecOps::RVec<unsigned long>':'ROOT::VecOps::RVec<unsigned long>',
  }

def make_df(inputFileCentral,inputFileShifted,outFile):

  df_out = ROOT.RDataFrame('Events', inputFileShifted)
  colNames = [str(c) for c in df_out.GetColumnNames()]
  entryIndexIdx = colNames.index("entryIndex")
  colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
  #print(colNames)
  col_types = [str(df_out.GetColumnType(c)) for c in colNames]
  #print(','.join(f'"{c}"' for c in colNames))
  #print(list(set(col_types)))
  #nEvents = 10
  #df_out = df_out.Range(nEvents)
  tuple_maker = ROOT.analysis.TupleMaker(*col_types)('Events', inputFileCentral, 4)
  print("tuplemaker created")
  df_out = tuple_maker.processOut(ROOT.RDF.AsRNode(df_out))
  print("tuplemaker processed")
  df_out = df_out.Define("isValid", "_entryCentral.use_count() > 0")
  print("defined isValid entry")
  df_unique = df_out.Filter("!isValid")
  print("defined df_unique")
  df_out_valid = df_out.Filter('isValid')

  print("defined df_valid")
  colToSave_diff= []
  for var_idx,var_name in enumerate(colNames):
    # if colNames[var_idx] in df_in.GetColumnNames():
      #df_out_valid= df_out.Filter('isValid').Define(f"diff_{var_name}", f"""std::cout <<"isValid \t entryCentral_idx = " << _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}) << "\t entryShifted = " << {var_name} << std::endl; return _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
    df_out_valid=df_out_valid.Define(f"diff_{var_name}", f"""GetDifference(_entryCentral->GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}),{var_name})""")
    colToSave_diff.append(f"diff_{var_name}")
  df_out_valid_noDiff = df_out_valid
  colToSave_noDiff= ["period","run", "sample_name", "sample_type", "channelId", "entryIndex", "event", "isData", "luminosityBlock"]
  condition_noDiff_list = [f"IsEqualToZero({c}) " for c in colToSave_diff]
  condition_noDiff = ' && '.join(condition_noDiff_list)
  print(condition_noDiff)
  df_out_valid_noDiff = df_out_valid_noDiff.Define("condition_noDiff", condition_noDiff).Filter("condition_noDiff")
  #condition_Valid_list = [f"IsDifferentFromZero({c}) " for c in colToSave_diff]
  #condition_Valid = ' || '.join(condition_Valid_list)
  #print(condition_Valid)
  #df_out_valid = df_out_valid.Define("condition_Valid", condition_Valid).Filter("condition_Valid")

  snaps = []
  print("start making screenshot")
  snapshotOptions = ROOT.RDF.RSnapshotOptions()
  snapshotOptions.fOverwriteIfExists=False
  snapshotOptions.fLazy=True
  snapshotOptions.fMode="RECREATE"
  snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + 'LZ4')
  snapshotOptions.fCompressionLevel = 5
  #colToSave_noDiff_v = ListToVector(colToSave_noDiff)
  colToSave_diff_v = ListToVector(colToSave_diff)
  colNames_v = ListToVector(colNames)
  print(colToSave_diff_v.size())
  print(colNames_v.size())
  outFile_Valid = f"{outFile}.root"
  outFile_nonValid = f"{outFile}_nonValid.root"
  outFile_Valid_noDiff = f"{outFile}_noDiff.root"
  if os.path.exists(outFile_Valid):
    os.remove(outFile_Valid)
  if os.path.exists(outFile_nonValid):
    os.remove(outFile_nonValid)
  if os.path.exists(outFile_Valid_noDiff):
    os.remove(outFile_Valid_noDiff)
  #df_out_valid.Snapshot(f"Events", outFile_Valid, colToSave_diff_v, snapshotOptions)
  #snaps.append(df_out_valid_noDiff.Snapshot(f"Events", outFile_Valid_noDiff, colToSave_noDiff_v, snapshotOptions))
  snaps.append(df_out_valid.Snapshot(f"Events", outFile_Valid, colToSave_diff_v, snapshotOptions))
  snaps.append(df_unique.Snapshot(f"Events", outFile_nonValid, colNames_v, snapshotOptions))
  tuple_maker.processIn(colNames_v)
  ROOT.RDF.RunGraphs(snaps)
  print("finished screenshot")

  tuple_maker.join()



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