import ROOT
import argparse
import os
from Common.BaselineSelection import DefineDataFrame,GetDaughters
 
_rootpath = os.path.abspath(os.path.dirname(__file__)+"/../../..")
ROOT.gROOT.ProcessLine(".include "+_rootpath)
ROOT.gROOT.ProcessLine("#include \""+_rootpath+"/hhbbTauTauRes/Framework/Common/exception.cpp\"")
ROOT.gROOT.SetBatch(True)
def PrintDecayChain(df, evtId, outFile):
    df_Chain = df.Filter(f"event=={evtId}")
    if df_Chain.Count().GetValue()==0:
        print(f"attention: event {evtId} not present in the dF")
        return
    else:
        df_Chain = df_Chain.Define("printer", f"PrintDecayChain({evtId},  GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status,genPart_daughtersm {outFile})")
    histo = df_Chain.Histo1D("printer").GetValue()
    return

parser = argparse.ArgumentParser()
parser.add_argument('--particleFile', type=str, default = 'config/pdg_name_type_charge.txt')
parser.add_argument('--evtId', type=int, default = 905)
parser.add_argument('--mass', type=str, default = '300,500,800,1000')
parser.add_argument('--allMasses', type=bool, default = False)
parser.add_argument('--outFile', type=str, default = '')
parser.add_argument('--inFile', type=str, default = '')

args = parser.parse_args()
particleFile = f"{os.environ['ANALYSIS_PATH']}/{args.particleFile}"
ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{particleFile}\");")


filesPath=f"{os.environ['ANALYSIS_PATH']}/data/nanoAOD"

if(args.inFile==''):
    files=os.listdir(filesPath)
else:
    files=[f"{filesPath}/{args.inFile}"]
    print(f"using the following files {files}")

if(args.allMasses):
    print("evaluating for all masses")
outDir_prefix=f"{os.environ['ANALYSIS_PATH']}/data/output"
if not os.path.exists(outDir_prefix):
    os.makedirs(outDir_prefix)

for file in files:
    mass_start =  file.find('-')+1
    mass_end = file.find('.root')
    mass = file[ mass_start : mass_end]
    if(args.allMasses==False and str(mass) not in args.mass.split(',')): continue
    print(f"evaluating for mass {mass} and file {file}")
    df = ROOT.RDataFrame("Events", f"{filesPath}/{file}")
    for ch in ['eTau']:#'muTau', 'tauTau']:
        df_matched = DefineDataFrame(df, ch)
        df_withDaughters = GetDaughters(df_matched)
        outDir=f"{outDir_prefix}/{ch}/DecayChains/"
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFile = f"\"{outDir}/output_{mass}_{args.evtId}.txt\""
        #print(outFile)
        PrintDecayChain(df_withDaughters, args.evtId, outFile)
