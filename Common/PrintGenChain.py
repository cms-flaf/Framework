import ROOT
import argparse
import os
from Common.BaselineSelection import DefineDataFrame



ROOT.gROOT.SetBatch(True)
def PrintDecayChain(df, evtId, outFile):
    df_Chain = df.Define("printer", f"PrintDecayChain({evtId},  GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, {outFile})")
    histo = df_Chain.Histo1D("printer").GetValue()
    return

parser = argparse.ArgumentParser()
parser.add_argument('--particleFile', type=str, default = 'config/pdg_name_type_charge.txt')
parser.add_argument('--inFile', type=str, default = '')
parser.add_argument('--outFile', type=str, default = '')

args = parser.parse_args()
particleFile = f"{os.environ['ANALYSIS_PATH']}/{args.particleFile}"
ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{particleFile}\");")

filesPath=f"{os.environ['ANALYSIS_PATH']}/data/nanoAOD"
print(filesPath)
if(args.inFile==''):
    files=os.listdir(filesPath)
    file=files[0]
    mass_start =  file.find('-')+1
    mass_end = file.find('.root')
    mass = file[ mass_start : mass_end]
    print(f"evaluating for mass {mass}")
else:
    file=f"{filesPath}/{args.file}"
    print(f"evaluating file {file}")
df = ROOT.RDataFrame("Events", f"{filesPath}/{file}")
outDir=f"{os.environ['ANALYSIS_PATH']}/data/output"
if not os.path.exists(outDir):
    os.makedirs(outDir)
for ch in ['eTau']:#'muTau', 'tauTau']:
    df_matched = DefineDataFrame(df, ch)
    outFile = f"\"{outDir}/{ch}/{args.outFile}\""
    print("evt id on which printing is 905")
    PrintDecayChain(df_matched, 905, outFile)
