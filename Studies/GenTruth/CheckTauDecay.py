if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    import ROOT
    ROOT.gROOT.SetBatch(True)
    ROOT.EnableImplicitMT(8)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")


    ROOT.gInterpreter.Declare("""
    bool GetTauDaughters(std::vector<int>& tau_daughters, const reco_tau::gen_truth::GenParticle* part)
    {
        static const std::set<int> directTauProducts = { 11, 12, 13, 14, 16, 22 };
        static const std::set<int> finalHadrons = { 111, 130, 211, 310, 311, 321 };
        static const std::set<int> intermediateHadrons = { 221, 223, 323 };
        if(part->daughters.size() == 0) {
            std::cout << "No daughters for pdgId = " << part->pdgId << std::endl;
            return false;
        }
        for(auto daughter : part->daughters) {
            const int daughter_pdgId = std::abs(daughter->pdgId);
            if( (std::abs(part->pdgId) == 15 && directTauProducts.count(daughter_pdgId))
                || finalHadrons.count(daughter_pdgId)) {
                tau_daughters.push_back(daughter->pdgId);
            } else if (intermediateHadrons.count(daughter_pdgId)) {
                if(!GetTauDaughters(tau_daughters, daughter))
                    return false;
            } else {
                // std::cout << "Unknown daughter pdgId = " << daughter_pdgId << ". Mother pdgId = " << part->pdgId << std::endl;
                return false;
                // throw std::runtime_error("Unknown daughter pdgId = " + std::to_string(daughter_pdgId));
            }
        }
        return true;
    }
    """)

    df = ROOT.RDataFrame("Events", args.inFile)
    df = df.Define("genLeptons", """reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                        GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                        GenPart_statusFlags, GenPart_vx, GenPart_vy, GenPart_vz, event)""")
    df = df.Filter("""
        for(const auto& lep: genLeptons) {
            std::vector<int> tau_daughters;
            if(lep.lastCopy().pdgId != 15) continue;
            if(!GetTauDaughters(tau_daughters, &lep.lastCopy())) {
                // std::cout << lep << std::endl;
                return true;
            }
            size_t n_part = 0;
            for(int dauther_pdg : tau_daughters) {
                if (std::abs(dauther_pdg) == 22) n_part++;
            }
            if(n_part > 1) {
                // std::cout << lep << std::endl;
                return true;
            }
        }
        return false;
    """)
    event_ids = df.AsNumpy([ 'event' ])['event']
    print(f'n_selected={len(event_ids)}')
    n_to_print=10
    to_print = ' '.join(map(str, event_ids[:n_to_print].tolist()))
    prefix = f'first {n_to_print} ' if len(event_ids) > n_to_print else ''
    print(f'{prefix}selected event ids: {to_print}')
