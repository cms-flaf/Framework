#include "../HHbtag/interface/HH_BTag.h"
#include "AnalysisTools.h"
#include "HHCore.h"
 
struct HHBtagWrapper{
    static void Initialize(const std::string& path, int version)
    {
        std::array <std::string, 2> models;
        for(size_t n = 0; n < 2; ++n) {
            std::ostringstream ss_model;
            ss_model << path + "HHbtag_v" << version << "_par_" << n;
            models.at(n) = ss_model.str();
        }
        _Get() = std::make_unique<hh_btag::HH_BTag>(models);
    }
    static hh_btag::HH_BTag& Get()
    {
        auto& hh_btag = HHBtagWrapper::_Get();
        if(!hh_btag)
            throw std::runtime_error("HHBtag is not initialized.");
        return *hh_btag;
    }
    private:
        static std::unique_ptr<hh_btag::HH_BTag>& _Get()
        {
            static std::unique_ptr<hh_btag::HH_BTag> hh_btag;
            return hh_btag;
        }
};
 

RVecF GetHHBtagScore(const RVecLV& jet_p4,const RVecF& Jet_deepFlavour, const float& met_pt, const float& met_phi, 
                            const HTTCand& HTT_Cand, const int& period, int event){
    const ULong64_t parity = event % 2;
    int channelId = static_cast<int>(HTT_Cand.channel());
    
    std::vector<float> jet_pt;
    std::vector<float> jet_eta;
    std::vector<float> jet_deepFlavour;
    std::vector<float> rel_jet_M_pt;
    std::vector<float> rel_jet_E_pt;
    std::vector<float> jet_htt_deta;
    std::vector<float> jet_htt_dphi; 

    LorentzVectorM hTT_p4 =HTT_Cand.leg_p4[0]+HTT_Cand.leg_p4[1];  
    float htt_pt=hTT_p4.Pt();
    float htt_eta=hTT_p4.Eta();
    float htt_met_dphi = DeltaPhi(static_cast<float>(hTT_p4.Phi()),static_cast<float>(met_phi));
    float htt_scalar_pt= HTT_Cand.leg_p4[0].Pt()+HTT_Cand.leg_p4[1].Pt();
    float rel_met_pt_htt_pt=met_pt/htt_scalar_pt;

    for (size_t jet_idx=0; jet_idx<jet_p4.size(); jet_idx++){
        jet_pt.push_back(jet_p4.at(jet_idx).Pt());
        jet_eta.push_back(jet_p4.at(jet_idx).Eta());
        jet_deepFlavour.push_back(Jet_deepFlavour.at(jet_idx));
        rel_jet_M_pt.push_back(jet_p4.at(jet_idx).M()/jet_p4.at(jet_idx).Pt());
        rel_jet_E_pt.push_back(jet_p4.at(jet_idx).E()/jet_p4.at(jet_idx).Pt());
        jet_htt_deta.push_back(DeltaEta(static_cast<float>(hTT_p4.Eta()),static_cast<float>(jet_p4.at(jet_idx).Eta())));
        jet_htt_dphi.push_back(DeltaPhi(static_cast<float>(hTT_p4.Phi()),static_cast<float>(jet_p4.at(jet_idx).Phi())));
    }  

    
    RVecF scores = HHBtagWrapper::Get().GetScore(jet_pt, jet_eta,
                                             rel_jet_M_pt, rel_jet_E_pt,
                                             jet_htt_deta, jet_deepFlavour,
                                             jet_htt_dphi, period,
                                             channelId, htt_pt,
                                             htt_eta, htt_met_dphi,
                                             rel_met_pt_htt_pt,
                                             htt_scalar_pt, parity); 
    
    return scores;

}

