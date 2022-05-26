#include <math.h>
#include <iostream>
#include <fstream>
#include <string>
using LorentzVectorM = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>;
using vec_i = ROOT::VecOps::RVec<int>;
using vec_s = ROOT::VecOps::RVec<size_t>;
using vec_f = ROOT::VecOps::RVec<float>;
using vec_b = ROOT::VecOps::RVec<bool>;
using vec_uc = ROOT::VecOps::RVec<unsigned char>;

namespace Channel{
    enum {
        eTau = 0,
        muTau = 1,
        tauTau = 2,
        eMu = 3,
        eE =4,
        muMu = 5
    } ;
}

std::map<int, float> particleMasses {
  {11, 0.00051099894},
  {12, 0.},
  {13, 1.0565837},
  {14,0.},
  {15, 1.77686},
  {16, 0.},
  {22, 0.},
  {111, 0.134977},
  {-111, 0.134977},
  {211, 0.13957},
  {-211, 0.13957},
  {311, 0.497611},
  {-311, 0.497611},
  {321, 0.493677},
  {-321, 0.493677},
  {421, 1.86483},
  {-421, 1.86483},
  {411, 1.8695},
  {-11, 1.8695}
};


double muon_mass = particleMasses.at(13);
double electron_mass = particleMasses.at(11);

std::map<int, std::string> pdgNames{
  {329 ,"K_4*+: "},
  {110553 ,"h_b(2P): "},
  {5 ,"b: "},
  {-12 ,"anti-nu_e: "},
  {425 ,"D_2*0: "},
  {553 ,"Upsilon: "},
  {-20513 ,"anti-B'_10: "},
  {-311 ,"anti-K0: "},
  {13122 ,"Lambda(1405)0: "},
  {323 ,"K*+: "},
  {-11 ,"e+: "},
  {510 ,"B0H: "},
  {-325 ,"K_2*-: "},
  {20513 ,"B'_10: "},
  {-321 ,"K-: "},
  {110555 ,"eta_b2(2D): "},
  {100313 ,"K'*0: "},
  {10533 ,"B_s10: "},
  {310 ,"K_S0: "},
  {130553 ,"Upsilon_1(2D): "},
  {-10521 ,"B_0*-: "},
  {20423 ,"D'_10: "},
  {521 ,"B+: "},
  {10521 ,"B_0*+: "},
  {5114 ,"Sigma_b*-: "},
  {-13122 ,"anti-Lambda(1405)0: "},
  {-10533 ,"anti-B_s10: "},
  {150 ,"B0L: "},
  {-10511 ,"anti-B_0*0: "},
  {-13212 ,"anti-Sigma(1660)0: "},
  {5232 ,"Xi_b0: "},
  {-415 ,"D_2*-: "},
  {-10213 ,"b_1-: "},
  {313 ,"K*0: "},
  {-100313 ,"anti-K'*0: "},
  {-23122 ,"anti-Lambda(1600)0: "},
  {20523 ,"B'_1+: "},
  {15 ,"tau-: "},
  {20333 ,"f'_1: "},
  {-100411 ,"D(2S)-: "},
  {-435 ,"D_s2*-: "},
  {2112 ,"n0: "},
  {-3314 ,"anti-Xi*+: "},
  {-413 ,"D*-: "},
  {20213 ,"a_1+: "},
  {413 ,"D*+: "},
  {82 ,"rndmflav: "},
  {-2203 ,"anti-uu_1: "},
  {-4 ,"anti-c: "},
  {9000553 ,"Upsilon(5S): "},
  {4422 ,"Xi_cc++: "},
  {-541 ,"B_c-: "},
  {-3114 ,"anti-Sigma*+: "},
  {-87 ,"anti-b'-hadron: "},
  {30553 ,"Upsilon_1(1D): "},
  {100441 ,"eta_c(2S): "},
  {-2214 ,"anti-Delta-: "},
  {-10421 ,"anti-D_0*0: "},
  {-18 ,"anti-nu_L: "},
  {13126 ,"Lambda(1830)0: "},
  {3122 ,"Lambda0: "},
  {2203 ,"uu_1: "},
  {9010443 ,"psi(4160): "},
  {5322 ,"Xi'_b0: "},
  {87 ,"b'-hadron: "},
  {3334 ,"Omega-: "},
  {10513 ,"B_10: "},
  {-2103 ,"anti-ud_1: "},
  {86 ,"t-hadron: "},
  {-86 ,"anti-t-hadron: "},
  {-3212 ,"anti-Sigma0: "},
  {-20523 ,"B'_1-: "},
  {14122 ,"Lambda_c(2593)+: "},
  {-3126 ,"anti-Lambda(1820)0: "},
  {4432 ,"Omega_cc+: "},
  {-2224 ,"anti-Delta--: "},
  {515 ,"B_2*0: "},
  {20313 ,"K'_10: "},
  {9000221 ,"sigma_0: "},
  {-4424 ,"anti-Xi_cc*--: "},
  {-10423 ,"anti-D_10: "},
  {-14122 ,"anti-Lambda_c(2593)-: "},
  {-10523 ,"B_1-: "},
  {-4412 ,"anti-Xi_cc-: "},
  {-431 ,"D_s-: "},
  {-4122 ,"anti-Lambda_c-: "},
  {4412 ,"Xi_cc+: "},
  {10523 ,"B_1+: "},
  {-10411 ,"D_0*-: "},
  {-4422 ,"anti-Xi_cc--: "},
  {-5114 ,"anti-Sigma_b*+: "},
  {200555 ,"chi_b2(3P): "},
  {-30323 ,"K''*-: "},
  {10022 ,"vpho: "},
  {-329 ,"K_4*-: "},
  {130 ,"K_L0: "},
  {200551 ,"eta_b(3S): "},
  {30323 ,"K''*+: "},
  {43122 ,"Lambda(1800)0: "},
  {-43122 ,"anti-Lambda(1800)0: "},
  {10313 ,"K_10: "},
  {-100413 ,"D*(2S)-: "},
  {98 ,"CELLjet: "},
  {-545 ,"B_c2*-: "},
  {10311 ,"K_0*0: "},
  {-10431 ,"D_s0*-: "},
  {210553 ,"h_b(3P): "},
  {4122 ,"Lambda_c+: "},
  {4132 ,"Xi_c0: "},
  {10431 ,"D_s0*+: "},
  {88 ,"junction: "},
  {-20323 ,"K'_1-: "},
  {9910445 ,"X_2(3872): "},
  {321 ,"K+: "},
  {-4414 ,"anti-Xi_cc*-: "},
  {3 ,"s: "},
  {100413 ,"D*(2S)+: "},
  {433 ,"D_s*+: "},
  {-4334 ,"anti-Omega_c*0: "},
  {10411 ,"D_0*+: "},
  {-2212 ,"anti-p-: "},
  {-5103 ,"anti-bd_1: "},
  {10553 ,"h_b: "},
  {-13126 ,"anti-Lambda(1830)0: "},
  {-2 ,"anti-u: "},
  {-13 ,"mu+: "},
  {5401 ,"bc_0: "},
  {1000020040 ,"alpha: "},
  {3114 ,"Sigma*-: "},
  {30313 ,"K''*0: "},
  {-523 ,"B*-: "},
  {83 ,"phasespa: "},
  {10113 ,"b_10: "},
  {411 ,"D+: "},
  {85 ,"b-hadron: "},
  {-5324 ,"anti-Xi_b*0: "},
  {220553 ,"chi_b1(3P): "},
  {100423 ,"D*(2S)0: "},
  {-423 ,"anti-D*0: "},
  {53122 ,"Lambda(1810)0: "},
  {113 ,"rho0: "},
  {-513 ,"anti-B*0: "},
  {-211 ,"pi-: "},
  {-20533 ,"anti-B'_s10: "},
  {10223 ,"h_1: "},
  {480000000 ,"geantino: "},
  {93 ,"indep: "},
  {20022 ,"Cerenkov: "},
  {-3214 ,"anti-Sigma*0: "},
  {20113 ,"a_10: "},
  {37 ,"Higgs+: "},
  {3214 ,"Sigma*0: "},
  {4414 ,"Xi_cc*+: "},
  {335 ,"f'_2: "},
  {5132 ,"Xi_b-: "},
  {-319 ,"anti-K_4*0: "},
  {100213 ,"rho(2S)+: "},
  {441 ,"eta_c: "},
  {10421 ,"D_0*0: "},
  {5203 ,"bu_1: "},
  {-3203 ,"anti-su_1: "},
  {315 ,"K_2*0: "},
  {-4332 ,"anti-Omega_c0: "},
  {-20433 ,"D_s1-: "},
  {-1000010020 ,"anti-deuteron: "},
  {4103 ,"cd_1: "},
  {10543 ,"B_c1+: "},
  {435 ,"D_s2*+: "},
  {5332 ,"Omega_b-: "},
  {-10541 ,"B_c0*-: "},
  {300553 ,"Upsilon(4S): "},
  {5224 ,"Sigma_b*+: "},
  {-2114 ,"anti-Delta0: "},
  {3101 ,"sd_0: "},
  {-3101 ,"anti-sd_0: "},
  {20433 ,"D_s1+: "},
  {100553 ,"Upsilon(2S): "},
  {331 ,"eta': "},
  {-3216 ,"anti-Sigma(1775)0: "},
  {10423 ,"D_10: "},
  {5214 ,"Sigma_b*0: "},
  {-323 ,"K*-: "},
  {-14 ,"anti-nu_mu: "},
  {5222 ,"Sigma_b+: "},
  {-315 ,"anti-K_2*0: "},
  {20443 ,"chi_c1: "},
  {20413 ,"D'_1+: "},
  {-5132 ,"anti-Xi_b+: "},
  {-30363 ,"anti-Xss: "},
  {-411 ,"D-: "},
  {513 ,"B*0: "},
  {44 ,"Xu+: "},
  {20533 ,"B'_s10: "},
  {-3322 ,"anti-Xi0: "},
  {3324 ,"Xi*0: "},
  {-85 ,"anti-b-hadron: "},
  {97 ,"CLUSjet: "},
  {1 ,"d: "},
  {9020443 ,"psi(4415): "},
  {10511 ,"B_0*0: "},
  {10333 ,"h'_1: "},
  {-4232 ,"anti-Xi_c-: "},
  {-5 ,"anti-b: "},
  {4201 ,"cu_0: "},
  {-10323 ,"K_1-: "},
  {20223 ,"f_1: "},
  {100223 ,"omega(2S): "},
  {3224 ,"Sigma*+: "},
  {5201 ,"bu_0: "},
  {-20213 ,"a_1-: "},
  {30221 ,"f'_0: "},
  {-30313 ,"anti-K''*0: "},
  {-30213 ,"rho(3S)-: "},
  {325 ,"K_2*+: "},
  {-10433 ,"D'_s1-: "},
  {10433 ,"D'_s1+: "},
  {20323 ,"K'_1+: "},
  {-5332 ,"anti-Omega_b+: "},
  {10213 ,"b_1+: "},
  {-213 ,"rho-: "},
  {-543 ,"B_c*-: "},
  {200553 ,"Upsilon(3S): "},
  {523 ,"B*+: "},
  {13212 ,"Sigma(1660)0: "},
  {-4201 ,"anti-cu_0: "},
  {3203 ,"su_1: "},
  {225 ,"f_2: "},
  {-84 ,"anti-c-hadron: "},
  {-215 ,"a_2-: "},
  {84 ,"c-hadron: "},
  {-4103 ,"anti-cd_1: "},
  {7 ,"b': "},
  {-5224 ,"anti-Sigma_b*-: "},
  {-6 ,"anti-t: "},
  {6 ,"t: "},
  {211 ,"pi+: "},
  {-13124 ,"anti-Lambda(1690)0: "},
  {2114 ,"Delta0: "},
  {-521 ,"B-: "},
  {2103 ,"ud_1: "},
  {33 ,"Z''0: "},
  {-3303 ,"anti-ss_1: "},
  {-4101 ,"anti-cd_0: "},
  {-4301 ,"anti-cs_0: "},
  {431 ,"D_s+: "},
  {-425 ,"anti-D_2*0: "},
  {17 ,"L-: "},
  {223 ,"omega: "},
  {11 ,"e-: "},
  {115 ,"a_20: "},
  {8 ,"t': "},
  {4301 ,"cs_0: "},
  {100211 ,"pi(2S)+: "},
  {4203 ,"cu_1: "},
  {5212 ,"Sigma_b0: "},
  {-3201 ,"anti-su_0: "},
  {-20423 ,"anti-D'_10: "},
  {-4212 ,"anti-Sigma_c-: "},
  {120553 ,"chi_b1(2P): "},
  {5101 ,"bd_0: "},
  {4434 ,"Omega_cc*+: "},
  {-33122 ,"anti-Lambda(1670)0: "},
  {12 ,"nu_e: "},
  {94 ,"CMshower: "},
  {3201 ,"su_0: "},
  {5334 ,"Omega_b*-: "},
  {100551 ,"eta_b(2S): "},
  {10443 ,"h_c: "},
  {24 ,"W+: "},
  {215 ,"a_2+: "},
  {-10321 ,"K_0*-: "},
  {557 ,"Upsilon_3(1D): "},
  {10555 ,"eta_b2(1D): "},
  {91 ,"cluster: "},
  {423 ,"D*0: "},
  {4114 ,"Sigma_c*0: "},
  {-327 ,"K_3*-: "},
  {-82 ,"anti-rndmflav: "},
  {-4114 ,"anti-Sigma_c*0: "},
  {-10313 ,"anti-K_10: "},
  {30223 ,"omega(1650): "},
  {-8 ,"anti-t': "},
  {5122 ,"Lambda_b0: "},
  {210551 ,"chi_b0(3P): "},
  {-3334 ,"anti-Omega+: "},
  {5303 ,"bs_1: "},
  {1000010030 ,"tritium: "},
  {33122 ,"Lambda(1670)0: "},
  {-4403 ,"anti-cc_1: "},
  {-13214 ,"anti-Sigma(1670)0: "},
  {531 ,"B_s0: "},
  {36 ,"A0: "},
  {-4124 ,"anti-Lambda_c(2625)-: "},
  {96 ,"THRUaxis: "},
  {100555 ,"chi_b2(2P): "},
  {95 ,"SPHEaxis: "},
  {-20543 ,"B'_c1-: "},
  {92 ,"string: "},
  {100221 ,"eta(2S): "},
  {4312 ,"Xi'_c0: "},
  {4403 ,"cc_1: "},
  {-4132 ,"anti-Xi_c0: "},
  {-100421 ,"anti-D(2S)0: "},
  {9000111 ,"a_00: "},
  {2214 ,"Delta+: "},
  {3103 ,"sd_1: "},
  {-100211 ,"pi(2S)-: "},
  {5503 ,"bb_1: "},
  {327 ,"K_3*+: "},
  {-5303 ,"anti-bs_1: "},
  {4303 ,"cs_1: "},
  {-5203 ,"anti-bu_1: "},
  {100333 ,"phi(1680): "},
  {4222 ,"Sigma_c++: "},
  {30363 ,"Xss: "},
  {-4434 ,"anti-Omega_cc*-: "},
  {-3103 ,"anti-sd_1: "},
  {34 ,"W'+: "},
  {13214 ,"Sigma(1670)0: "},
  {5103 ,"bd_1: "},
  {-511 ,"anti-B0: "},
  {333 ,"phi: "},
  {511 ,"B0: "},
  {90 ,"system: "},
  {4314 ,"Xi_c*0: "},
  {-4324 ,"anti-Xi_c*-: "},
  {23122 ,"Lambda(1600)0: "},
  {-3124 ,"anti-Lambda(1520)0: "},
  {-533 ,"anti-B_s*0: "},
  {25 ,"Higgs0: "},
  {100421 ,"D(2S)0: "},
  {4424 ,"Xi_cc*++: "},
  {-41 ,"anti-R0: "},
  {30443 ,"psi(3770): "},
  {-525 ,"B_2*-: "},
  {-3324 ,"anti-Xi*0: "},
  {213 ,"rho+: "},
  {100323 ,"K'*+: "},
  {41 ,"R0: "},
  {30353 ,"Xsu: "},
  {-30353 ,"anti-Xsu: "},
  {-3112 ,"anti-Sigma+: "},
  {4124 ,"Lambda_c(2625)+: "},
  {4322 ,"Xi'_c+: "},
  {-317 ,"anti-K_3*0: "},
  {3124 ,"Lambda(1520)0: "},
  {-10531 ,"anti-B_s0*0: "},
  {3216 ,"Sigma(1775)0: "},
  {-15 ,"tau+: "},
  {-3222 ,"anti-Sigma-: "},
  {319 ,"K_4*0: "},
  {-20413 ,"D'_1-: "},
  {525 ,"B_2*+: "},
  {-1000020030 ,"anti-He3: "},
  {-20313 ,"anti-K'_10: "},
  {-9042413 ,"Z(4430)-: "},
  {415 ,"D_2*+: "},
  {23 ,"Z0: "},
  {3322 ,"Xi0: "},
  {445 ,"chi_c2: "},
  {10531 ,"B_s0*0: "},
  {-5334 ,"anti-Omega_b*+: "},
  {30343 ,"Xsd: "},
  {5403 ,"bc_1: "},
  {-5403 ,"anti-bc_1: "},
  {4324 ,"Xi_c*+: "},
  {-5222 ,"anti-Sigma_b-: "},
  {5301 ,"bs_0: "},
  {20553 ,"chi_b1: "},
  {4332 ,"Omega_c0: "},
  {110551 ,"chi_b0(2P): "},
  {-421 ,"anti-D0: "},
  {-30343 ,"anti-Xsd: "},
  {-4203 ,"anti-cu_1: "},
  {311 ,"K0: "},
  {100443 ,"psi(2S): "},
  {-515 ,"anti-B_2*0: "},
  {13 ,"mu-: "},
  {-5312 ,"anti-Xi'_b+: "},
  {-1000010030 ,"anti-tritium: "},
  {2 ,"u: "},
  {5324 ,"Xi_b*0: "},
  {2101 ,"ud_0: "},
  {-5401 ,"anti-bc_0: "},
  {317 ,"K_3*0: "},
  {-2101 ,"anti-ud_0: "},
  {30213 ,"rho(3S)+: "},
  {-4432 ,"anti-Omega_cc-: "},
  {3222 ,"Sigma+: "},
  {-4112 ,"anti-Sigma_c0: "},
  {4212 ,"Sigma_c+: "},
  {1000020030 ,"He3: "},
  {-3122 ,"anti-Lambda0: "},
  {421 ,"D0: "},
  {20555 ,"Upsilon_2(1D): "},
  {9000443 ,"psi(4040): "},
  {350 ,"B_s0L: "},
  {5312 ,"Xi'_b-: "},
  {-4322 ,"anti-Xi'_c-: "},
  {-5201 ,"anti-bu_0: "},
  {-44 ,"Xu-: "},
  {4112 ,"Sigma_c0: "},
  {-100323 ,"K'*-: "},
  {99 ,"table: "},
  {35 ,"Higgs'0: "},
  {-5301 ,"anti-bs_0: "},
  {3112 ,"Sigma-: "},
  {-5214 ,"anti-Sigma_b*0: "},
  {-10513 ,"anti-B_10: "},
  {-3 ,"anti-s: "},
  {-5322 ,"anti-Xi'_b0: "},
  {10551 ,"chi_b0: "},
  {10413 ,"D_1+: "},
  {443 ,"J/psi: "},
  {221 ,"eta: "},
  {81 ,"specflav: "},
  {-24 ,"W-: "},
  {14 ,"nu_mu: "},
  {2212 ,"p+: "},
  {-3224 ,"anti-Sigma*-: "},
  {4334 ,"Omega_c*0: "},
  {5314 ,"Xi_b*-: "},
  {111 ,"pi0: "},
  {-433 ,"D_s*-: "},
  {9000211 ,"a_0+: "},
  {18 ,"nu_L: "},
  {4232 ,"Xi_c+: "},
  {10321 ,"K_0*+: "},
  {-100213 ,"rho(2S)-: "},
  {551 ,"eta_b: "},
  {3303 ,"ss_1: "},
  {-1 ,"anti-d: "},
  {-5314 ,"anti-Xi_b*+: "},
  {-313 ,"anti-K*0: "},
  {-531 ,"anti-B_s0: "},
  {1114 ,"Delta-: "},
  {-2112 ,"anti-n0: "},
  {-34 ,"W'-: "},
  {10541 ,"B_c0*+: "},
  {535 ,"B_s2*0: "},
  {9920443 ,"X_1(3872): "},
  {-4222 ,"anti-Sigma_c--: "},
  {-535 ,"anti-B_s2*0: "},
  {2224 ,"Delta++: "},
  {120555 ,"Upsilon_2(2D): "},
  {10441 ,"chi_c0: "},
  {43 ,"Xu0: "},
  {-4312 ,"anti-Xi'_c0: "},
  {555 ,"chi_b2: "},
  {20543 ,"B'_c1+: "},
  {1000010020 ,"deuteron: "},
  {-53122 ,"anti-Lambda(1810)0: "},
  {543 ,"B_c*+: "},
  {545 ,"B_c2*+: "},
  {3314 ,"Xi*-: "},
  {9030221 ,"f_0(1500): "},
  {-1114 ,"anti-Delta+: "},
  {3212 ,"Sigma0: "},
  {9042413 ,"Z(4430)+: "},
  {541 ,"B_c+: "},
  {533 ,"B_s*0: "},
  {100113 ,"rho(2S)0: "},
  {-1000020040 ,"anti-alpha: "},
  {1103 ,"dd_1: "},
  {3312 ,"Xi-: "},
  {3126 ,"Lambda(1820)0: "},
  {-5212 ,"anti-Sigma_b0: "},
  {-5112 ,"anti-Sigma_b+: "},
  {4101 ,"cd_0: "},
  {-4303 ,"anti-cs_1: "},
  {10323 ,"K_1+: "},
  {-37 ,"Higgs-: "},
  {-10311 ,"anti-K_0*0: "},
  {-4314 ,"anti-Xi_c*0: "},
  {-3312 ,"anti-Xi+: "},
  {-100423 ,"anti-D*(2S)0: "},
  {-1103 ,"anti-dd_1: "},
  {-7 ,"anti-b': "},
  {-9000211 ,"a_0-: "},
  {-16 ,"anti-nu_tau: "},
  {32 ,"Z'0: "},
  {21 ,"g: "},
  {16 ,"nu_tau: "},
  {4 ,"c: "},
  {100557 ,"Upsilon_3(2D): "},
  {13124 ,"Lambda(1690)0: "},
  {9010221 ,"f_0: "},
  {100111 ,"pi(2S)0: "},
  {4214 ,"Sigma_c*+: "},
  {23212 ,"Sigma(1750)0: "},
  {-10413 ,"D_1-: "},
  {-5101 ,"anti-bd_0: "},
  {-10543 ,"B_c1-: "},
  {100411 ,"D(2S)+: "},
  {-4214 ,"anti-Sigma_c*-: "},
  {-5122 ,"anti-Lambda_b0: "},
  {30113 ,"rho(3S)0: "},
  {-5503 ,"anti-bb_1: "},
  {5112 ,"Sigma_b-: "},
  {-23212 ,"anti-Sigma(1750)0: "},
  {-4224 ,"anti-Sigma_c*--: "},
  {4224 ,"Sigma_c*++: "},
  {530 ,"B_s0H: "},
  {-17 ,"L+: "},
  {-5232 ,"anti-Xi_b0: "},
  {22 ,"gamma: "},
  {30643 ,"Xdd: "},
  {-30643 ,"anti-Xdd: "},
  {30653 ,"Xdu+: "},
  {-30653 ,"anti-Xdu-: "},
  {9000113 ,"pi(1)(1400)0: "},
  {9000213 ,"pi(1)(1400)+: "},
  {9020221 ,"eta(1405): "},
  {10111 ,"a(0)(1450)0: "},
  {10211 ,"a(0)(1450)+: "},
  {100331 ,"eta(1475): "},
  {9010113 ,"pi(1)(1600)0: "},
  {9010213 ,"pi(1)(1600)+: "},
  {10225 ,"eta(2)(1645): "},
  {227 ,"omega(3)(1670): "},
  {10115 ,"pi(2)(1670)0: "},
  {10215 ,"pi(2)(1670)+: "},
  {117 ,"rho(3)(1690)0: "},
  {217 ,"rho(3)(1690)+: "},
  {10331 ,"f(0)(1710): "},
  {9010111 ,"pi(1800)0: "},
  {9010211 ,"pi(1800)+: "},
  {337 ,"phi(3)(1850): "},
  {9050225 ,"f(2)(1950): "},
  {9060225 ,"f(2)(2010): "},
  {119 ,"a(4)(2040)0: "},
  {219 ,"a(4)(2040)+: "},
  {229 ,"f(4)(2050): "},
  {9080225 ,"f(2)(2300): "},
  {9090225 ,"f(2)(2340): "},
  {10315 ,"K(2)(1770)0: "},
  {10325 ,"K(2)(1770)+: "},
  {20315 ,"K(2)(1820)0: "},
  {20325 ,"K(2)(1820)+: "},
  {9010553 ,"Upsilon(6S): "},
  {-9052513 ,"Zb(10610)-: "},
  {-9152513 ,"Zb(10650)-: "},
  {9052513 ,"Zb(10610)+: "},
  {9152513 ,"Zb(10650)+: "},
  {88511 ,"B0long: "},
  {-88511 ,"anti-B0long: "},
  {99513 ,"B0heavy: "},
  {-99513 ,"anti-B0heavy: "},
  {99523 ,"B*+nospin: "},
  {-99523 ,"B*-nospin: "},
  {313 ,"K*L: "},
  {313 ,"K*S: "},
  {-313 ,"K*BL: "},
  {-313 ,"K*BS: "},
  {313 ,"K*0T: "},
  {-313 ,"anti-K*0T: "},
  {-313 ,"K*BR: "},
  {313 ,"K*0R: "},
  {-10311 ,"anti-K_0*0N: "},
  {10311 ,"K_0*0N: "},
  {511 ,"B0"},
  {-511 ,"anti-B0"},
  {521 ,"B+"},
  {-521 ,"B-"},
  {531 ,"B_s0"},
  {-531 ,"anti-B_s0"},
  {541 ,"B_c+"},
  {-541 ,"B_c-"},
  {551 ,"eta_b"},
  {10553 ,"h_b"},
  {5112 ,"Sigma_b-"},
  {-5112 ,"anti-Sigma_b+"},
  {5122 ,"Lambda_b0"},
  {-5122 ,"anti-Lambda_b0"},
  {5332 ,"Omega_b-"},
  {-5332 ,"anti-Omega_b+"},
  {5132 ,"Xi_b-"},
  {-5132 ,"anti-Xi_b+"},
  {5232 ,"Xi_b0"},
  {-5232 ,"anti-Xi_b0"},
  {443 ,"J/psi"},
  {10441 ,"chi_c0"},
  {20443 ,"chi_c1"},
  {445 ,"chi_c2"},
  {100443 ,"psi(2S)"},
  {30443 ,"psi(3770)"},
  {413 ,"D*+"},
  {-413 ,"D*-"},
  {423 ,"D*0"},
  {-423 ,"anti-D*0"},
  {421 ,"D0"},
  {-421 ,"anti-D0"},
  {411 ,"D+"},
  {-411 ,"D-"},
  {431 ,"D_s+"},
  {-431 ,"D_s-"},
  {4122 ,"Lambda_c+"},
  {-4122 ,"anti-Lambda_c-"},
  {-15 ,"tau+"},
  {15 ,"tau-"},
  {553 ,"Upsilon"},
  {100553 ,"Upsilon(2S)"},
  {200553 ,"Upsilon(3S)"},
  {300553 ,"Upsilon(4S)"},
  {9000553 ,"Upsilon(5S)"},
  {9920443 ,"X_1(3872)"},
  {10443 ,"h_c"},
  {3222 ,"Sigma+"},
  {-3222 ,"anti-Sigma-"},
  {3122 ,"Lambda0"},
  {-3122 ,"anti-Lambda0"},
  {10513 ,"B_10"},
};
/*std::map <int, std::string> pdgNames{
  {0, "none"},
  {1, "d"},
  {2, "u"},
  {3, "s"},
  {4, "c"},
  {5, "b"},
  {6, "t"},
  {11, "e-"},
  {12, "nu_e"},
  {13, "mu-"},
  {14, "nu_mu"},
  {15, "tau-"},
  {16, "nu_tau"},
  {21, "gluon"},
  {22, "gamma"},
  {23, "Z0"},
  {24, "W+"},
  {25, "H0/h0"},
  {32, "Z'"},
  {33, "Z''"},
  {34, "W'"},
  {35, "H0/H2"},
  {36, "A0/H3"},
  {37, "H+"},
  {39, "G"},
  {41, "R"},
  {42, "LQ'"},
  {111, "pi0"},
  {113, "rho0"},
  {130, "K0L"},
  {211, "pi+"},
  {213, "rho+"},
  {221, "eta"},
  {223, "omega"},
  {310, "K0S"},
  {311, "K0"},
  {321, "K+"},
  {323, "K*+"},
  {331, "eta'"},
  {411, "D+"},
  {413, "D*0"},
  {415, "D2*+"},
  {421, "D0"},
  {423, "D*+"},
  {425, "D*02"},
  {431, "Ds+"},
  {433, "Ds*+"},
  {441, "eta_c"},
  {443, "J/psi"},
  {511, "B0"},
  {513, "B*0"},
  {521, "B+"},
  {523, "B*+"},
  {531, "B0s"},
  {533, "Bs*0"},
  {541, "Bc+"},
  {543, "Bc*+"},
  {2101, "ud_0"},
  {2203, "uu_1"},
  {4112, "Sigma_c 0"},
  {4114, "Sigma_c * 0"},
  {4122, "Lambda_c +"},
  {4124, "Lambda_c(2625) +"},
  {4132, "Csi_c 0"},
  {4212, "Sigma_c +"},
  {4214, "Sigma_c 0"},
  {4222, "Sigma c ++"},
  {4224, "Sigma c * ++"},
  {4232, "Csi_c +"},
  {4312, "Csi_c ' 0"},
  {4314, "Csi_c * 0"},
  {5112, "Sigma_b -"},
  {5114, "Sigma_b * -"},
  {5122, "Lambda_b 0"},
  {5132, "Csi_b -"},
  {5214, "Sigma_b * 0"},
  {5222, "Sigma b+"},
  {5232, "Csi_b 0"},
  {5332, "Omega b -"},


  {-1, "d bar"},
  {-2, "u bar"},
  {-3, "s bar"},
  {-4, "c bar"},
  {-5, "b bar"},
  {-6, "t bar"},
  {-11, "e+"},
  {-12, "nu_e bar"},
  {-13, "mu+"},
  {-14, "nu_mu bar"},
  {-15, "tau+"},
  {-16, "nu_tau bar"},
  {-22, "gamma"},
  {-24, "W-"},
  {-34, "W'"},
  {-37, "H-"},
  {-111, "pi0 bar"},
  {-113, "rho0 bar"},
  {-130, "K0L bar"},
  {-211, "pi-"},
  {-213, "rho-"},
  {-221, "eta bar"},
  {-223, "omega bar"},
  {-310, "K0S bar"},
  {-311, "K0 bar"},
  {-321, "K-"},
  {-323, "K*-"},
  {-331, "eta' bar"},
  {-411, "D-"},
  {-413, "D*0 bar"},
  {-415, "D2*-"},
  {-421, "D0 bar"},
  {-423, "D*-"},
  {-425, "D*02 bar"},
  {-431, "Ds-"},
  {-433, "Ds*-"},
  {-441, "eta_c"},
  {-443, "J/psi bar"},
  {-511, "B0 bar"},
  {-513, "B*0 bar"},
  {-541, "Bc-"},
  {-543, "Bc*-"},
  {-521, "B-"},
  {-523, "B*-"},
  {-531, "B0s bar"},
  {-533, "Bs*0 bar"},
  {-531, "Bs0 bar"},
  {-2101, "ubar dbar 0"},
  {-2203, "ubar ubar 1"},


  {-4112, "Sigma_c 0 bar"},
  {-4114, "Sigma_c *0 bar"},
  {-4122, "Lambda_c -"},
  {-4124, "Lambda_c(2625) -"},
  {-4132, "Csi_c 0 bar"},
  {-4212, "Sigma_c -"},
  {-4214, "Sigma_c 0 bar"},
  {-4222, "Sigma c --"},
  {-4224, "Sigma c * --"},
  {-4232, "Csi_c -"},
  {-4312, "Csi_c ' 0 bar"},
  {-4314, "Csi_c * 0 bar"},
  {-5112, "Sigma_b +"},
  {-5114, "Sigma_b * +"},
  {-5122, "Lambda_b 0 bar"},
  {-5132, "Csi_b +"},
  {-5214, "Sigma_b * 0 bar"},
  {-5222, "Sigma b -"},
  {-5232, "Csi_b 0 bar"},
  {-5332, "Omega b +"},
  {4224,"Sigma*_c++"},
  {-4224,"Sigma*_cbar--"},
  {-5312,"Xi'_bbar+"},
  {2112,"n0"},
  {-2112,"nbar0"},
  {5312,"Xi'_b-"},
  {32,"Z'"},
  {-5424,"Xi*_bcbar-"},
  {1000016,"~nu_tauL"},
  {-2224,"Deltabar--"},
  {4432,"Omega_cc+"},
  {-3312,"Xibar+"},
  {16,"nu_tau"},
  {4112,"Sigma_c0"},
  {-5232,"Xi_bbar0"},
  {2000016,"~nu_tauR"},
  {-2000016,"~nu_tauRbar"},
  {5232,"Xi_b0"},
  {9900016,"nu_Rtau"},
  {-4112,"Sigma_cbar0"},
  {-16,"nu_taubar"},
  {3312,"Xi-"},
  {-4432,"Omega_ccbar-"},
  {2224,"Delta++"},
  {-1000016,"~nu_tauLbar"},
  {5424,"Xi*_bc+"},
  {-24,"W-"},
  {5224,"Sigma*_b+"},
  {-3224,"Sigma*bar-"},
  {-4312,"Xi'_cbar0"},
  {-3112,"Sigma-"},
  {-5512,"Xi_bb-"},
  {4232,"Xi_c+"},
  {8,"t',quark"},
  {5112,"Sigma_bbar+"},
  {5432,"Omega'_bcba"},
  {4424,"Xi*_cc++"},
  {1000020040,"Alpha-(He4)"},
  {-4424,"Xi*_ccbar--"},
  {-5432,"Omega'_bc0"},
  {-5112,"Sigma_b-"},
  {-8,"t'bar,quark,"},
  {-4232,"Xi_cbar-"},
  {5512,"Xi_bbbar+"},
  {5544,"Omega*_bbcbar0"},
  {3112,"Sigmabar+"},
  {4312,"Xi'_c0"},
  {3224,"Sigma*+"},
  {9900440,"J/psi_di"},
  {-5224,"Sigma*_bbar-"},
  {-104,"He3"},
  {24,"W+"},
  {-4000012,"nu*_ebar0"},
  {-5132,"Xi_bbar+"},
  {-12,"nu_ebar"},
  {-2000012,"~nu_eRbar"},
  {-3212,"Sigmabar0"},
  {4212,"Sigma_c+"},
  {-5324,"Xi*_bbar0"},
  {-1000012,"~nu_eLbar"},
  {5524,"Xi*_bb0"},
  {-4332,"Omega_cbar0"},
  {5332,"Omega_b-"},
  {84,"c-hadron,quark"},
  {-4412,"Xi_ccbar-"},
  {5444,"Omega*_bcc+"},
  {4,"c,quark"},
  {-3324,"Xi*bar0"},
  {2000004,"~c_R,quark"},
  {-5532,"Omega_bbbar+"},
  {4324,"Xi*_c+"},
  {-5212,"Sigma_bbar0"},
  {2212,"p+"},
  {-4444,"Omega*_cccbar-"},
  {4132,"Xi_c0"},
  {36,"A0"},
  {5412,"Xi'_bc0"},
  {1000010020,"Deuterium"},
  {-5412,"Xi'_bcbar0"},
  {-4132,"Xi_cbar0"},
  {4444,"Omega*_ccc++"},
  {-2212,"pbar-"},
  {5212,"Sigma_b0"},
  {-4324,"Xi*_cbar-"},
  {5532,"Omega_bb-"},
  {-100,"deuteron"},
  {3324,"Xi*0"},
  {-4,"cbar,quark,"},
  {-5444,"Omega*_bccbar-"},
  {9900220,"omega_di"},
  {4412,"Xi_cc+"},
  {-84,"c-hadronbar,quark,"},
  {-5332,"Omega_bbar+"},
  {9900012,"nu_Re"},
  {4332,"Omega_c0"},
  {-5524,"Xi*_bbbar0"},
  {1000012,"~nu_eL"},
  {5324,"Xi*_b0"},
  {-4212,"Sigma_cbar-"},
  {3212,"Sigma0"},
  {2000012,"~nu_eR"},
  {12,"nu_e"},
  {5132,"Xi_b-"},
  {4000012,"nu*_e0"},
  {5242,"Xi_bc+"},
  {-2000006,"~t_2bar,quark,"},
  {3322,"Xi0"},
  {-3334,"Omegabar+"},
  {5114,"Sigma*_b-"},
  {-6,"tbar,quark,"},
  {-1000006,"~t_1bar,quark,"},
  {-4422,"Xi_ccbar--"},
  {5434,"Omega*_bc0"},
  {4122,"Lambda_c+"},
  {-5222,"Sigma_bbar-"},
  {-102,"alpha"},
  {-2214,"Deltabar-"},
  {-5542,"Omega_bbcbar0"},
  {1114,"Delta-"},
  {4314,"Xi*_c0"},
  {-5414,"Xi*_bcbar0"},
  {5322,"Xi'_b0"},
  {5514,"Xi*_bb-"},
  {-4214,"Sigma*_cbar-"},
  {9900042,"H_R++"},
  {-5142,"Xi_bcbar0"},
  {-3222,"Sigmabar-"},
  {-5334,"Omega*_bbar+"},
  {3114,"Sigma*-"},
  {-5534,"Omega*_bbbar+"},
  {4322,"Xi'_c+"},
  {-5214,"Sigma*_bbar0"},
  {34,"W'+i"},
  {-5342,"Omega_bcbar0"},
  {2,"u,quark"},
  {5122,"Lambda_b0"},
  {4000002,"u*i,quark"},
  {130,"K_L0"},
  {-4222,"Sigma_cbar--"},
  {5442,"Omega_bcc+"},
  {2114,"Delta0"},
  {-4414,"Xi*_ccbar-"},
  {5314,"Xi*_b-"},
  {3122,"Lambda0"},
  {9900210,"pi_diffr+"},
  {-1000014,"~nu_muLbar"},
  {5554,"Omega*_bbb-"},
  {-3214,"Sigma*bar0"},
  {-2000014,"~nu_muRbar"},
  {-14,"nu_mubar"},
  {3314,"Xi*-"},
  {-5422,"Xi'_bcbar-"},
  {4434,"Omega*_cc+"},
  {-4334,"Omega*_cbar0"},
  {4114,"Sigma*_c0"},
  {18,"nu'_tau"},
  {5522,"Xi_bb0"},
  {110,"reggeon"},
  {-5522,"Xi_bbbar0"},
  {-18,"nu'_taubar"},
  {9900014,"nu_Rmu"},
  {-4114,"Sigma*_cbar0"},
  {4334,"Omega*_c0"},
  {-4434,"Omega*_ccbar-"},
  {1000010030,"Tritium"},
  {5422,"Xi'_bc+"},
  {-3314,"Xi*bar+"},
  {14,"nu_mu"},
  {2000014,"~nu_muR"},
  {3214,"Sigma*0"},
  {-5554,"Omega*_bbbbar+"},
  {1000014,"~nu_muL"},
  {-9900210,"pi_diffr-"},
  {-3122,"Lambdabar0"},
  {-5314,"Xi*_bbar+"},
  {4414,"Xi*_cc+"},
  {1000020030,"Helium3"},
  {-1000002,"~u_Lbar,quark,"},
  {-2114,"Deltabar0"},
  {-5442,"Omega_bccbar-"},
  {4222,"Sigma_c++"},
  {-2000002,"~u_Rbar,quark,"},
  {-4000002,"u*bar,quark,"},
  {-5122,"Lambda_bbar0"},
  {-2,"ubar,quark,"},
  {5342,"Omega_bc0"},
  {990,"pomeron"},
  {-34,"W'-"},
  {5214,"Sigma*_b0"},
  {94,"CMshower"},
  {-4322,"Xi'_cbar-"},
  {5534,"Omega*_bb-"},
  {-3114,"Sigma*bar+"},
  {5334,"Omega*_b-"},
  {3222,"Sigma+"},
  {22,"gamma"},
  {5142,"Xi_bc0"},
  {-9900042,"H_R--i"},
  {4214,"Sigma*_c+"},
  {-5514,"Xi*_bbbar+"},
  {310,"K_S0"},
  {-5322,"Xi'_bbar0"},
  {5414,"Xi*_bc0"},
  {-4314,"Xi*_cbar0"},
  {-1114,"Deltabar+"},
  {5542,"Omega_bbc0"},
  {2214,"Delta+"},
  {5222,"Sigma_b+"},
  {-4122,"Lambda_cbar-"},
  {-5434,"Omega*_bcbar0"},
  {4422,"Xi_cc++"},
  {1000006,"~t_1,quark"},
  {6,"t,quark"},
  {-5114,"Sigma*_bbar+"},
  {3334,"Omega-"},
  {-3322,"Xibar0"},
  {2000006,"~t_2,quark"},
  {-5242,"Xi_bcbar-"},
  {445,"chi_2c"},
  {20413,"D*_1+"},
  {-323,"K*-"},
  {-3203,"su_1bar,diquark,"},
  {-515,"B*_2bar0"},
  {-3,"sbar,quark"},
  {-10531,"B*_0sbar0"},
  {221,"eta"},
  {10333,"h'_1"},
  {541,"B_c+"},
  {3101,"sd_0,diquark,"},
  {-10211,"a_0-"},
  {-20323,"K*_1-"},
  {413,"D*+"},
  {3000221,"pi'_tc0"},
  {4000013,"mu*-"},
  {13,"mu-"},
  {525,"B*_2+"},
  {2000013,"~mu_R-"},
  {-435,"D*_2s-"},
  {1000013,"~mu_L-"},
  {333,"phi"},
  {4301,"cs_0,diquark"},
  {-4403,"cc_1bar,diquark,"},
  {10413,"D_1+"},
  {-5203,"bu_1bar,diquark,"},
  {-10323,"K_1-"},
  {10541,"B*_0c+"},
  {-211,"pi-"},
  {-531,"B_sbar0"},
  {10221,"f_0"},
  {5101,"bd_0,diquark,"},
  {-3000211,"pi_tc-"},
  {20333,"f'_1"},
  {10533,"B_1s0"},
  {37,"H+"},
  {421,"D0"},
  {-411,"D-"},
  {1000037,"~chi_2+"},
  {-2203,"uu_1bar,diquark,"},
  {-5403,"bc_1bar,diquark,"},
  {-10523,"B_1-"},
  {10213,"b_1+"},
  {-315,"K*_2bar0"},
  {325,"K*_2+"},
  {5,"b,quark,"},
  {4101,"cd_0,diquark"},
  {2000005,"~b_2,quark,"},
  {20213,"a_1+"},
  {-4000011,"e*bar+"},
  {-523,"B*-"},
  {-11,"e+"},
  {-2000011,"~e_R+"},
  {20533,"B*_1s0"},
  {2101,"ud_0,diquark"},
  {10421,"D*_00"},
  {5301,"bs_0,diquark,"},
  {-1000011,"~e_L+"},
  {-4203,"cu_1bar,diquark,"},
  {3000213,"rho_tc+"},
  {21,"g"},
  {533,"B*_s0"},
  {-20523,"B*_1-"},
  {213,"rho+"},
  {1000021,"~g"},
  {85,"b-hadron,quark,"},
  {-10411,"D*_0-"},
  {3000113,"rho_tc0"},
  {-4303,"cs_1bar,diquark,"},
  {-1103,"dd_1bar,diquark"},
  {-1000015,"~tau_1+"},
  {433,"D*_s+"},
  {3400113,"rho_22_tc"},
  {-2000015,"~tau_2+"},
  {3200113,"rho_12_tc"},
  {113,"rho0"},
  {-15,"tau+"},
  {-10511,"B*_0bar0"},
  {-4000015,"tau*bar+"},
  {20433,"D*_1s+"},
  {3100113,"rho_11_tc"},
  {-10543,"B_1c-"},
  {-431,"D_s-"},
  {10321,"K*_0+"},
  {5201,"bu_0,diquark"},
  {3300113,"rho_21_tc"},
  {10513,"B_10"},
  {-5103,"bd_1bar,diquark"},
  {17,"tau'-"},
  {20113,"a_10"},
  {1,"d,quark,"},
  {-511,"Bbar0"},
  {513,"B*0"},
  {4000001,"d*,quark,"},
  {-5503,"bb_1bar,diquark"},
  {3201,"su_0,diquark"},
  {2000001,"~d_R,quark,"},
  {10113,"b_10"},
  {-10431,"D*_0s-"},
  {321,"K+"},
  {1000001,"~d_L,quark,"},
  {-20543,"B*_1c-"},
  {10433,"D_1s+"},
  {545,"B*_2c+"},
  {20513,"B*_10"},
  {33,"Z0"},
  {225,"f_2"},
  {-3103,"sd_1bar,diquark"},
  {-543,"B*_c-"},
  {-415,"D*_2-"},
  {25,"h0"},
  {-3303,"ss_1bar,diquark"},
  {10521,"B*_0+"},
  {5401,"bc_0,diquark"},
  {9900441,"cc~[1S08]"},
  {20313,"K*_10"},
  {-423,"D*bar0"},
  {1000025,"~chi_30"},
  {441,"eta_c"},
  {-10311,"K*_0bar0"},
  {-20423,"D*_1bar0"},
  {313,"K*0"},
  {10553,"h_1b"},
  {-4103,"cd_1bar,diquark,"},
  {-7,"b'bar,diquark"},
  {-535,"B*_2sbar0"},
  {4201,"cu_0,diquark"},
  {-215,"a_2-"},
  {553,"Upsilon"},
  {41,"R0"},
  {425,"D*_20"},
  {9910441,"cc~[3P08]"},
  {9900041,"H_L++"},
  {521,"B+"},
  {9900553,"bb~[3S18]"},
  {100553,"Upsilon'"},
  {10441,"chi_0c"},
  {-311,"Kbar0"},
  {-2103,"ud_1bar,diquark,"},
  {-5303,"bs_1bar,diquark"},
  {-10423,"D_1bar0"},
  {20553,"chi_1b"},
  {10313,"K_10"},
  {-10313,"K_1bar0"},
  {10423,"D_10"},
  {5303,"bs_1,diquark,"},
  {2103,"ud_1,diquark"},
  {10551,"chi_0b"},
  {311,"K0"},
  {-521,"B-"},
  {-9900041,"H_L--"},
  {9900023,"Z_R0"},
  {-425,"D*_2bar0"},
  {-41,"Rbar0"},
  {215,"a_2+"},
  {-4201,"cu_0bar,diquark,"},
  {9910551,"bb~[3P08]"},
  {23,"Z0"},
  {535,"B*_2s0"},
  {7,"b',quark,"},
  {4103,"cd_1,diquark"},
  {9900551,"bb~[1S08]"},
  {-313,"K*bar0"},
  {20423,"D*_10"},
  {10311,"K*_00"},
  {551,"eta_b"},
  {39,"Graviton"},
  {423,"D*0"},
  {-20313,"K*_1bar0"},
  {1000039,"~Gravitino"},
  {5000039,"Graviton*"},
  {-5401,"bc_0bar,diquark,"},
  {-10521,"B*_0-"},
  {3303,"ss_1,diquark,"},
  {3000223,"omega_tc"},
  {415,"D*_2+"},
  {543,"B*_c+"},
  {3103,"sd_1,diquark,"},
  {223,"omega"},
  {-20513,"B*_1bar0"},
  {-545,"B*_2c-"},
  {-10433,"D_1s-"},
  {20543,"B*_1c+"},
  {-1000001,"~d_Lbar,quark"},
  {-321,"K-"},
  {10431,"D*_0s+"},
  {10111,"a_00"},
  {-2000001,"~d_Rbar,quark"},
  {-3201,"su_0bar,diquark,"},
  {5503,"bb_1,diquark,"},
  {-4000001,"d*bar,quark"},
  {20223,"f_1"},
  {-513,"B*bar0"},
  {511,"B0"},
  {-1,"dbar,quark"},
  {3200111,"pi_22_8_tc"},
  {111,"pi0"},
  {-17,"tau'+"},
  {10223,"h_1"},
  {5103,"bd_1,diquark,"},
  {-10513,"B_1bar0"},
  {-5201,"bu_0bar,diquark,"},
  {-10321,"K*_0-"},
  {431,"D_s+"},
  {10543,"B_1c+"},
  {3000111,"pi_tc0"},
  {-20433,"D*_1s-"},
  {4000015,"tau*-"},
  {10511,"B*_00"},
  {15,"tau-"},
  {2000015,"~tau_2-"},
  {-433,"D*_s-"},
  {1000015,"~tau_1-"},
  {1103,"dd_1,diquark,"},
  {335,"f'_2"},
  {4303,"cs_1,diquark"},
  {3100111,"pi_22_1_tc"},
  {3100331,"eta_tc0"},
  {10411,"D*_0+"},
  {-213,"rho-"},
  {555,"chi_2b"},
  {20523,"B*_1+"},
  {-533,"B*_sbar0"},
  {-3000213,"rho_tc-"},
  {4203,"cu_1,diquark"},
  {1000011,"~e_L-"},
  {-5301,"bs_0bar,diquark"},
  {-10421,"D*_0bar0"},
  {331,"eta'"},
  {10443,"h_1c"},
  {-2101,"ud_0bar,diquark,"},
  {-20533,"B*_1sbar0"},
  {2000011,"~e_R-"},
  {11,"e-"},
  {523,"B*+"},
  {4000011,"e*-"},
  {-20213,"a_1-"},
  {-2000005,"~b_2bar,diquark"},
  {-4101,"cd_0bar,diquark,"},
  {-5,"bbar,quark"},
  {443,"J/psi"},
  {-325,"K*_2-"},
  {315,"K*_20"},
  {-10213,"b_1-"},
  {10523,"B_1+"},
  {5403,"bc_1,diquark"},
  {2203,"uu_1,diquark"},
  {9900443,"cc~[3S18]"},
  {411,"D+"},
  {-101,"triton"},
  {-421,"Dbar0"},
  {100443,"psi'"},
  {10331,"f'_0"},
  {91,"cluster"},
  {20443,"chi_1c"},
  {-37,"H-"},
  {-10533,"B_1sbar0"},
  {3000211,"pi_tc+"},
  {-5101,"bd_0bar,diquark"},
  {531,"B_s0"},
  {211,"pi+"},
  {-10541,"B*_0c-"},
  {10323,"K_1+"},
  {5203,"bu_1,diquark"},
  {-10413,"D_1-"},
  {4403,"cc_1,diquark"},
  {-4301,"cs_0bar,diquark,"},
  {-1000013,"~mu_L+"},
  {435,"D*_2s+"},
  {-2000013,"~mu_R+"},
  {115,"a_20"},
  {-525,"B*_2-"},
  {-13,"mu+"},
  {-4000013,"mu*bar+"},
  {-413,"D*-"},
  {1000035,"~chi_40"},
  {20323,"K*_1+"},
  {10211,"a_0+"},
  {-3101,"sd_0bar,diquark"},
  {-541,"B_c-"},
  {35,"H0"},
  {10531,"B*_0s0"},
  {3,"s,quark,"},
  {515,"B*_20"},
  {2000003,"~s_R,quark,"},
  {3203,"su_1,diquark"},
  {323,"K*+"},
  {-20413,"D*_1-"},
  {1000003,"~s_L,quark,"},

};*/

struct EvtInfo{
    int channel;
    std::array<LorentzVectorM,2> leg_p4;
};

struct ParticleInfo{
  int pdgId;
  int charge;
  std::string name;
  std::string type;
};
ParticleInfo findParticleByPdgIdx(int pdgIdx){
  std::string line;
  ifstream file ("/Users/valeriadamante/Desktop/Dottorato/lxplus/hhbbTauTauRes/Framework/config/pdg_name_type_charge.txt", ios::in );
  std::string name, type;
  int pdg, charge;
  ParticleInfo finalInfo;
  while (getline(file, line)){
    std::stringstream ss(line);

    std::string name, type, pdgid ,charge;
    int pdgid_value;
    int charge_value;
    std::getline(ss,pdgid,',');
    pdgid_value = stoi(pdgid);
    std::getline(ss,name,',');
    std::getline(ss,type,',');
    std::getline(ss,charge,',');
    charge_value = stoi(charge);
    if(pdgid_value==pdgIdx){
      finalInfo.pdgId = pdgid_value;
      finalInfo.name = name;
      finalInfo.type= type;
      finalInfo.charge= charge_value;
    }
    //cout << finalInfo.pdgId << " " <<   finalInfo.name << " " <<   finalInfo.type << " " <<   finalInfo.charge << " " << endl;
  }
  return finalInfo;

}

// utilities

float DeltaPhi(Float_t phi1, Float_t phi2){
    static constexpr float pi = M_PI;
    float dphi = phi1 - phi2;
    if(dphi > pi){
        dphi -= 2*pi;
    }
    else if(dphi <= -pi){
        dphi += 2*pi;
    }
    return dphi;
}

float DeltaEta(Float_t eta1, Float_t eta2){
  return (eta1-eta2);
}

float DeltaR(Float_t phi1,Float_t eta1,Float_t phi2,Float_t eta2) {
  float dphi = DeltaPhi(phi1, phi2);
  float deta = DeltaEta(eta1, eta2);
  return (std::sqrt(deta * deta + dphi * dphi));
}

bool isTauDaughter(int tau_idx, int particle_idx, const vec_i& GenPart_genPartIdxMother){
    bool isTauDaughter = false;
    int idx_mother = GenPart_genPartIdxMother[particle_idx];
    while(1){
        if(idx_mother == -1){
            return false;
        }
        else {
            if(idx_mother==tau_idx){
                return true;
            }
            else{
                int newParticle_index = idx_mother;
                idx_mother = GenPart_genPartIdxMother[newParticle_index];
            }
        }
    }
}

LorentzVectorM GetTauP4(int tau_idx, const vec_f& pt, const vec_f& eta, const vec_f& phi, const vec_f& mass, const vec_i& GenPart_genPartIdxMother, const vec_i& GenPart_pdgId, const vec_i& GenPart_status){
    LorentzVectorM sum(0.,0.,0.,0.);
    LorentzVectorM TauP4;

    for(size_t particle_idx=0; particle_idx< GenPart_pdgId.size(); particle_idx++ ){
        if(GenPart_pdgId[particle_idx] == 12 || GenPart_pdgId[particle_idx] == 14 || GenPart_pdgId[particle_idx] == 16) continue;
        if(GenPart_status[particle_idx]!= 1 ) continue;

        bool isRelatedToTau = isTauDaughter(tau_idx, particle_idx, GenPart_genPartIdxMother);
        if(isRelatedToTau){
            LorentzVectorM current_particleP4 ;
            float p_mass ;
            if (particleMasses.find(GenPart_pdgId[particle_idx]) != particleMasses.end()){
                p_mass=particleMasses.at(GenPart_pdgId[particle_idx]);
            }
            else{
                p_mass = mass[particle_idx];
            }
            current_particleP4=LorentzVectorM(pt[particle_idx], eta[particle_idx], phi[particle_idx],p_mass);
            sum = sum + current_particleP4;
        }

    }
    TauP4=LorentzVectorM(sum.Pt(), sum.Eta(), sum.Phi(), sum.M());
    return TauP4;

}

// Channel selection

std::map<std::string,std::set<int>> GetLeptonIndices(int evt, const vec_i& GenPart_pdgId, const vec_i& GenPart_genPartIdxMother, const vec_i& GenPart_statusFlags){
  std::map<std::string,std::set<int>> lep_indices;
  //lep_indices["Electron"] = std::set<int>;
  std::set<int> e_indices;
  std::set<int> mu_indices;
  std::set<int> tau_indices;

  // start the loop to look for the indices of e, mu and taus
  for(size_t i=0; i< GenPart_pdgId.size(); i++ ){
    // check particle mother has pdg index positive, and that isLastCopy is on
      if(GenPart_genPartIdxMother[i]>=0 && ((GenPart_statusFlags[i])&(1<<13))  ){
        // case 1 : is tau --> need to check that the status on is: isPrompt or isDirectPromptTauDecayProduct or fromHardProcess
          if(std::abs(GenPart_pdgId[i])==15  && ( ( (GenPart_statusFlags[i])&(1<<0) || (GenPart_statusFlags[i])&(1<<5) )  ) && ((GenPart_statusFlags[i])&(1<<8)) ){
              tau_indices.insert(i);
          }
        // case 2 : is NOT tau --> is either electron or muon, need to check that the status on is: isDirectPromptTauDecayProduct or isHardProcessTauDecayProduct or isDirectHardProcessTauDecayProduct
          else if((std::abs(GenPart_pdgId[i])==13 || std::abs(GenPart_pdgId[i])==11) && ( (GenPart_statusFlags[i])&(1<<5) ) && ((GenPart_statusFlags[i])&(1<<9) || (GenPart_statusFlags[i])&(1<<10)) ) {
              int mother_index = GenPart_genPartIdxMother[i];
              // check that the mother is tau
              if( std::abs(GenPart_pdgId[mother_index]) != 15) {
                  std::cout << "event " << evt << " particle " << i << " is " << GenPart_pdgId[i] <<" particle mother " << GenPart_genPartIdxMother[i] << " is " << GenPart_pdgId[GenPart_genPartIdxMother[i]]  << std::endl;
                  throw std::runtime_error("particle mother is not tau");
              }

              // remove the mother from tau indices!!
              tau_indices.erase(GenPart_genPartIdxMother[i]);
              if(std::abs(GenPart_pdgId[i])==11 ){
                  e_indices.insert(i);
              }
              if(std::abs(GenPart_pdgId[i])==13 ){
                  mu_indices.insert(i);

              }
          } // closes if on pdg ids for mu and e
      } // closes if on status flags and have idx mother
  } // closes for on genParticles
  lep_indices["Electron"] = e_indices ;
  lep_indices["Muon"] = mu_indices ;
  lep_indices["Tau"] = tau_indices ;

  return lep_indices;
}

EvtInfo GetEventInfo(int evt, std::map<std::string, std::set<int>>& lep_indices, const vec_i& GenPart_pdgId, const vec_i& GenPart_genPartIdxMother, const vec_i& GenPart_status, const vec_f& GenPart_pt, const vec_f& GenPart_eta, const vec_f& GenPart_phi, const vec_f& GenPart_mass ){
  EvtInfo evt_info;
  // 1. tauTau
  if(lep_indices["Electron"].size()==0 && lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==2 ){

      /* uncomment this part to require tau with pt > 10 GeV*/
      /*
      if(lep_indices["Tau"].size()!=2){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */

      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      // check that there are two taus
      if(lep_indices["Tau"].size()!=2){
          std::cout << "in this event tauTau = " << evt << " there should be 2 taus but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;

          // loop over taus
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          } // end of loop over taus
          throw std::runtime_error("it should be tautau, but tau indices is not 2 ");
      }
      */
      int leg_p4_index = 0;
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices

      evt_info.channel = Channel::tauTau;

  } // closes tauTau Channel

  // 2. muTau
  if(lep_indices["Electron"].size()==0 && (lep_indices["Muon"].size()==1 && lep_indices["Tau"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Tau"].size()!=1){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // 2.1: when there is 1 mu and != 1 tau
      if(lep_indices["Tau"].size()!=1){
          std::cout << "in this event muTau = " << evt << " there should be 1 tau but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          }
          throw std::runtime_error("it should be muTau, but tau indices is not 1 ");
      }

      // 2.2: when there is 1 tau and != 1 mu
      if(lep_indices["Muon"].size()!=1){
          std::cout << "in this event muTau = " << evt << " there should be 1 mu but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          }
          throw std::runtime_error("it should be muTau, but mu indices is not 1 ");
      }
      */
      int leg_p4_index = 0;

      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      evt_info.channel = Channel::muTau;
  } // closes muTau Channel


  // eTau
  if(lep_indices["Muon"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Tau"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Tau"].size()!=1){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // case 1: when there is 1 e and != 1 tau
      if(lep_indices["Tau"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 tau but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but tau indices is not 1 ");
      }

      // case 2: when there is 1 tau and != 1 e
      if(lep_indices["Electron"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 e but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but e indices is not 1 ");
      }
      */
      int leg_p4_index = 0;


      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      evt_info.channel = Channel::eTau;

  } // closes eTau Channel

  // muMu
  if(lep_indices["Electron"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Muon"].size()==2){

      /* uncomment this part to require mu with pt > 10 GeV */
      /*
      if(lep_indices["Muon"].size()!=2){
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              if(GenPart_pt[mu_idx]<10.){
                  lep_indices["Muon"].erase(mu_idx);
              }
          }
      } // closes if on lep_indices["Muon"] size
      */
      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      if(lep_indices["Muon"].size()!=2){
          std::cout << "in this event muTau = " << evt << " there should be 2 mus but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;

          // loop over mus
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          } // end of loop over mus

          throw std::runtime_error("it should be mumu, but mu indices is not 2 ");
      }
      */

      int leg_p4_index = 0;

      // loop over mu_indices

      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices

      evt_info.channel = Channel::muMu;

  } // closes muMu Channel


  // eE
  if(lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Electron"].size()==2){

      /* uncomment this part to require mu with pt > 10 GeV */
      /*
      if(lep_indices["Electron"].size()!=2){
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              if(GenPart_pt[e_idx]<10.){
                  lep_indices["Electron"].erase(e_idx);
              }
          }
      } // closes if on e_indices size
      */
      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      if(lep_indices["Electron"].size()!=2){
          std::cout << "in this event eTau = " << evt << " there should be 2 es but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;

          // loop over es
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          } // end of loop over es

          throw std::runtime_error("it should be ee, but e indices is not 2 ");
      }
      */
      int leg_p4_index = 0;

      // loop over lep_indices["Electron"]

      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      evt_info.channel = Channel::eE;

  } // closes eE Channel

  // eMu
  if(lep_indices["Tau"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Muon"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Muon"].size()!=1){
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              if(GenPart_pt[mu_idx]<10.){
                  lep_indices["Muon"].erase(mu_idx);
              }
          }
      } // closes if on lep_indices["Muon"] size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // case 1: when there is 1 e and != 1 mu
      if(lep_indices["Muon"].size()!=1){
          std::cout << "in this event eMu = " << evt << " there should be 1 mu but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          }
          throw std::runtime_error("it should be eMu, but mu indices is not 1 ");
      }

      // case 2: when there is 1 mu and != 1 e
      if(lep_indices["Electron"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 e but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but e indices is not 1 ");
      }
      */
      int leg_p4_index = 0;

      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices

      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      evt_info.channel = Channel::eMu;

  } // closes eMu Channel

  return evt_info;

}

bool PassAcceptance(const EvtInfo& evt_info){
    for(size_t i =0; i<evt_info.leg_p4.size(); i++){
        if(!(evt_info.leg_p4.at(i).pt()>20 && std::abs(evt_info.leg_p4.at(i).eta())<2.3 )){
            //std::cout << "channel that does not pass acceptance == " << evt_info.channel << std::endl;
            //if(evt_info.channel == Channel::eTau){
            //    std::cout << "pt == " << evt_info.leg_p4.at(i).pt() << "eta == " << evt_info.leg_p4.at(i).eta() << std::endl;
            //}
            return false;
        }
    }
    return true;
}

vec_s RecoEleSelectedIndices(int event, const vec_f& Electron_dz, const vec_f& Electron_dxy, const vec_f& Electron_eta, const vec_f& Electron_phi, const vec_f& Electron_pt, const vec_uc& Electron_mvaFall17V2Iso_WP80){
  vec_s Electron_indices;
  for(size_t i=0; i< Electron_pt.size(); i++ ){
      if(Electron_mvaFall17V2Iso_WP80[i]==1 && Electron_pt[i] > 20 && std::abs(Electron_eta[i])<2.3 && std::abs(Electron_dz[i])<0.2 &&  std::abs(Electron_dxy[i])<0.045 ){
          Electron_indices.push_back(i);
      }
  }
  return Electron_indices;
}

vec_s RecoMuSelectedIndices(int event, const vec_f& Muon_dz,  const vec_f& Muon_dxy, const vec_f& Muon_eta, const vec_f& Muon_phi, const vec_f& Muon_pt, const vec_uc& Muon_tightId, const vec_uc& Muon_highPtId, const vec_f& Muon_tkRelIso,  const vec_f& isolation_variable_muon){
  vec_s Muon_indices;
  for(size_t i=0; i< Muon_pt.size(); i++ ){
      //std::cout << Muon_pt[i] <<"\t"<< Muon_tightId[i] <<"\t"<< Muon_eta[i] <<"\t"<< Muon_dz[i] <<"\t"<< Muon_dxy[i]<<"\t"<< isolation_variable_muon[i] << std::endl;
      if(Muon_pt[i] > 20 && std::abs(Muon_eta[i])<2.3 && std::abs(Muon_dz[i])<0.2 &&  std::abs(Muon_dxy[i])<0.045  && ( ( Muon_tightId[i]==1  && isolation_variable_muon[i]<0.15) || (Muon_highPtId[i]==1 && Muon_tkRelIso[i]<0.1) ) ){
          Muon_indices.push_back(i);
      }
  }
  return Muon_indices;
}

vec_s RecoTauSelectedIndices(int event, EvtInfo& evt_info, const vec_f& Tau_dz, const vec_f& Tau_eta, const vec_f& Tau_phi, const vec_f& Tau_pt, const vec_uc& Tau_idDeepTau2017v2p1VSjet, const vec_uc&  Tau_idDeepTau2017v2p1VSmu, const vec_uc& Tau_idDeepTau2017v2p1VSe, const vec_i& Tau_decayMode ){
  vec_s tau_indices;
  for(size_t i=0; i< Tau_dz.size(); i++ ){
      if(Tau_decayMode[i]!=0 && Tau_decayMode[i]!=1 && Tau_decayMode[i]!=10 && Tau_decayMode[i]!=11) continue;
      if(Tau_pt[i] > 20 && std::abs(Tau_eta[i])<2.3 && std::abs(Tau_dz[i])<0.2){
          if(evt_info.channel == Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<0)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<1)) ){
              tau_indices.push_back(i);
          }
          else if(evt_info.channel != Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<3)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<2))){
              tau_indices.push_back(i);
          }
      }
  }

  return tau_indices;

}


std::vector<std::pair<size_t, size_t>> GetPairs(const vec_s &indices_leg1,const  vec_s &indices_leg2, const vec_f& Phi_leg1, const vec_f& Eta_leg1, const vec_f& Phi_leg2, const vec_f& Eta_leg2){
  std::vector<std::pair<size_t, size_t>> pairs;
  for(size_t i=0; i< indices_leg1.size(); i++ ){
      for(size_t j=0; j< indices_leg2.size(); j++ ){
          size_t cand1 = indices_leg1.at(i);
          size_t cand2 = indices_leg2.at(j);
          float current_dR = DeltaR(Phi_leg1[cand1], Eta_leg1[cand1],Phi_leg2[cand2], Eta_leg2[cand2]);
          //std::cout<<"current_dR = " << current_dR << std::endl;
          if(current_dR > 0.5){
              pairs.push_back(std::make_pair(cand1, cand2));
          }
      }
  }
  return pairs;
}

vec_s GetFinalIndices(const std::vector<std::pair<size_t, size_t>>& pairs, int event, const vec_f& isolation_variable_first_cand, const vec_f& first_cand_pt, const vec_f& isolation_variable_second_cand, const vec_f& second_cand_pt, const vec_i& first_cand_charge,const vec_i& second_cand_charge){
  vec_s final_indices;

  const auto Comparitor = [&](std::pair<size_t, size_t> pair_1, std::pair<size_t, size_t> pair_2) -> bool
  {
     if(pair_1 == pair_2) return false;
     // First prefer the pair with the most isolated candidate 1 (electron for eTau, muon for muTau)
     const size_t first_cand_pair_1 = pair_1.first ;
     const size_t first_cand_pair_2 = pair_2.first ;
     if(first_cand_pair_1 != first_cand_pair_2) {
         auto iso_first_cand_pair_1 = isolation_variable_first_cand.at(first_cand_pair_1);
         auto iso_first_cand_pair_2 = isolation_variable_first_cand.at(first_cand_pair_2);
         int iso_cmp;
         if(iso_first_cand_pair_1 == iso_first_cand_pair_2){ iso_cmp= 0;}
         else {iso_cmp =  iso_first_cand_pair_1 > iso_first_cand_pair_2 ? 1 : -1; }
         if(iso_cmp != 0) {return iso_cmp == 1;}
         // If the isolation of candidate 1 is the same in both pairs, prefer the pair with the highest candidate 1 pt (for cases of genuinely the same isolation value but different possible candidate 1).
         if(first_cand_pt.at(first_cand_pair_1) != first_cand_pt.at(first_cand_pair_2)){
             return first_cand_pt.at(first_cand_pair_1) > first_cand_pt.at(first_cand_pair_2);
         }
         else{
           const size_t second_cand_pair_1 = pair_1.second ;
           const size_t second_cand_pair_2 = pair_2.second ;
           if(second_cand_pair_1 != second_cand_pair_2) {
               auto iso_second_cand_pair_1 = isolation_variable_second_cand.at(second_cand_pair_1);
               auto iso_second_cand_pair_2 = isolation_variable_second_cand.at(second_cand_pair_2);
               int iso_cmp;
               if(iso_second_cand_pair_1 == iso_second_cand_pair_2){ iso_cmp= 0;}
               else {iso_cmp =  iso_second_cand_pair_1 > iso_second_cand_pair_2 ? 1 : -1; }
               if(iso_cmp != 0) {return iso_cmp == 1;}
               //If the isolation of candidate 2 is the same, prefer the pair with highest candidate 2 pt (for cases of genuinely the same isolation value but different possible candidate 2).
               if(second_cand_pt.at(second_cand_pair_1) != second_cand_pt.at(second_cand_pair_2)){
                   return second_cand_pt.at(second_cand_pair_1) > second_cand_pt.at(second_cand_pair_2);
               } // closes if on tau pts
           } // closes if on tau indices
         } // closes else

     } // closes if
     // If the pt of candidate 1 in both pairs is the same (likely because it's the same object) then prefer the pair with the most isolated candidate 2 (tau for eTau and muTau).
     else{
       const size_t second_cand_pair_1 = pair_1.second ;
       const size_t second_cand_pair_2 = pair_2.second ;
       if(second_cand_pair_1 != second_cand_pair_2) {
           auto iso_second_cand_pair_1 = isolation_variable_second_cand.at(second_cand_pair_1);
           auto iso_second_cand_pair_2 = isolation_variable_second_cand.at(second_cand_pair_2);
           int iso_cmp;
           if(iso_second_cand_pair_1 == iso_second_cand_pair_2){ iso_cmp= 0;}
           else {iso_cmp =  iso_second_cand_pair_1 > iso_second_cand_pair_2 ? 1 : -1; }
           if(iso_cmp != 0) {return iso_cmp == 1;}
           //If the isolation of candidate 2 is the same, prefer the pair with highest candidate 2 pt (for cases of genuinely the same isolation value but different possible candidate 2).
           if(second_cand_pt.at(second_cand_pair_1) != second_cand_pt.at(second_cand_pair_2)){
               return second_cand_pt.at(second_cand_pair_1) > second_cand_pt.at(second_cand_pair_2);
           } // closes if on tau pts
       } // closes if on tau indices
     } // closes else
     std::cout << event << std::endl;
     throw std::runtime_error("not found a good criteria for best tau pair");
     };

   if(!pairs.empty()){
    // std::cout << "found " << pairs.size() << "pairs in the event " << event << std::endl;
         const auto best_pair = *std::min_element(pairs.begin(), pairs.end(), Comparitor);
         if(first_cand_charge[best_pair.first]!= second_cand_charge[best_pair.second]){
             final_indices.push_back(best_pair.first);
             //std::cout << "Electron has charge " << Electron_charge[best_pair.first] << " and index " << best_pair.first << std::endl;
             final_indices.push_back(best_pair.second);
             //std::cout << "Tau has charge " << Tau_charge[best_pair.second] << " and index " << best_pair.second << std::endl;
         }
     }
   //std::cout << "final indices size == " << final_indices.size() << std::endl;
   return final_indices;

}


vec_s GetFinalIndices_tauTau(const std::vector<std::pair<size_t, size_t>>& pairs, int event, const vec_f& isolation_variable_tau,const vec_f& Tau_pt, const vec_i& Tau_charge){
  vec_s final_indices;
  // define comparitor for tauTau
  //std::cout << "size tau pairs = " << pairs.size()<< std::endl;

  const auto Comparitor = [&](std::pair<size_t, size_t> pair_1, std::pair<size_t, size_t> pair_2) -> bool
  {
     if(pair_1 == pair_2) return false;
     for(size_t leg_id = 0; leg_id < 2; ++leg_id) {
         const size_t h1_leg_id = leg_id == 0 ? pair_1.first : pair_1.second;
         const size_t h2_leg_id = leg_id == 0 ? pair_2.first : pair_2.second;

         if(h1_leg_id != h2_leg_id) {
             // per ora lo faccio solo per i tau ma poi va aggiustato!!
             auto iso_cand1_pair_1 = isolation_variable_tau.at(h1_leg_id);
             auto iso_cand1_pair_2 = isolation_variable_tau.at(h2_leg_id);
             int iso_cmp;
             if(iso_cand1_pair_1 == iso_cand1_pair_2){ iso_cmp= 0;}
             else {iso_cmp =  iso_cand1_pair_1 > iso_cand1_pair_2 ? 1 : -1; }
             if(iso_cmp != 0) return iso_cmp == 1;

             if(Tau_pt.at(h1_leg_id) != Tau_pt.at(h2_leg_id))
                 return Tau_pt.at(h1_leg_id) > Tau_pt.at(h2_leg_id);
         }
     }
     std::cout << event << std::endl;
     throw std::runtime_error("not found a good criteria for best tau pair");
 };

 if(!pairs.empty()){
     const auto best_pair = *std::min_element(pairs.begin(), pairs.end(), Comparitor);
     //std::cout <<"Tau_charge[best_pair.first] = " << Tau_charge[best_pair.first]<< "\t Tau_charge[best_pair.second] = "<<Tau_charge[best_pair.second]<<std::endl;
     if(Tau_charge[best_pair.first]!=Tau_charge[best_pair.second]){
         final_indices.push_back(best_pair.first);
         final_indices.push_back(best_pair.second);
     }
 }
 return final_indices;
}


bool GenRecoMatching(const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const LorentzVectorM& lep1_p4, const LorentzVectorM& lep2_p4){
    auto dR_1 = DeltaR(leg1_p4.Phi(), leg1_p4.Eta(), lep1_p4.Phi(), lep1_p4.Eta());
    auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), lep2_p4.Phi(), lep2_p4.Eta());
    //std::cout << "DR_1 = " <<dR_1 <<"\t DR_2 = "<< dR_2<<std::endl;
    //std::cout << " are they smaller than 0.2 ? " <<(dR_1 < 0.2 && dR_2<0.2)<< std::endl;
    return (dR_1 < 0.2 && dR_2<0.2);
    //bool correspondance= (dR_1 < 0.2 && dR_2<0.2);
    //return correspondence;
}

bool JetLepSeparation(const LorentzVectorM& tau_p4, const vec_f& Jet_eta, const vec_f& Jet_phi, const vec_i & RecoJetIndices){
  vec_i JetSeparatedWRTLeptons;
  for (auto& jet_idx:RecoJetIndices){
    auto dR = DeltaR(tau_p4.Phi(), tau_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //if(dR_1>0.5 && dR_2>0.5){
    if(dR>0.5){
      JetSeparatedWRTLeptons.push_back(jet_idx);
    }
  }
  return (JetSeparatedWRTLeptons.size()>=2);
}

bool ElectronVeto(EvtInfo& evt_info, const vec_i& indices, const vec_f& Electron_pt, const vec_f& Electron_dz, const vec_f& Electron_dxy, const vec_f& Electron_eta, const vec_b& Electron_mvaFall17V2Iso_WP90, const vec_b& Electron_mvaFall17V2noIso_WP90 , const vec_f&  Electron_pfRelIso03_all){
int nElectrons =0 ;
// do not consider signal electron
int signalElectron_idx = -1;
if(evt_info.channel==Channel::eTau){
  signalElectron_idx = indices[0];
}

  for (size_t el_idx =0 ; el_idx < Electron_pt.size(); el_idx++){
      if(Electron_pt.at(el_idx) >10 && std::abs(Electron_eta.at(el_idx)) < 2.5 && std::abs(Electron_dz.at(el_idx)) < 0.2 && std::abs(Electron_dxy.at(el_idx)) < 0.045 && ( Electron_mvaFall17V2Iso_WP90.at(el_idx) == true || ( Electron_mvaFall17V2noIso_WP90.at(el_idx) == true && Electron_pfRelIso03_all.at(el_idx)<0.3 )) ){
          if(el_idx != signalElectron_idx){
              nElectrons +=1 ;
          }
      }
  }
  if(nElectrons>=1){
      return false;
  }
  return true;
}

bool MuonVeto(EvtInfo& evt_info, const vec_i& indices,const vec_f& Muon_pt, const vec_f& Muon_dz, const vec_f& Muon_dxy, const vec_f& Muon_eta, const vec_b& Muon_tightId, const vec_b& Muon_mediumId , const vec_f&  Muon_pfRelIso04_all){
int nMuons =0 ;
// do not consider signal muon
int signalMuon_idx = -1;
if(evt_info.channel==Channel::muTau){
  signalMuon_idx = indices[0];
}
  for (size_t mu_idx =0 ; mu_idx < Muon_pt.size(); mu_idx++){
      if( Muon_pt.at(mu_idx) >10 && std::abs(Muon_eta.at(mu_idx)) < 2.4 && std::abs(Muon_dz.at(mu_idx)) < 0.2 && std::abs(Muon_dxy.at(mu_idx)) < 0.045 && ( Muon_mediumId.at(mu_idx) == true ||  Muon_tightId.at(mu_idx) == true ) && Muon_pfRelIso04_all.at(mu_idx)<0.3  ){
          if(mu_idx != signalMuon_idx){
              nMuons +=1 ;
          }
      }
  }
  if(nMuons>=1){
      return false;
  }
  return true;
}

vec_i ReorderJets(const vec_f& GenJet_pt, const vec_i& index_vec){

  vec_i reordered_jet_indices ;
  while(reordered_jet_indices.size()<2){
    float pt_max = 0;
    int i_max = -1;
    for(auto&i :index_vec){
      //std::cout << "i " << i << "\t i max "<< i_max<< "\tGenJet_pt " << GenJet_pt.at(i) << "\tpt_max " << pt_max << std::endl;
      if(std::find(reordered_jet_indices.begin(), reordered_jet_indices.end(), i)!=reordered_jet_indices.end()) continue;
      if(GenJet_pt.at(i)>pt_max){
        pt_max=GenJet_pt.at(i);
        i_max = i;
      }
    }
    if(i_max>=0){
      reordered_jet_indices.push_back(i_max);
      pt_max = 0;
      i_max = -1;
    }
  }
  return reordered_jet_indices;
}
vec_i ReorderAllJets(const vec_f& GenJet_pt, const vec_i& index_vec){

  vec_i reordered_jet_indices ;
  //while(reordered_jet_indices.size()<2){
  float pt_max = 0;
  int i_max = -1;
  for(auto&i :index_vec){
    //std::cout << "i " << i << "\t i max "<< i_max<< "\tGenJet_pt " << GenJet_pt.at(i) << "\tpt_max " << pt_max << std::endl;
    if(std::find(reordered_jet_indices.begin(), reordered_jet_indices.end(), i)!=reordered_jet_indices.end()) continue;
    if(GenJet_pt.at(i)>=pt_max){
      pt_max=GenJet_pt.at(i);
      i_max = i;
    }
  }
  if(i_max>=0){
    reordered_jet_indices.push_back(i_max);
    pt_max = 0;
    i_max = -1;
  }
  //}
  return reordered_jet_indices;
}


vec_f ReorderVSJet(EvtInfo& evt_info,const vec_i& final_indices, const vec_f& Tau_rawDeepTau2017v2p1VSjet){

  vec_f reordered_vs_jet ;
  if(evt_info.channel == Channel::tauTau){
      for(auto& i : final_indices){
          reordered_vs_jet.push_back(Tau_rawDeepTau2017v2p1VSjet.at(i));
      }
  }
  else{
      reordered_vs_jet.push_back(Tau_rawDeepTau2017v2p1VSjet.at(final_indices[1] ) );
  }
  return reordered_vs_jet;
}

int AK4JetFilter(EvtInfo& evt_info, const vec_i& indices, const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const vec_f&  Jet_eta, const vec_f&  Jet_phi, const vec_f&  Jet_pt, const vec_i&  Jet_jetId, const int& is2017){
  int nFatJetsAdd=0;
  int nJetsAdd=0;

  for (size_t jet_idx =0 ; jet_idx < Jet_pt.size(); jet_idx++){
      if(Jet_pt.at(jet_idx)>20 && std::abs(Jet_eta.at(jet_idx)) < 2.5 && ( (Jet_jetId.at(jet_idx))&(1<<1) || is2017 == 1) ) {
          float dr1 = DeltaR(leg1_p4.phi(), leg1_p4.eta(),Jet_phi[jet_idx], Jet_eta[jet_idx]);
          float dr2 = DeltaR(leg2_p4.phi(), leg2_p4.eta(),Jet_phi[jet_idx], Jet_eta[jet_idx]);
          if(dr1 > 0.5 && dr2 >0.5 ){
              nJetsAdd +=1;
            }
      }
  }
  int n_Jets = 2;
  //if(evt_info.channel!=Channel::tauTau){
  //     n_Jets = 1;
  //}
  if(nJetsAdd>=n_Jets){
      return 1;
  }
  return 0;
}

int AK8JetFilter(EvtInfo& evt_info, const vec_i& indices, const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const vec_f&  FatJet_pt, const vec_f&  FatJet_eta, const vec_f&  FatJet_phi, const vec_f& FatJet_msoftdrop, const int& is2017){
  int nFatJetsAdd=0;
  for (size_t fatjet_idx =0 ; fatjet_idx < FatJet_pt.size(); fatjet_idx++){
      if(FatJet_msoftdrop.at(fatjet_idx)>30 && std::abs(FatJet_eta.at(fatjet_idx)) < 2.5) {
          float dr1 = DeltaR(leg1_p4.phi(), leg1_p4.eta(),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
          float dr2 = DeltaR(leg2_p4.phi(), leg2_p4.eta(),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
          if(dr1 > 0.5 && dr2 >0.5 ){
              nFatJetsAdd +=1;
            }
      }
  }
  //std::cout << nFatJetsAdd << "\t" <<nJetsAdd << std::endl;
  int n_FatJets = 1 ;
  //if(evt_info.channel!=Channel::tauTau){
  //     n_Jets = 1;
  //}
  if(nFatJetsAdd>=n_FatJets ){
      return 1;
  }
  return 0;
}
std::string FromDecimalToBinary(const int& decimalNumber)
{
    int n=decimalNumber;
    std::string r;
    while(n!=0) {r=(n%2==0 ?"0":"1")+r; n/=2;}
    return r;
}

// to be merged in only 1 function
/*
LorentzVectorM MomentumSum(const vec_f &pt,const vec_f &eta,const vec_f &phi,const vec_f &mass, bool wantIndicesForFlavour ){
  LorentzVectorM Tot_momentum;
  vec_i part_indices;
  if(wantIndicesForFlavour){

  }
  for(int part_idx = 0 ;part_idx<GenJet_pt.size(); part_idx++){
    if(wantOnlybPartonFlavour==true && abs(GenJet_partonFlavour[part_idx])!=5)continue;
    const float particleMass = (particleMasses.find(GenJet_partonFlavour[part_idx]) != particleMasses.end()) ? particleMasses.at(GenJet_partonFlavour[part_idx]) : GenJet_mass[part_idx];
    genParticle_Tot_momentum+=LorentzVectorM(GenJet_pt[part_idx], GenJet_eta[part_idx], GenJet_phi[part_idx],particleMass);
  }
}*/

vec_i FindRecoGenJetCorrespondence(const vec_i& RecoJet_genJetIdx, const vec_i& GenJet_taggedIndices){
  vec_i RecoJetIndices;
  for (int i =0 ; i<RecoJet_genJetIdx.size(); i++){
    for(auto& genJet_idx : GenJet_taggedIndices ){
      int recoJet_genJet_idx = RecoJet_genJetIdx[i];
      if(recoJet_genJet_idx==genJet_idx){
        RecoJetIndices.push_back(recoJet_genJet_idx);
      }
    }
  }
  return RecoJetIndices;
}

float InvMassByFalvour(const vec_f &GenJet_pt,const vec_f &GenJet_eta,const vec_f &GenJet_phi,const vec_f &GenJet_mass, const vec_i& GenJet_partonFlavour, bool wantOnlybPartonFlavour){
    LorentzVectorM genParticle_Tot_momentum;
    for(int part_idx = 0 ;part_idx<GenJet_pt.size(); part_idx++){
      if(wantOnlybPartonFlavour==true && abs(GenJet_partonFlavour[part_idx])!=5)continue;
      const float particleMass = (particleMasses.find(GenJet_partonFlavour[part_idx]) != particleMasses.end()) ? particleMasses.at(GenJet_partonFlavour[part_idx]) : GenJet_mass[part_idx];
      genParticle_Tot_momentum+=LorentzVectorM(GenJet_pt[part_idx], GenJet_eta[part_idx], GenJet_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}


float InvMassByIndices(const vec_i &indices, const vec_f& GenPart_pt,const vec_f &GenPart_eta,const vec_f &GenPart_phi,const vec_f &GenPart_mass,const vec_i& GenPart_pdgId, bool wantOnlybParticle){
    LorentzVectorM genParticle_Tot_momentum;
    //if(evt!=905) return 0.;
    for(int i = 0 ;i<indices.size(); i++){
      auto part_idx = indices.at(i);
      //std::cout << part_idx<<"\n";
      //std::cout << GenPart_pt.size()<<"\n";
      if(wantOnlybParticle && abs(GenPart_pdgId[part_idx])!=5) continue;
      //if(wantOnlybPartonFlavour==true && abs(GenJet_pdgId[part_idx])!=5)continue;
      const float particleMass = (particleMasses.find(GenPart_pdgId[part_idx]) != particleMasses.end()) ? particleMasses.at(GenPart_pdgId[part_idx]) : GenPart_mass[part_idx];
      //std::cout << GenPart_pt[part_idx]<<"\t"<< GenPart_eta[part_idx]<<"\t"<< GenPart_phi[part_idx]<<"\t"<<particleMass<< std::endl;
      genParticle_Tot_momentum+=LorentzVectorM(GenPart_pt[part_idx], GenPart_eta[part_idx], GenPart_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}

vec_i FindTwoJetsClosestToMPV(float mpv, const vec_f& GenPart_pt,const vec_f &GenPart_eta,const vec_f &GenPart_phi,const vec_f &GenPart_mass,const vec_i& GenPart_pdgId){
  vec_i indices;
  int i_min, j_min;
  float delta_min = 100;
  //float closest_value=10.*mpv;
  for(int i =0 ; i< GenPart_pt.size(); i++){
    for(int j=0; j<i; j++){
      vec_i temporary_indices;
      temporary_indices.push_back(i);
      temporary_indices.push_back(j);
      float inv_mass = InvMassByIndices(temporary_indices, GenPart_pt,GenPart_eta,GenPart_phi,GenPart_mass,GenPart_pdgId, true);
      float delta_mass = abs(inv_mass-mpv);
      if(delta_mass<=delta_min){
        i_min=i;
        j_min=j;
        delta_min = delta_mass;
        //closest_value = inv_mass
      }
    }
  }

  indices.push_back(i_min);
  indices.push_back(j_min);
  return indices;
}

vec_i GetDaughters(const int& mother_idx, vec_i& already_considered_daughters, const vec_i& GenPart_genPartIdxMother ){
  vec_i daughters;
  for (int daughter_idx =mother_idx; daughter_idx<GenPart_genPartIdxMother.size(); daughter_idx++){

    if(GenPart_genPartIdxMother[daughter_idx] == mother_idx && !(std::find(already_considered_daughters.begin(), already_considered_daughters.end(), daughter_idx)!=already_considered_daughters.end())){
      daughters.push_back(daughter_idx);
      already_considered_daughters.push_back(daughter_idx);
    }
  }
  //std::cout<<daughters.size()<<std::endl;
  return daughters;

}
vec_i GetMothers(const int &part_idx, const vec_i& GenPart_genPartIdxMother ){
  vec_i mothers;
  int new_idx = part_idx;
  int mother_idx = GenPart_genPartIdxMother[new_idx];
  while(mother_idx >=0 ){
    mothers.push_back(mother_idx);
    new_idx = mother_idx;
    mother_idx = GenPart_genPartIdxMother[new_idx];

  }
  return mothers;

}

vec_i GetLastHadrons(const vec_i& GenPart_pdgId, const vec_i& GenPart_genPartIdxMother){

  vec_i already_considered_daughters;
  vec_i lastHadrons;
  //if(evt!=905) return lastHadrons;

  for(int part_idx =0; part_idx<GenPart_pdgId.size(); part_idx++){
    vec_i daughters = GetDaughters(part_idx, already_considered_daughters, GenPart_genPartIdxMother);
    vec_i mothers = GetMothers(part_idx, GenPart_genPartIdxMother);

    bool comesFrom_b = false;
    bool comesFrom_H = false;
    bool hasHadronsDaughters = false;

    for (auto& mother : mothers){
      if(abs(GenPart_pdgId[mother])==5) comesFrom_b=true;
      if(abs(GenPart_pdgId[mother])==25) comesFrom_H=true;
    }
    for (auto& daughter:daughters){
      ParticleInfo daughterInformation = findParticleByPdgIdx(GenPart_pdgId[daughter]);
      if(daughterInformation.type == "baryon" || daughterInformation.type == "meson"){
        hasHadronsDaughters = true;
      }
    }
    if(comesFrom_b && comesFrom_H && !hasHadronsDaughters){
      ParticleInfo lastHadronInformation = findParticleByPdgIdx(GenPart_pdgId[part_idx]);
      if(lastHadronInformation.type == "baryon" || lastHadronInformation.type == "meson"){
        lastHadrons.push_back(part_idx);
      }
    }
  }/*
  for(auto & lastHadron:lastHadrons){
      ParticleInfo lastHadronInformation = findParticleByPdgIdx(GenPart_pdgId[lastHadron]);
      std::cout << "last hadron " <<lastHadron<<"\t" <<lastHadronInformation.pdgId << "\t" << lastHadronInformation.name << "\t" << lastHadronInformation.type<< "\t" << lastHadronInformation.charge << std::endl;
  }*/
  return lastHadrons;
}

// print decay chain functions

void PrintDecayChainParticle(const ULong64_t evt, const int& mother_idx, const vec_i& GenPart_pdgId, const vec_i& GenPart_genPartIdxMother, const vec_i& GenPart_statusFlags, const vec_f& GenPart_pt, const vec_f& GenPart_eta, const vec_f& GenPart_phi, const vec_f& GenPart_mass, const vec_i& GenPart_status, const std::string pre, vec_i &already_considered_daughters, std::ostream& os)
{
  //if(evt!=8793) return ;
  //std::vector<int> daughters;
  //const bool isFirstCopy = (GenPart_statusFlags[mother_idx])&(1<<12);
  //bool isLastCopy = (GenPart_statusFlags[mother_idx])&(1<<13);
  //bool isStartingGluon = ( GenPart_genPartIdxMother[mother_idx] == -1) && (GenPart_pdgId[mother_idx] == 21 || GenPart_pdgId[mother_idx] == 9);
  ParticleInfo particle_information = findParticleByPdgIdx(GenPart_pdgId[mother_idx]);
  vec_i daughters = GetDaughters(mother_idx, already_considered_daughters, GenPart_genPartIdxMother);
  const float particleMass = (particleMasses.find(GenPart_pdgId[mother_idx]) != particleMasses.end()) ? particleMasses.at(GenPart_pdgId[mother_idx]) : GenPart_mass[mother_idx];
  const LorentzVectorM genParticle_momentum = LorentzVectorM(GenPart_pt[mother_idx], GenPart_eta[mother_idx], GenPart_phi[mother_idx],particleMass);
  int mother_mother_index = GenPart_genPartIdxMother[mother_idx];
  const auto flag = GenPart_statusFlags[mother_idx];

  os << particle_information.name      << " <" << GenPart_pdgId[mother_idx]
     << "> pt = " << genParticle_momentum.Pt()      << " eta = " << genParticle_momentum.Eta()
     << " phi = " << genParticle_momentum.Phi()     << " E = " << genParticle_momentum.E()
     << " m = "   << genParticle_momentum.M()       << " index = " << mother_idx
     << " flag = " << FromDecimalToBinary(flag)     << " particleStatus = " << GenPart_status[mother_idx]
     << " charge = " << particle_information.charge << " type = " << particle_information.type
     //<< " binary flag= " << FromDecimalToBinary(flag)  //    << " binary particleStatus = " <<
     << " mother_idx = " << mother_mother_index;
  os << "\n";

    if(daughters.size()==0 ) return;
    //if(daughters.size()==0 && !isStartingGluon) return;
    for(int d_idx =0; d_idx<daughters.size(); d_idx++) {
      int n = daughters[d_idx];
      os << pre << "+-> ";
      const char pre_first = d_idx == daughters.size() -1 ?  ' ' : '|';
      const std::string pre_d = pre + pre_first ;//+ "  ";
      PrintDecayChainParticle(evt, n, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, pre_d, already_considered_daughters, os);

  }
}
int PrintDecayChain(const ULong64_t evt, const vec_i& GenPart_pdgId, const vec_i& GenPart_genPartIdxMother, const vec_i& GenPart_statusFlags, const vec_f& GenPart_pt, const vec_f& GenPart_eta, const vec_f& GenPart_phi, const vec_f& GenPart_mass, const vec_i& GenPart_status,const std::string& dirName)
{
    std::string fileName = dirName+"/file_evt"+std::to_string(evt)+".txt";
    std::ofstream out_file(fileName);
    //total_receipt(out_file);
    //  if(evt!=8793) return 0;
    vec_i already_considered_daughters;
    for(int mother_idx =0; mother_idx<GenPart_pdgId.size(); mother_idx++){
      bool isStartingParticle = ( GenPart_genPartIdxMother[mother_idx] == -1);//&& (GenPart_pdgId[mother_idx] == 21 || GenPart_pdgId[mother_idx] == 9);
      if(!isStartingParticle) continue;
      PrintDecayChainParticle(evt, mother_idx, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, "", already_considered_daughters, out_file);
    }

return 0;
}
