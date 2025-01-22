for res in radion graviton;
    do for cat in res1b_cat3_masswindow res2b_cat3_masswindow boosted_cat3_masswindow;
        do for channel in eTau muTau tauTau;
            do for mass in 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900  ;
                do echo ${res} ${cat} ${channel} ${mass}  ;
                python3 /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/GetMassesDistributions.py --year all --cat ${cat} --channels ${channel} --mass ${mass} --res ${res};
                done ;
            done ;
        done ;
    done ;



