
for year in 2016 2016_HIPM 2018 2017; do for cat in res1b res2b boosted; do mkdir -p /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/output/Run2_${year}/${cat} ; done ; done

for res in radion ; do for mass in all 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2017 2016 2016_HIPM ; do python3 /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/MassCutHistograms.py --year ${year} --mass ${mass} --res ${res}; done ; done ; done

for res in  graviton ; do for mass in all 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2017 2016 2016_HIPM ; do python3 /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/MassCutHistograms.py --year ${year} --mass ${mass} --res ${res}; done ; done ; done

for res in  both; do for mass in all 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2017 2016 2016_HIPM ; do python3 /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/MassCutHistograms.py --year ${year} --mass ${mass} --res ${res} ; done ; done ; done

for year in 2016 2017 2018 2016_HIPM; do
    for cat in res1b res2b boosted ; do
        mkdir -p /eos/home-v/vdamante/www//MassCuts/Run2_${year}/${cat}
        cp index_1.php /eos/home-v/vdamante/www/index.php
        cp index_2.php /eos/home-v/vdamante/www/MassCuts/Run2_${year}/index.php
        cp index_3.php /eos/home-v/vdamante/www/MassCuts/Run2_${year}/${cat}/index.php
        cp /afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/output/Run2_${year}/${cat}/*png /eos/home-v/vdamante/www/MassCuts/Run2_${year}/${cat}/
    done
done
