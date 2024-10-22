for res in radion; do for mass in 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2016_HIPM 2017 2016 ; do python3 Studies/MassCuts/KinFitMass_distributions.py --year ${year} --mass ${mass} ; done ; done ; done

for res in graviton; do for mass in 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2016_HIPM 2017 2016 ; do python3 Studies/MassCuts/KinFitMass_distributions.py --year ${year} --mass ${mass} ; done ; done ; done

for res in both; do for mass in 1000 1250 1500 1750 2000 2500 250 260 270 280 3000 300 320 350 400 450 500 550 600 650 700 750 800 850 900 ; do for year in 2018 2016_HIPM 2017 2016 ; do python3 Studies/MassCuts/KinFitMass_distributions.py --year ${year} --mass ${mass} ; done ; done ; done
