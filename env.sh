#!/usr/bin/env bash

function run_cmd {
    "$@"
    RESULT=$?
    if (( $RESULT != 0 )); then
        echo "Error while running '$@'"
        kill -INT $$
    fi
}

action() {
    # determine the directory of this file
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

    export PYTHONPATH="$this_dir:$PYTHONPATH"
    export LAW_HOME="$this_dir/.law"
    export LAW_CONFIG_FILE="$this_dir/config/law.cfg"

    export ANALYSIS_PATH="$this_dir"
    export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"
    export X509_USER_PROXY="$ANALYSIS_DATA_PATH/voms.proxy"
    export CENTRAL_STORAGE="/eos/home-v/vdamante/HH_bbtautau_resonant_Run2"
    export ANALYSIS_BIG_DATA_PATH="$CENTRAL_STORAGE/tmp/$(whoami)/data"
    export PATH=$PATH:$HOME/.local/bin:$ANALYSIS_PATH/scripts

    run_cmd mkdir -p "$ANALYSIS_DATA_PATH"
    # source /cvmfs/sft.cern.ch/lcg/views/setupViews.sh LCG_101 x86_64-centos7-gcc8-opt
    # conda activate bbtt

    local os_version=$(cat /etc/os-release | grep VERSION_ID | sed -E 's/VERSION_ID="([0-9]+)"/\1/')
    if [ $os_version = "7" ]; then
        export SCRAM_ARCH=slc7_amd64_gcc10
    else
        export SCRAM_ARCH=el8_amd64_gcc10
    fi
    CMSSW_VER=CMSSW_12_6_0_patch1
    if ! [ -f "$this_dir/soft/CentOS$os_version/$CMSSW_VER/.installed" ]; then
        run_cmd mkdir -p "$this_dir/soft/CentOS$os_version"
        run_cmd cd "$this_dir/soft/CentOS$os_version"
        if [ -d $CMSSW_VER ]; then
            echo "Removing incomplete $CMSSW_VER installation..."
            run_cmd rm -rf $CMSSW_VER
            run_cmd rm -f "$this_dir/soft/CentOS$os_version/bin/python"
        fi
        echo "Creating $CMSSW_VER area for CentOS$os_version in $PWD ..."
        run_cmd scramv1 project CMSSW $CMSSW_VER
        run_cmd cd $CMSSW_VER/src
        run_cmd eval `scramv1 runtime -sh`
        # clone repos
        run_cmd git clone ssh://git@gitlab.cern.ch:7999/akhukhun/roccor.git RoccoR
        run_cmd git clone git@github.com:SVfit/ClassicSVfit.git TauAnalysis/ClassicSVfit -b fastMTT_19_02_2019
        run_cmd git clone git@github.com:SVfit/SVfitTF.git TauAnalysis/SVfitTF
        run_cmd git clone ssh://git@gitlab.cern.ch:7999/rgerosa/particlenetstudiesrun2.git ParticleNetStudiesRun2 -b cmssw_126X
        run_cmd git clone git@github.com:cms-data/RecoBTag-Combined.git recobtag_data
        # copy model files
        run_cmd mkdir -p Framework/NanoProd/data/ParticleNetAK4/CHS/PNETUL
        run_cmd cp -r ParticleNetStudiesRun2/TrainingNtupleMakerAK4/data/ParticleNetAK4/CHS/PNETUL/ClassRegQuantileNoJECLost Framework/NanoProd/data/ParticleNetAK4/CHS/PNETUL/
        run_cmd mkdir -p Framework/NanoProd/data/ParticleNetAK8/Puppi/PNETUL/ClassReg
        run_cmd cp -r ParticleNetStudiesRun2/TrainingNtupleMakerAK8/data/ParticleNetAK8/Puppi/PNETUL/ClassReg/* Framework/NanoProd/data/ParticleNetAK8/Puppi/PNETUL/ClassReg/
        run_cmd mkdir -p Framework/NanoProd/data/ParticleNetAK8/MassRegression/V01
        run_cmd cp recobtag_data/ParticleNetAK8/MassRegression/V01/modelfile/model.onnx Framework/NanoProd/data/ParticleNetAK8/MassRegression/V01/particle-net.onnx
        run_cmd cp recobtag_data/ParticleNetAK8/MassRegression/V01/*.json Framework/NanoProd/data/ParticleNetAK8/MassRegression/V01/
        run_cmd rm -rf recobtag_data
        # link framework files into release
        run_cmd ln -s "$this_dir/NanoProd" Framework/NanoProd/python
        run_cmd mkdir -p HHTools
        run_cmd ln -s "$this_dir/HHbtag" HHTools/HHbtag
        run_cmd scram b -j8
        # workaround for CMSSW - CRAB compatibility
        # since recently, "scram b" does not link files into the python directory any longer and
        # therefore, crab's bundling of the python directory will miss all files (for now), so below,
        # create symlinks manually that are picked up through dereferencing during crab job bundling
        run_cmd touch ../python/__init__.py
        for n in */*/python; do
            subsys="$( echo ${n} | awk -F '/' '{print $1}' )"
            mod="$( echo ${n} | awk -F '/' '{print $2}' )"
            run_cmd mkdir -p "../python/${subsys}"
            run_cmd rm -rf "../python/${subsys}/${mod}"
            run_cmd ln -s "../../src/${subsys}/${mod}/python" "../python/${subsys}/${mod}"
            run_cmd touch "../python/${subsys}/__init__.py" "../python/${subsys}/${mod}/__init__.py"
        done
        # end of workaround
        run_cmd cd "$this_dir"
        run_cmd mkdir -p "$this_dir/soft/CentOS$os_version/bin"
        run_cmd rm -f "$this_dir/soft/CentOS$os_version/bin/python"
        run_cmd ln -s $(which python3) "$this_dir/soft/CentOS$os_version/bin/python"
        run_cmd touch "$this_dir/soft/CentOS$os_version/$CMSSW_VER/.installed"
    else
        run_cmd cd "$this_dir/soft/CentOS$os_version/$CMSSW_VER/src"
        run_cmd eval `scramv1 runtime -sh`
        run_cmd cd "$this_dir"
    fi
    mkdir -p "$ANALYSIS_DATA_PATH"
    export PATH="$this_dir/soft/CentOS$os_version/bin:$PATH"
    source /afs/cern.ch/user/m/mrieger/public/law_sw/setup.sh
    source "$( law completion )"
}
action
