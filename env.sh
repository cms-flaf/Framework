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

    # source /cvmfs/sft.cern.ch/lcg/views/setupViews.sh LCG_101 x86_64-centos7-gcc8-opt
    # conda activate bbtt

    export SCRAM_ARCH=el8_amd64_gcc10
    CMSSW_VER=CMSSW_12_4_7
    if ! [ -f soft/$CMSSW_VER/.installed ]; then
        run_cmd mkdir -p soft
        run_cmd cd soft
        if [ -d $CMSSW_VER ]; then
            echo "Removing incomplete $CMSSW_VER installation..."
            run_cmd rm -rf $CMSSW_VER
        fi
        echo "Creating new $CMSSW_VER area..."
        run_cmd scramv1 project CMSSW $CMSSW_VER
        run_cmd cd $CMSSW_VER/src
        run_cmd eval `scramv1 runtime -sh`
        run_cmd scram b -j8
        run_cmd cd ../../..
        touch soft/$CMSSW_VER/.installed
    else
        run_cmd cd soft/$CMSSW_VER/src
        run_cmd eval `scramv1 runtime -sh`
        run_cmd cd ../../..
    fi
    source /afs/cern.ch/user/m/mrieger/public/law_sw/setup.sh
    source "$( law completion )"
}
action
