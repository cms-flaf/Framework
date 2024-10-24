#!/bin/bash

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local file_name=$(basename $this_file)
    env -i FLAF_CMSSW_BASE=$FLAF_CMSSW_BASE $ANALYSIS_PATH/cmsEnv.sh $FLAF_CMSSW_BASE/bin/$FLAF_CMSSW_ARCH/$file_name "$@"
}

action "$@"

