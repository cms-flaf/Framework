#!/bin/bash
CURRENT_DIR=$PWD
cd $ANALYSIS_PATH/inference
source setup.sh "flaf" &> /dev/null
cd $CURRENT_DIR
"$@"
