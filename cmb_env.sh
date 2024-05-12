#!/bin/bash
CURRENT_DIR=$PWD
cd $ANALYSIS_PATH/inference
source setup.sh "flaf"
cd $CURRENT_DIR
"$@"