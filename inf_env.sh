#!/bin/bash
CURRENT_DIR=$PWD
cd $INF_PATH
source setup.sh ""
cd $CURRENT_DIR
"$@"