#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MODEL_DIR="${SCRIPT_DIR}/Models"
RESULTS_DIR="${SCRIPT_DIR}/Results"

OUTPUT="$RESULTS_DIR/logs/${NAME/.mps/.log}"
SOLUTION="$RESULTS_DIR/${NAME/.mps/.sol}"


for MODEL in `find $MODEL_DIR -iname '*.mps'`
do
    NAME=`basename $MODEL`
    SOLUTION="$RESULTS_DIR/${NAME/.mps/.sol}"
    [ ! -f "$SOLUTION" ] && echo $MODEL
done


