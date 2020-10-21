#!/bin/bash

set -e

# timeout
LIMIT_HOURS=3
# timeout, in seconds
LIMIT=$((LIMIT_HOURS * 60 * 60))

MODEL=$1
SOLVER=gurobi_cl

[ -z "$MODEL" ] && (echo "No input file."; exit 1)

NAME=`basename $MODEL`
RESULTS_DIR='./Results'

OUTPUT="$RESULTS_DIR/logs/${NAME/.mps/.log}"
SOLUTION="$RESULTS_DIR/${NAME/.mps/.sol}"

mkdir -p $RESULTS_DIR/logs


echo "$MODEL -> $OUTPUT"

exec $SOLVER LogToConsole=0 LogFile=$OUTPUT ResultFile=$SOLUTION  TimeLimit=$LIMIT Threads=1 $MODEL
