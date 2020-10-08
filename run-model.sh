#!/bin/bash

set -e

# 1 hour timeout, in seconds
LIMIT=$((1 * 60 * 60))

MODEL=$1
SOLVER=gurobi_cl

[ -z "$MODEL" ] && (echo "No input file."; exit 1)

NAME=`basename $MODEL`
RESULTS_DIR='./Results'

OUTPUT="$RESULTS_DIR/logs/${NAME/.mps/.log}"
SOLUTION="$RESULTS_DIR/${NAME/.mps/.sol}"

mkdir -p $RESULTS_DIR/logs


echo "$MODEL -> $OUTPUT"

exec $SOLVER LogToConsole=0 LogFile=$OUTPUT ResultFile=$SOLUTION  TimeLimit=$LIMIT $MODEL
