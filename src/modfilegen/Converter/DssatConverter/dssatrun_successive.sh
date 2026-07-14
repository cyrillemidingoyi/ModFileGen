#!/usr/bin/env bash
OLD_PWD="${PWD}"
USM_DIR="$1"
input_dir="$2"
summary_id="$3"
cd "$USM_DIR"
dssat Q DSSBatch.v47
if [ -f "Summary.OUT" ]; then
    mv Summary.OUT "$input_dir/Summary_${summary_id}.OUT"
fi
cd "$OLD_PWD"
