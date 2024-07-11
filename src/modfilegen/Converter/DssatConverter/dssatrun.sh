#!/usr/bin/env bash

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
dt=$3

cd "$USM_DIR"

dssat B DSSBatch.v47  #> /dev/null

base=$(basename "$USM_DIR")
if [ -f "Summary.OUT" ]; then
    mv Summary.OUT "$input_dir/Summary_$base.OUT"
fi

# if dt=1, then delete the USM_DIR
if [ $dt -eq 1 ]; then
    rm -rf "$USM_DIR"
fi

cd "$OLD_PWD"

