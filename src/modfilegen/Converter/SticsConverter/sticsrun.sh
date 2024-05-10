#!/usr/bin/env bash

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"

cd "$USM_DIR"

/opt/stics/bin/stics_modulo

base=$(basename "$USM_DIR")
if [ -f "mod_rapport.sti" ]; then
    mv mod_rapport.sti "$input_dir/mod_rapport_$base.sti"
fi
rm -rf "$USM_DIR"
cd "$OLD_PWD"


