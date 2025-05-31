#!/usr/bin/env bash

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
dt=$(echo $3 | awk '{print int($1)}')

cd "$USM_DIR"

sed -i -z 's/codeseprapport\n1/codeseprapport\n2/g' "$USM_DIR"/tempopar.sti

/opt/stics/bin/stics_modulo > /dev/null

base=$(basename "$USM_DIR")
if [ -f "mod_rapport.sti" ]; then
    mv mod_rapport.sti "$input_dir/mod_rapport_$base.sti"
fi
if [ $dt -eq 1 ]; then
    (rm -rf "$USM_DIR" &)
fi
cd "$OLD_PWD"

