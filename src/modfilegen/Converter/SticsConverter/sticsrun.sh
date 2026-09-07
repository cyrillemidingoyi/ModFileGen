#!/usr/bin/env bash
set -euo pipefail

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
dt=$(echo "$3" | awk '{print int($1)}')
dailyoutput=$(echo "${4:-0}" | awk '{print int($1)}')

cd -- "$USM_DIR"

sed -i -z 's/codeseprapport\n1/codeseprapport\n2/g' -- tempopar.sti

/opt/stics/bin/stics_modulo > /dev/null

base=$(basename "$USM_DIR")
if [ -f "mod_rapport.sti" ]; then
    cp -- mod_rapport.sti "$input_dir/mod_rapport_$base.sti"
fi
if [ "$dailyoutput" -eq 1 ]; then
    for daily_file in mod_s*.sti; do
        if [ -f "$daily_file" ]; then
            cp -- "$daily_file" "$input_dir/mod_s_${base}.sti"
            break
        fi
    done
    if [ -f "mod_profil.sti" ]; then
        cp -- mod_profil.sti "$input_dir/mod_profil_${base}.sti"
    fi
fi
if [ "$dt" -eq 1 ]; then
    (rm -rf -- "$USM_DIR" &)
fi
cd -- "$OLD_PWD"
