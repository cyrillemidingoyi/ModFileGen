#!/usr/bin/env bash
set -euo pipefail

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
dt=$(echo "$3" | awk '{print int($1)}')
dailyoutput=$(echo "${4:-0}" | awk '{print int($1)}')

cd -- "$USM_DIR"

sed -i -z 's/codeseprapport\n1/codeseprapport\n2/g' -- tempopar.sti

/opt/sticsv11/bin/stics_modulo > /dev/null

base=$(basename "$USM_DIR")
if [ -f "mod_rapport.sti" ]; then
    cp -- mod_rapport.sti "$input_dir/mod_rapport_$base.sti"
fi
if [ -f "mod_rapportA.sti" ]; then
    cp -- mod_rapportA.sti "$input_dir/mod_rapportA_${base}.sti"
fi

if [ -f "mod_rapportP.sti" ]; then
    cp -- mod_rapportP.sti "$input_dir/mod_rapportP_${base}.sti"
fi
if [ "$dailyoutput" -eq 1 ]; then
    for daily_file in mod_s*.sti; do
        if [ -f "$daily_file" ]; then
            daily_name="${daily_file%.sti}"
            cp -- "$daily_file" "$input_dir/mod_s_${base}__${daily_name}.sti"
        fi
    done
    for profile_file in mod_profil*.sti; do
        if [ -f "$profile_file" ]; then
            profile_name="${profile_file%.sti}"
            cp -- "$profile_file" "$input_dir/mod_profil_${base}__${profile_name}.sti"
        fi
    done
fi
if [ "$dt" -eq 1 ]; then
    (rm -rf -- "$USM_DIR" &)
fi
cd -- "$OLD_PWD"
