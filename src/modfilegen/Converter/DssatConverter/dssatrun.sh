#!/usr/bin/env bash

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
# convert dt to integer
dt=$(echo $3 | awk '{print int($1)}')
dailyoutput=$(echo "${4:-0}" | awk '{print int($1)}')


cd "$USM_DIR"

dssat B DSSBatch.v47  #> /dev/null

base=$(basename "$USM_DIR")
if [ -f "Summary.OUT" ]; then
    cp -- Summary.OUT "$input_dir/Summary_$base.OUT"
fi

if [ "$dailyoutput" -eq 1 ]; then
    for output_file in ET.out PlantGro.out PlantN.out SoilOrg.out SoilWat.Out Weather.out; do
        if [ -f "$output_file" ]; then
            source_name="${output_file%.*}"
            cp -- "$output_file" "$input_dir/${source_name}_${base}.out"
        fi
    done
fi

# if dt=1, then delete the USM_DIR
if [ $dt -eq 1 ]; then
    (rm -rf "$USM_DIR" &)
fi

cd "$OLD_PWD" 
