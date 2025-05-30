#!/usr/bin/env bash
set -euo pipefail

#timestamp() {
#  date +%s%3N  # Milliseconds since epoch (GNU date)
#}

#start_all=$(timestamp)

OLD_PWD="${PWD}"

USM_DIR="$1"
input_dir="$2"
# convert dt to integer
dt=$(($3)) #$(echo $3 | awk '{print int($1)}')


cd "$USM_DIR"

#start_dssat=$(timestamp)
dssat B DSSBatch.v47  > /dev/null
#end_dssat=$(timestamp)

#start_file_ops=$(timestamp)
base=$(basename "$USM_DIR")
if [ -f "Summary.OUT" ]; then
    mv Summary.OUT "$input_dir/Summary_$base.OUT"
fi
#end_file_ops=$(timestamp)

# if dt=1, then delete the USM_DIR
#start_cleanup=$(timestamp)
if [ $dt == 1 ]; then
    (rm -r "$USM_DIR" &)
fi

cd "$OLD_PWD" 
#end_cleanup=$(timestamp)

#end_all=$(timestamp)

# Print timings
#echo "DSSAT: $((end_dssat - start_dssat))ms"
#echo "FileOps: $((end_file_ops - start_file_ops))ms"
#echo "Cleanup: $((end_cleanup - start_cleanup))ms"
#echo "Total: $((end_all - start_all))ms"
