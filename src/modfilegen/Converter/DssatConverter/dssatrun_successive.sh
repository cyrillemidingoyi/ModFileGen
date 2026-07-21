#!/usr/bin/env bash
OLD_PWD="${PWD}"
USM_DIR="$1"
input_dir="$2"
summary_id="$3"
dssat_version="${4:-v47}"
case "$dssat_version" in
    v47)
        dssat_command="dssat"
        batch_file="DSSBatch.v47"
        ;;
    v48)
        dssat_command="dssatv48"
        batch_file="DSSBatch.v48"
        ;;
    *)
        echo "Unsupported DSSAT version: $dssat_version (expected v47 or v48)" >&2
        exit 2
        ;;
esac
cd "$USM_DIR"
"$dssat_command" Q "$batch_file"
if [ -f "Summary.OUT" ]; then
    mv Summary.OUT "$input_dir/Summary_${summary_id}.OUT"
fi
cd "$OLD_PWD"
