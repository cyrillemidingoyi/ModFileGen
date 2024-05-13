#!/bin/sh

WKS_PATH="$1"


THREADS="$3"
#echo "tttttt $THREADS"

OUTPUT_DIR="$2"
USMS_DIRS=$(find "$WKS_PATH"  -mindepth 1 -type d)

echo "input dir : $USMS_DIRS"

echo "run several usms (parallelized)"
echo "output dir : $OUTPUT_DIR"
# script to run stics in parallel on the different usms in $USMS_DIRS. a single run call the bash script sticsrun.sh with the usm directory and the output directory as arguments
#printf "%s\n" "$USMS_DIRS" | xargs -d '\n' -n1 -P8 -I"{}" bash -c '/mnt/d/"Mes Donnees"/TCMP/github/ModFileGen/src/modfilegen/Converter/SticsConverter/sticsrun.sh "{}" "$2"' _ {} "$OUTPUT_DIR"
printf "%s\n" "$USMS_DIRS" | parallel -d '\n' -P THREADS  -q bash -c '/mnt/d/"Mes Donnees"/TCMP/github/ModFileGen/src/modfilegen/Converter/SticsConverter/sticsrun.sh "{}" "$2"' _ {} "$OUTPUT_DIR"

echo "*************************"
echo "*  STICS completed !!!"
echo "*************************"
exit 0