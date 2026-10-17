#!/bin/sh
set -eu

src=${1:-data/hits_v1.tsv}
out=${2:-data/batches}

mkdir -p "$out"
LC_ALL=C awk -F'\t' -v out="$out" '{ print > (out "/" $6 ".tsv") }' "$src"
wc -l "$out"/*.tsv
