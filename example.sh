#!/bin/bash

set -u

wsm="./target/wasm32-wasip1/release-wasi/pq-inmem-cols2ints.wasm"

ischema="./sample.d/psch.txt"
icsv="./sample.d/input.csv"
iparquet="./sample.d/input.parquet"

export ENV_BATCH_SIZE=8192
export ENV_OFFSET=0
export ENV_COL_INDEX=3
export ENV_ENDIAN=little
export ENV_PARQUET_FILENAME="${iparquet}"
export ENV_SCALE=1000.0
export ENV_FILE_SIZE_MAX=167772160

geninput(){
  echo generating the input file...

  mkdir -p "./sample.d"
  parquet-fromcsv \
    --has-header \
    --schema "${ischema}" \
    --input-file "${icsv}" \
    --output-file "${iparquet}"
}

run_wasi(){
  wasmtime \
    run \
    --env ENV_BATCH_SIZE=$ENV_BATCH_SIZE \
    --env ENV_OFFSET=$ENV_OFFSET \
    --env ENV_COLUMN_INDEX=$ENV_COL_INDEX \
    --env ENV_ENDIAN=$ENV_ENDIAN \
    --env ENV_PARQUET_FILENAME=/guest.d/input.parquet \
    --env ENV_SCALE=$ENV_SCALE \
    --env ENV_FILE_SIZE_MAX=$ENV_FILE_SIZE_MAX \
    --dir "${PWD}/sample.d::/guest.d" \
    "${wsm}"
}

test -f "${iparquet}" || geninput

echo 'converted using wasi'
run_wasi |
  od -t d4 -v -An
echo

echo 'original'
cat -n ./sample.d/input.csv
