#!/bin/bash

IFS=","

for times in \
  2022-01-01T00:00:00Z,2022-01-15T00:00:00Z \
  2022-01-15T00:00:00Z,2022-02-01T00:00:00Z \
  2022-02-01T00:00:00Z,2022-02-15T00:00:00Z \
  2022-02-15T00:00:00Z,2022-03-01T00:00:00Z \
  2022-03-01T00:00:00Z,2022-03-15T00:00:00Z \
  2022-03-15T00:00:00Z,2022-04-01T00:00:00Z;
do
  set -- $times
  FORMATS=clickhouse SCALE=1000 BULK_DATA_DIR=$BULK_DATA TS_START=$1 TS_END=$2 USE_CASE=iot ./generate_data.sh &

done

echo "waiting for data generation to complete"
wait
echo "Data generated"