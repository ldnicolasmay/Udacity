#!/usr/bin/env bash

echo "date,key_count" > ooni_jsonl_key_counts.csv

for YEAR in "2013" "2014" "2015" "2016" "2017" "2018" "2019" "2020"; do
  for MONTH in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12"; do
    for DAY in "05" "15" "25"; do
      echo "$YEAR-$MONTH-$DAY,$(aws s3 ls s3://ooni-data/autoclaved/jsonl/$YEAR-$MONTH-$DAY/ | wc -l)"
      echo "$YEAR-$MONTH-$DAY,$(aws s3 ls s3://ooni-data/autoclaved/jsonl/$YEAR-$MONTH-$DAY/ | wc -l)" >> \
          ooni_jsonl_key_counts.csv
    done
  done
done

