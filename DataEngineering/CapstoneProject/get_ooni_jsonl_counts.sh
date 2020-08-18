#!/usr/bin/env bash

for YEAR in "2012" "2013" "2014" "2015" "2016" "2017" "2018" "2019" "2020"; do
  for MONTH in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12"; do
    for DAY in "15"; do
      echo "$YEAR-$MONTH-$DAY,$(aws s3 ls s3://ooni-data/autoclaved/jsonl/$YEAR-$MONTH-$DAY/ | wc -l)"
    done
  done
done

