#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

#Delete jobs

file="/home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/sqoop_data_ingestion/tables.txt"
while IFS== read -r key val 
do

sqoop job --delete $val

done < "$file"
