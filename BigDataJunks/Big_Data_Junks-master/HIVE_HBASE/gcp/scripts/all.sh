#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

#Execute all scripts

#Clean HDFS

echo "################################## Cleaning HDFS ###################################################################"

hdfs dfs -rm -r /user/input

#make hdfs directories

echo "################################## Creating HDFS PATHS/Directories ##################################################"

hdfs dfs -mkdir /user/input/
hdfs dfs -mkdir /user/input/mysql
hdfs dfs -mkdir /user/input/sqlserver
hdfs dfs -mkdir /user/input/postgresql
hdfs dfs -mkdir /user/input/csv
hdfs dfs -mkdir /user/input/csv/opensource
hdfs dfs -mkdir /user/input/csv/spotify_api

#execute all scripts
echo "################################## Executing Sqoop Jobs #############################################################"

sh /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/sqoop_data_ingestion/execute_jobs.sh

echo "################################## Copying CSV to HDFS ##############################################################"

sh /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/sqoop_data_ingestion/csv_to_hdfs.sh

echo "################################## Creating Hive External Tables ####################################################"

#hive -f create_hive_external_tables.hql

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/hive_gcp_external.hql

echo "################################## Creating Hive Internal Tables ####################################################"

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/create_hive_internal_tables.hql

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/build_spotify_api_tables.hql

echo "################################## Integrating Hive Internal Tables to Hbase #########################################"

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/create_hive_hbase_tables.hql

echo "################################## Completed #########################################################################"





