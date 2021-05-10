#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

#Execute all scripts

#Clean HDFS

echo "################################## Cleaning HDFS ###################################################################"

hdfs dfs -rm -r /user/input


#execute all scripts
echo "################################## Running Spark Jobs #############################################################"

spark-submit /home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/src/rdbms_spark_df.py


echo "################################## Creating Hive External Tables ####################################################"

#hive -f create_hive_external_tables.hql

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/hive_gcp_external.hql

echo "################################## Creating Hive Internal Tables ####################################################"

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/create_hive_internal_tables.hql

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/build_spotify_api_tables.hql

echo "################################## Integrating Hive Internal Tables to Hbase #########################################"

hive -f /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/hive_hbase_processing/create_hive_hbase_tables.hql

echo "################################## Completed #########################################################################"





