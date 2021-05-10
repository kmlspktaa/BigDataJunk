#!/bin/sh
#Author: Abuchi Okeke
#28/20/2020

#Execute all scripts

#Clean HDFS

echo "################################## Cleaning HDFS ##############################################################################################"

#hdfs dfs -rm -r /user/input/*

echo "################################## Creating Hive External Tables (Avro Formatted) ##############################################################"


hive -f /home/fieldemployee/Big_Data_Training/Fileformatted/hive_external_avro.hql


echo "################################## Creating Hive Internal Tables (ORC Formatted) ###############################################################"

hive -f /home/fieldemployee/Big_Data_Training/Fileformatted/hive_internal_tables_orc.hql

hive -f /home/fieldemployee/Big_Data_Training/Fileformatted/build_spotify_api_tables.hql


echo "################################## Completed  ##################################################################################################"

