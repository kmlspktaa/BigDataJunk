#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020


echo "################################## Creating Hive External Tables ####################################################"

#hive -f create_hive_external_tables.hql

hive -f hive_gcp_external.hql

echo "################################## Creating Hive Internal Tables ####################################################"

hive -f create_hive_internal_tables.hql

hive -f build_spotify_api_tables.hql

echo "################################## Integrating Hive Internal Tables to Hbase #########################################"

hive -f create_hive_hbase_tables.hql


