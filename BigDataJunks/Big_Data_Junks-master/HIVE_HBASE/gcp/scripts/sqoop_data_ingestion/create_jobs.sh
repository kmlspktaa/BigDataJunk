#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

db1="mysql"
db2="postgresql"
db3="sqlserver"
tb1="edges"
tb2="albums"

file="/home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/sqoop_data_ingestion/tables.txt"
while IFS== read -r key val 
do

case $db1 in

  "$key")
echo "################################## Creating Sqoop Job $val #############################################################"
sqoop job --create $val -- import --connect jdbc:mysql://localhost/musicbrainz --username root --password-file file:///home/fieldemployee/bin/passwords/mysql.password --table $val --m 1 --target-dir /user/input/mysql/$val/

    ;;
esac

case $tb1 in

  "$val")
echo "################################## Creating Sqoop Job $val #############################################################"
sqoop job --create $val -- import --connect jdbc:mysql://localhost/musicbrainz --username root --password-file file:// file:///home/fieldemployee/bin/passwords/mysql.password --table $val --m 1 --columns id,edge_id,fr0m,t0,label --target-dir /user/input/mysql/$val/

    ;;
esac

case $tb2 in

  "$val")
echo "################################## Creating Sqoop Job $val #############################################################"
sqoop job --create $val -- import --connect jdbc:mysql://localhost/musicdb --username root --password-file file:///home/okekeag/bin/passwords/mysql.password --table $val --m 1 --columns id,label,title,year,number --target-dir /user/input/mysql/$val/
    ;;
esac


case $db2 in

  "$key")
echo "################################## Creating Sqoop Job $val #############################################################"
sqoop job --create  $val -- import --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password-file  file:///home/fieldemployee/bin/passwords/postgres_local.password --m 1 --table $val --target-dir /user/input/postgresql/$val/
    ;;
esac

case $db3 in

  "$key")
echo "################################## Creating Sqoop Job $val #############################################################"
sqoop job --create $val -- import --connect 'jdbc:sqlserver://localhost:1433;databaseName=master' --username 'SA' --password-file  file:///home/fieldemployee/bin/passwords/sqlserver.password --m 1 --table $val --target-dir /user/input/sqlserver/$val/
    ;;
esac

done < "$file"


