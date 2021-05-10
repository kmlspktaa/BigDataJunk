#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

#creat sqoop jobs for tables on the differrent RDBMS servers

sqoop job --create albums -- import --connect jdbc:mysql://localhost/musicbrainz --username root --password-file file:///home/fieldemployee/bin/passwords/mysql.password --table albums --m 1 --target-dir /user/input/mysql/albums/

sqoop job --create edges -- import --connect jdbc:mysql://localhost/musicbrainz --username root --password-file file:///home/fieldemployee/bin/passwords/mysql.password --m 1 --table albums --columns id,edge_id,fr0m,t0,label --target-dir /user/input/mysql/edges/

sqoop job --create  artist -- import --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password-file file:///home/fieldemployee/bin/passwords/postgres_local.password --m 1 --table artist --target-dir /user/input/postgresql/artist/

sqoop job --create  genre -- import --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password-file file:///home/fieldemployee/bin/passwords/postgres_local.password --m 1 --table genre --target-dir /user/input/postgresql/genre/

sqoop job --create  subgenre -- import --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password-file file:///home/fieldemployee/bin/passwords/postgres_local.password --m 1 --table subgenre --target-dir /user/input/postgresql/subgenre/

sqoop job --create  year -- import --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password-file file:///home/fieldemployee/bin/passwords/postgres_local.password --m 1 --table year --target-dir /user/input/postgresql/year/

sqoop job --create playlist -- import --connect 'jdbc:sqlserver://localhost:1433;databaseName=master' --username 'SA' --password-file file:///home/fieldemployee/bin/passwords/sqlserver.password --m 1 --table playlist --target-dir /user/input/sqlserver/playlist/

sqoop job --create playlisttrack -- import --connect 'jdbc:sqlserver://localhost:1433;databaseName=master' --username 'SA' --password-file file:///home/fieldemployee/bin/passwords/sqlserver.password --m 1 --table playlisttrack --target-dir /user/input/sqlserver/playlisttrack/

sqoop job --create tracks -- import --connect 'jdbc:sqlserver://localhost:1433;databaseName=master' --username 'SA' --password-file file:///home/fieldemployee/bin/passwords/sqlserver.password --m 1 --table tracks --target-dir /user/input/sqlserver/tracks/


