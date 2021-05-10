
sqoop import-all-tables --connect jdbc:mysql://localhost/musicbrainz --username root --password Password! --m 1 --warehouse-dir /user/databases/mysql/musicdb

sqoop import-all-tables --connect jdbc:postgresql://localhost:5432/musicbrainz --username postgres --password Password --m 1 --warehouse-dir /user/databases/postgresql/musicdb

sqoop import-all-tables --connect 'jdbc:sqlserver://localhost:1433;databaseName=master' --username 'SA' --password Password --m 1 --warehouse-dir /user/databases/sqlserver/musicdb

