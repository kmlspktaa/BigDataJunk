-- Author: Abuchi Okeke
-- Modified: 10/16/2020


DROP DATABASE IF EXISTS raw cascade;
create database raw;
use raw;

DROP TABLE IF EXISTS raw.genre_external;
CREATE EXTERNAL TABLE raw.genre_external(
    id string,
    labels string,
    names string)
stored as avro
LOCATION '/user/input/postgresql/genre/';
--TBLPROPERTIES ("skip.header.line.count"="1");


DROP TABLE IF EXISTS raw.subgenre_external;
CREATE EXTERNAL TABLE raw.subgenre_external (
    id string,
    labels string,
    names string)
stored as avro
LOCATION '/user/input/postgresql/subgenre/';
--TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.year_external;
CREATE EXTERNAL TABLE raw.year_external(
    id string,
    label string,
    name int)
stored as avro
LOCATION '/user/input/postgresql/year/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.tracks_external;
CREATE EXTERNAL TABLE raw.tracks_external (
    Trackid int,
    Name string,
    AlbumId int,
    MediaTypeId int,
    GenreId int,
    Composer string,
    Milliseconds int,
    Bytes int,
    UnitPrice double)
stored as avro  
LOCATION '/user/input/postgresql/tracks/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.tracks_external;
CREATE EXTERNAL TABLE raw.tracks_external (notused INT)
   ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/postgresql/tracks/'
    TBLPROPERTIES ("skip.header.line.count"="2",
    'avro.schema.literal'='{
      "namespace": "com.music",
      "name": "opensource_schema",
      "type": "record",
      "fields": [ { "name": "Trackid", "type": ["int", "null"] },
{ "name": "Name", "type": ["string", "null"] },
{"name": "AlbumId", "type": ["int", "null"]},
{"name": "MediaTypeId", "type": ["int", "null"]},
{"name": "GenreId", "type": ["int", "null"]},
{ "name": "Composer", "type": ["string", "null"] },
{ "name": "Milliseconds", "type": ["int", "null"] },
{ "name": "Bytes", "type": ["int", "null"] },
{ "name": "UnitPrice", "type": ["float", "null"] }  ]
      }');

-- LOAD DATA INPATH '/user/databases/sqlserver/musicdb/tracks/' OVERWRITE INTO TABLE raw.tracks_external;

-- Create table playlist
DROP TABLE IF EXISTS raw.playlist_external;
CREATE EXTERNAL TABLE raw.playlist_external (
    id int,
    name string)
stored as avro  
LOCATION '/user/input/sqlserver/playlist/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

-- Create table playlist track
DROP TABLE IF EXISTS raw.playlisttrack_external;
CREATE EXTERNAL TABLE raw.playlisttrack_external (
    Playlistid int,
    trackid int)
stored as avro   
LOCATION '/user/input/sqlserver/playlisttrack/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.albums_external;
CREATE EXTERNAL TABLE raw.albums_external(
    id string,
    label string,
    title string,
    year int,
    number int)
stored as avro  
LOCATION '/user/input/mysql/albums/';
-- TBLPROPERTIES ("skip.header.line.count"="1");


-- Create table artist
DROP TABLE IF EXISTS raw.artist_external;
CREATE EXTERNAL TABLE raw.artist_external (
    id int,
    artist_id string,
    label string,
    name string)
stored as avro  
LOCATION '/user/input/postgresql/artist/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

-- Create table edges
DROP TABLE IF EXISTS raw.edges_external;
CREATE EXTERNAL TABLE raw.edges_external (
    id int,    
    edge_id string,
    fr0m string,
    t0 string,
    label string)
stored as avro   
LOCATION '/user/input/mysql/edges/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.opensource_external;
CREATE EXTERNAL TABLE raw.opensource_external (
    user_id string,
    artistname string,
    trackname string,
    playlistname string)
stored as avro  
LOCATION '/user/input/csv/opensource/';
--TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.opensource_external;
CREATE EXTERNAL TABLE raw.opensource_external (notused INT)
   ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/csv/opensource/'
    TBLPROPERTIES ("skip.header.line.count"="1",
    'avro.schema.literal'='{
      "namespace": "com.music",
      "name": "opensource_schema",
      "type": "record",
      "fields": [ { "name": "user_id", "type": ["string","null"] },
{ "name": "artistname", "type": ["string","null"] },
{ "name": "trackname", "type": ["string","null"] },
{ "name": "playlistname", "type": ["string","null"] }  ]
      }');


DROP TABLE IF EXISTS raw.opensource_external;
CREATE EXTERNAL TABLE raw.opensource_external (notused INT)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='file:///home/fieldemployee/bin/schemas/opensource.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/csv/opensource/';

