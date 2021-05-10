-- Author: Abuchi Okeke
-- Modified: 10/16/2020

-- create database raw;
use raw;

DROP TABLE IF EXISTS raw.genre_external;
CREATE EXTERNAL TABLE raw.genre_external(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")
LOCATION '/user/input/postgresql/genre/'
TBLPROPERTIES ("skip.header.line.count"="1");


DROP TABLE IF EXISTS raw.stocks_dev;
CREATE EXTERNAL TABLE raw.stocks_dev(
    open float,
    high float,
    low float,
    close float,
    volume int,
    datee TIMESTAMP,
    timestampp TIMESTAMP)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")
LOCATION '/user/kafka/streams';
#TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.subgenre_external;
CREATE EXTERNAL TABLE raw.subgenre_external (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")
LOCATION '/user/input/postgresql/subgenre/'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.year_external;
CREATE EXTERNAL TABLE raw.year_external(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")
LOCATION '/user/input/postgresql/year/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.tracks_external;
CREATE EXTERNAL TABLE raw.tracks_external (
    Trackid INT,
    Name VARCHAR(255),
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer VARCHAR(255),
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL (10,2))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")   
LOCATION '/user/input/sqlserver/tracks/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

-- LOAD DATA INPATH '/user/databases/sqlserver/musicdb/tracks/' OVERWRITE INTO TABLE raw.tracks_external;


-- Create table playlist
DROP TABLE IF EXISTS raw.playlist_external;
CREATE EXTERNAL TABLE raw.playlist_external (
    id INT,
    name VARCHAR(1000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/sqlserver/playlist/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

-- Create table playlist track
DROP TABLE IF EXISTS raw.playlisttrack_external;
CREATE EXTERNAL TABLE raw.playlisttrack_external (
    Playlistid int,
    trackid int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/sqlserver/playlisttrack/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.albums_external;
CREATE EXTERNAL TABLE raw.albums_external(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/mysql/albums/';
-- TBLPROPERTIES ("skip.header.line.count"="1");


-- Create table artist
DROP TABLE IF EXISTS raw.artist_external;
CREATE EXTERNAL TABLE raw.artist_external (
    id int,
    artist_id VARCHAR(255),
    label VARCHAR(255),
    name VARCHAR(10000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/postgresql/artist/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

-- Create table edges
DROP TABLE IF EXISTS raw.edges_external;
CREATE EXTERNAL TABLE raw.edges_external (
    id int,    
    edge_id VARCHAR(255),
    fr0m VARCHAR(255),
    t0 VARCHAR(255),
    label VARCHAR(1000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/mysql/edges/';
-- TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS raw.opensource_external;
CREATE EXTERNAL TABLE raw.opensource_external (
    user_id VARCHAR(255),
    artistname VARCHAR(255),
    trackname VARCHAR(10000),
    playlistname VARCHAR(10000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"")  
LOCATION '/user/input/csv/opensource/'
TBLPROPERTIES ("skip.header.line.count"="1");

