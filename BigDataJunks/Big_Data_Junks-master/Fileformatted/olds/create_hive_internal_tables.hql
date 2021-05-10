
-- Author: Abuchi Okeke
-- Modified: 10/16/2020

-- create database dsl;
-- create database asl;

use dsl;

DROP TABLE IF EXISTS dsl.genre_internal;
CREATE TABLE dsl.genre_internal(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.genre_internal (select * from raw.genre_external);


DROP TABLE IF EXISTS dsl.subgenre_internal;
CREATE TABLE dsl.subgenre_internal (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.subgenre_internal (select * from raw.subgenre_external);

DROP TABLE IF EXISTS dsl.year_internal;
CREATE TABLE dsl.year_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.year_internal (select * from raw.year_external);

DROP TABLE IF EXISTS dsl.tracks_internal;
CREATE TABLE dsl.tracks_internal (
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
"quoteChar" = "\"") ;


-- insert values
insert into dsl.tracks_internal (select * from raw.tracks_external);

-- Create table playlist
DROP TABLE IF EXISTS dsl.playlist_internal;
CREATE TABLE dsl.playlist_internal (
    id INT,
    name VARCHAR(1000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.playlist_internal (select * from raw.playlist_external);

-- Create table playlist track
DROP TABLE IF EXISTS dsl.playlisttrack_internal;
CREATE TABLE dsl.playlisttrack_internal (
    Playlistid int,
    trackid int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.playlisttrack_internal (select * from raw.playlisttrack_external);

DROP TABLE IF EXISTS dsl.albums_internal;
CREATE TABLE dsl.albums_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.albums_internal (select * from raw.albums_external);

-- Create table artist
DROP TABLE IF EXISTS dsl.artist_internal;
CREATE TABLE dsl.artist_internal (
    id int,
    artist_id VARCHAR(255),
    label VARCHAR(255),
    name VARCHAR(5000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.artist_internal (select * from raw.artist_external);

-- Create table edges
DROP TABLE IF EXISTS dsl.edges_internal;
CREATE TABLE dsl.edges_internal (
    id int,
    edge_id VARCHAR(255),
    fr0m VARCHAR(255),
    t0 VARCHAR(255),
    label VARCHAR(10000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.edges_internal (select * from raw.edges_external);

DROP TABLE IF EXISTS dsl.opensource_internal;
CREATE TABLE dsl.opensource_internal (
    user_id VARCHAR(255),
    artistname VARCHAR(255),
    trackname VARCHAR(10000),
    playlistname VARCHAR(10000))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\"") ;

insert into dsl.opensource_internal (select * from raw.opensource_external);

