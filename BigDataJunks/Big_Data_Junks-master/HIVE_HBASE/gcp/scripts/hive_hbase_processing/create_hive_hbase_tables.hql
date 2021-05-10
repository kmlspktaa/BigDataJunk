-- Author: Abuchi Okeke
-- Modified: 10/17/2020


-- create database hbase_dsl;

use hbase_dsl;

DROP TABLE IF EXISTS hbase_dsl.genre_hb;

CREATE TABLE hbase_dsl.genre_hb(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    names VARCHAR(255))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:labels,x:names");   

insert into hbase_dsl.genre_hb (select * from dsl.genre_internal);

DROP TABLE IF EXISTS hbase_dsl.subgenre_hb;

CREATE TABLE hbase_dsl.subgenre_hb (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    names VARCHAR(255))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'    
with serdeproperties("hbase.columns.mapping"=":key,x:labels,x:names");   

insert into hbase_dsl.subgenre_hb(select * from dsl.subgenre_internal);

DROP TABLE IF EXISTS hbase_dsl.year_hb;

CREATE TABLE hbase_dsl.year_hb(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:label,x:name");   

insert into hbase_dsl.year_hb(select * from dsl.year_internal);

DROP TABLE IF EXISTS hbase_dsl.albums_hb;

CREATE TABLE hbase_dsl.albums_hb(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year int,
    number int) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:label,x:title,x:year,x:number");   

insert into hbase_dsl.albums_hb(select * from dsl.albums_internal);

DROP TABLE IF EXISTS hbase_dsl.tracks_hb;

CREATE TABLE hbase_dsl.tracks_hb(
    trackid INT,
    name VARCHAR(255),
    albumId INT,
    mediaTypeId INT,
    genreId INT,
    composer VARCHAR(255),
    milliseconds INT,
    bytes INT,
    unitPrice DECIMAL (10,2))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:name,x:albumId,x:mediaTypeId,x: genreId,x: composer,x:milliseconds,x:bytes,x:unitPrice");


-- insert values
insert into hbase_dsl.tracks_hb(select * from dsl.tracks_internal);

-- Create table playlist
DROP TABLE IF EXISTS hbase_dsl.playlist_hb;

CREATE TABLE hbase_dsl.playlist_hb (
    id INT,
    name VARCHAR(1000))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:name");   


insert into hbase_dsl.playlist_hb (select * from dsl.playlist_internal);

-- Create table playlist track
DROP TABLE IF EXISTS hbase_dsl.playlisttrack_hb;

CREATE TABLE hbase_dsl.playlisttrack_hb(
    Playlistid int,
    trackid int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:trackid");   

insert into hbase_dsl.playlisttrack_hb (select * from dsl.playlisttrack_internal);

-- Create table artist
DROP TABLE IF EXISTS hbase_dsl.artist_hb;

CREATE TABLE hbase_dsl.artist_hb (
    rowkey int,
    artist_id VARCHAR(255),
    label VARCHAR(255),
    name VARCHAR(3000))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:artist_id,x:label,x:name");   

insert into hbase_dsl.artist_hb (select * from dsl.artist_internal);

-- Create table edges
DROP TABLE IF EXISTS hbase_dsl.edges_hb;

CREATE TABLE hbase_dsl.edges_hb(
    id int,
    edge_id VARCHAR(255),
    fr0m VARCHAR(255),
    t0 VARCHAR(255),
    label VARCHAR(10000))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:edge_id,x:fr0m,x:t0,x:label");   

insert into hbase_dsl.edges_hb (select * from dsl.edges_internal);

DROP TABLE IF EXISTS hbase_dsl.opensource_hb;

CREATE TABLE hbase_dsl.opensource_hb (
    user_id VARCHAR(255),
    artistname VARCHAR(255),
    trackname VARCHAR(10000),
    playlistname VARCHAR(10000))
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:artistname,x:trackname,x:playlistname"); 

insert into hbase_dsl.opensource_hb(select * from dsl.opensource_internal);


