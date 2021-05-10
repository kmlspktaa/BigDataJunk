
use raw;

DROP TABLE IF EXISTS raw.spotify_music_data;
CREATE EXTERNAL TABLE raw.spotify_music_data(
    id int,
    artist_name  string,
    track_name	 string,
    track_id	 string,
    popularity	 int,
    year int,
    danceability  double,
    energy	 double,
    key int,
    loudness  double,
    mode_  int,	
    speechiness	 double,
    acousticness  double,
    instrumentalness  double,
    liveness  double,
    valence	 double,
    tempo  double,
    type  string,
    uri	 string,
    track_href	  string,
    analysis_url   string,
    duration_ms	 int,
    time_signature  int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='file:///home/fieldemployee/bin/schemas/spotify.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/csv/spotify_api';

DROP TABLE IF EXISTS raw.spotify_music_data;
CREATE EXTERNAL TABLE raw.spotify_music_data(notused INT)
   ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/csv/spotify_api'
  TBLPROPERTIES (
  'avro.schema.literal'='{
  "type" : "record",
  "name" : "topLevelRecord",
  "fields" : [ {
    "name" : "_c0",
    "type" : [ "string", "null" ]
  }, {
    "name" : "artist_name",
    "type" : [ "string", "null" ]
  }, {
    "name" : "track_name",
    "type" : [ "string", "null" ]
  }, {
    "name" : "track_id",
    "type" : [ "string", "null" ]
  }, {
    "name" : "popularity",
    "type" : [ "string", "null" ]
  }, {
    "name" : "year",
    "type" : [ "string", "null" ]
  }, {
    "name" : "danceability",
    "type" : [ "string", "null" ]
  }, {
    "name" : "energy",
    "type" : [ "string", "null" ]
  }, {
    "name" : "key",
    "type" : [ "string", "null" ]
  }, {
    "name" : "loudness",
    "type" : [ "string", "null" ]
  }, {
    "name" : "mode",
    "type" : [ "string", "null" ]
  }, {
    "name" : "speechiness",
    "type" : [ "string", "null" ]
  }, {
    "name" : "acousticness",
    "type" : [ "string", "null" ]
  }, {
    "name" : "instrumentalness",
    "type" : [ "string", "null" ]
  }, {
    "name" : "liveness",
    "type" : [ "string", "null" ]
  }, {
    "name" : "valence",
    "type" : [ "string", "null" ]
  }, {
    "name" : "tempo",
    "type" : [ "string", "null" ]
  }, {
    "name" : "type",
    "type" : [ "string", "null" ]
  }, {
    "name" : "uri",
    "type" : [ "string", "null" ]
  }, {
    "name" : "track_href",
    "type" : [ "string", "null" ]
  }, {
    "name" : "analysis_url",
    "type" : [ "string", "null" ]
  }, {
    "name" : "duration_ms",
    "type" : [ "string", "null" ]
  }, {
    "name" : "time_signature",
    "type" : [ "string", "null" ]
  } ]
}');


use dsl;


DROP TABLE IF EXISTS dsl.spotify_music_internal;
CREATE EXTERNAL TABLE dsl.spotify_music_internal(
    id int,
    artist_name  string,
    track_name	 string,
    track_id	 string,
    popularity	 int,
    year int,
    danceability  double,
    energy	 double,
    key int,
    loudness  double,
    mode_  int,	
    speechiness	 double,
    acousticness  double,
    instrumentalness  double,
    liveness  double,
    valence	 double,
    tempo  double,
    type  string,
    uri	 string,
    track_href	  string,
    analysis_url   string,
    duration_ms	 int,
    time_signature  int)
stored as orc;

insert into dsl.spotify_music_internal select * from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.track_details;
CREATE TABLE dsl.track_details(
    id  int,
    artist_name  VARCHAR(1000),
    track_name	 VARCHAR(2500),
    track_id	 VARCHAR(1000),
    popularity	 int,
    year int)
stored as orc;



insert into dsl.track_details select
    _c0,
    artist_name,
    track_name,
    track_id,
    popularity,
    year from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf1;
CREATE TABLE dsl.auf1(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    danceability decimal(10,4),
    energy	decimal(10,4))
stored as orc;

insert into dsl.auf1 select 
    id,
    track_id,
    year,
    danceability,
    energy
from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf2;
CREATE TABLE dsl.auf2(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    key int,
    loudness decimal(10,4))
stored as orc;

insert into dsl.auf2 select 
    id,
    track_id,
    year,
    key,
    loudness
from raw.spotify_music_data

DROP TABLE IF EXISTS dsl.auf3;
CREATE TABLE dsl.auf3(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    mode int,	
    speechiness	decimal(10,5))
stored as orc;

insert into dsl.auf3 select 
    id,
    track_id,
    year,
    mode,	
    speechiness
from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf4;
CREATE TABLE dsl.auf4(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    acousticness decimal(10,6),
    instrumentalness decimal(10,6))
stored as orc;

insert into dsl.auf4 select 
    id,
    track_id,
    year,
    acousticness,
    instrumentalness
from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf5;
CREATE TABLE dsl.auf5(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    liveness decimal(10,6),
    valence	decimal(10,4))
stored as orc;

insert into dsl.auf5 select 
    id,
    track_id,
    year,
    liveness,
    valence
from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf6;
CREATE TABLE dsl.auf6(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    tempo decimal(10,4),
    type VARCHAR(2500))
stored as orc;

insert into dsl.auf6 select 
    id,
    track_id,
    year,
    tempo,
    type 
from raw.spotify_music_data;


DROP TABLE IF EXISTS dsl.auf7;
CREATE TABLE dsl.auf7(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    uri	VARCHAR(2500),
    track_href	VARCHAR(2500),
    analysis_url VARCHAR(2500))
stored as orc;

insert into dsl.auf7 select 
    id,
    track_id,
    year,
    uri,
    track_href,
    analysis_url
from raw.spotify_music_data;

DROP TABLE IF EXISTS dsl.auf8;
CREATE TABLE dsl.auf8(
    id int,
    track_id	VARCHAR(1000),
    year	int,
    duration_ms	int,
    time_signature int)
stored as orc;

insert into dsl.auf8 select 
    id,
    track_id,
    year,
    duration_ms,
    time_signature
from raw.spotify_music_data;

use asl;

DROP TABLE IF EXISTS asl.track_details;
CREATE TABLE asl.track_details(id int, artist_name VARCHAR(1000), track_name VARCHAR(2500), track_id VARCHAR(1000), popularity int, year int)
stored as orc;

insert into asl.track_details select * from dsl.track_details 
order by popularity, year desc 
limit 20;

DROP TABLE IF EXISTS asl.2020_songs;
CREATE TABLE asl.2020_songs(id int, artist_name VARCHAR(1000), track_name VARCHAR(2500), track_id VARCHAR(1000), popularity int, year int)
stored as orc;

insert into asl.2020_songs select * from dsl.track_details 
where year = 2020;


