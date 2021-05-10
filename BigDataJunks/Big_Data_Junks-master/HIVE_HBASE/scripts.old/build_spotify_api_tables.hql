-- create hive external tables
use raw;

DROP TABLE IF EXISTS raw.spotify_music_data;
CREATE EXTERNAL TABLE raw.spotify_music_data(
    id int,
    artist_name VARCHAR(1000),
    track_name	VARCHAR(2500),
    track_id	VARCHAR(1000),
    popularity	int,
    year_	int,
    danceability decimal(10,4),
    energy	decimal(10,4),
    key_ int,
    loudness decimal(10,4),
    mode_ int,	
    speechiness	decimal(10,5),
    acousticness decimal(10,6),
    instrumentalness decimal(10,6),
    liveness decimal(10,6),
    valence	decimal(10,4),
    tempo decimal(10,4),
    type_ VARCHAR(2500),
    uri	VARCHAR(2500),
    track_href	VARCHAR(2500),
    analysis_url VARCHAR(2500),
    duration_ms	int,
    time_signature int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/input/csv/spotify_api';


-- create internal tables

use dsl;

DROP TABLE IF EXISTS dsl.track_details;
CREATE TABLE dsl.track_details(
    row_id int,
    id  int,
    artist_name  VARCHAR(1000),
    track_name	 VARCHAR(2500),
    track_id	 VARCHAR(1000),
    popularity	 int,
    year_	 int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.track_details (select ROW_NUMBER() over(),
    id,
    artist_name,
    track_name,
    track_id,
    popularity,
    year_
from raw.spotify_music_data 
order by year_ desc);

-- audio features (auf)

DROP TABLE IF EXISTS dsl.auf1;
CREATE TABLE dsl.auf1(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    danceability decimal(10,4),
    energy	decimal(10,4))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf1 (select 
    id,
    track_id,
    year_,
    danceability,
    energy
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf2;
CREATE TABLE dsl.auf2(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    key_ int,
    loudness decimal(10,4))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf2 (select 
    id,
    track_id,
    year_,
    key_,
    loudness
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf3;
CREATE TABLE dsl.auf3(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    mode_ int,	
    speechiness	decimal(10,5))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf3 (select 
    id,
    track_id,
    year_,
    mode_,	
    speechiness
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf4;
CREATE TABLE dsl.auf4(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    acousticness decimal(10,6),
    instrumentalness decimal(10,6))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf4 (select 
    id,
    track_id,
    year_,
    acousticness,
    instrumentalness
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf5;
CREATE TABLE dsl.auf5(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    liveness decimal(10,6),
    valence	decimal(10,4))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf5 (select 
    id,
    track_id,
    year_,
    liveness,
    valence
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf6;
CREATE TABLE dsl.auf6(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    tempo decimal(10,4),
    type_ VARCHAR(2500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf6 (select 
    id,
    track_id,
    year_,
    tempo,
    type_ 
from raw.spotify_music_data);


DROP TABLE IF EXISTS dsl.auf7;
CREATE TABLE dsl.auf7(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    uri	VARCHAR(2500),
    track_href	VARCHAR(2500),
    analysis_url VARCHAR(2500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf7 (select 
    id,
    track_id,
    year_,
    uri,
    track_href,
    analysis_url
from raw.spotify_music_data);

DROP TABLE IF EXISTS dsl.auf8;
CREATE TABLE dsl.auf8(
    id int,
    track_id	VARCHAR(1000),
    year_	int,
    duration_ms	int,
    time_signature int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.auf8 (select 
    id,
    track_id,
    year_,
    duration_ms,
    time_signature
from raw.spotify_music_data);

use asl;

DROP TABLE IF EXISTS asl.track_details;
CREATE TABLE asl.track_details(id int, artist_name VARCHAR(1000), track_name VARCHAR(2500), track_id VARCHAR(1000), popularity int, year_ int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into asl.track_details (select * from dsl.track_details 
order by popularity, year_ desc 
limit 20);

DROP TABLE IF EXISTS asl.2020_songs;
CREATE TABLE asl.2020_songs(id int, artist_name VARCHAR(1000), track_name VARCHAR(2500), track_id VARCHAR(1000), popularity int, year_ int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into asl.2020_songs(select * from dsl.track_details 
where year_ = 2020);

-- HBase Tables

use hbase_dsl;

DROP TABLE IF EXISTS hbase_dsl.track_details_hb;
CREATE TABLE hbase_dsl.track_details_hb(
    row_id int,
    id int,
    artist_name  VARCHAR(1000),
    track_name	 VARCHAR(2500),
    track_id	 VARCHAR(1000),
    popularity	 int,
    year_	 int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:id,x:artist_name,x:track_name,x:track_id,x:popularity,x:year_");   

insert into hbase_dsl.track_details_hb(select * from dsl.track_details order by year_ desc);




