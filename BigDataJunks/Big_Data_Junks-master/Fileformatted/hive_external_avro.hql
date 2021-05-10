
DROP DATABASE IF EXISTS raw cascade;
create database raw;
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

DROP TABLE IF EXISTS raw.year_external;
CREATE EXTERNAL TABLE raw.year_external(
    id string,
    label string,
    name int)
stored as avro
LOCATION '/user/input/postgresql/year/';

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

DROP TABLE IF EXISTS raw.playlist_external;
CREATE EXTERNAL TABLE raw.playlist_external (
    id int,
    name string)
stored as avro  
LOCATION '/user/input/sqlserver/playlist/';

DROP TABLE IF EXISTS raw.playlisttrack_external;
CREATE EXTERNAL TABLE raw.playlisttrack_external (
    Playlistid int,
    trackid int)
 ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='file:///home/fieldemployee/bin/schemas/playlisttrack.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/input/sqlserver/playlisttrack/';


DROP TABLE IF EXISTS raw.albums_external;
CREATE EXTERNAL TABLE raw.albums_external(
    id string,
    label string,
    title string,
    year int,
    number int)
stored as avro  
LOCATION '/user/input/mysql/albums/';

DROP TABLE IF EXISTS raw.artist_external;
CREATE EXTERNAL TABLE raw.artist_external (
    id int,
    artist_id string,
    label string,
    name string)
stored as avro  
LOCATION '/user/input/postgresql/artist/';

DROP TABLE IF EXISTS raw.edges_external;
CREATE EXTERNAL TABLE raw.edges_external (
    id int,    
    edge_id string,
    fr0m string,
    t0 string,
    label string)
stored as avro   
LOCATION '/user/input/mysql/edges/';

DROP TABLE IF EXISTS raw.opensource_external;
CREATE EXTERNAL TABLE raw.opensource_external (
    user_id string,
    artistname string,
    trackname string,
    playlistname string)
stored as avro  
LOCATION '/user/input/csv/opensource/';

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

DROP DATABASE IF EXISTS dsl cascade;
create database dsl;

DROP DATABASE IF EXISTS asl cascade;
create database asl;


