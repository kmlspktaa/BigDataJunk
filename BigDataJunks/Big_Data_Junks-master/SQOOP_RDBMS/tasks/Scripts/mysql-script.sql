/* Author: Abuchi Okeke
 * Description: MySQL script to create database, tables and importing data through .csv file
 */


-- Create database musicbrainz;
-- SELECT * from musicbrainz.albums;

-- SHOW VARIABLES LIKE "secure_file_priv";

-- SET GLOBAL local_infile=1;

-- Create table albums
CREATE TABLE musicbrainz.albums (
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT,
    PRIMARY KEY (id)
);

-- Load data into tables from the .csv file

LOAD DATA INFILE '/var/lib/mysql-files/Albums.csv' 
INTO TABLE musicbrainz.albums
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Create table artist
CREATE TABLE musicbrainz.artist (
    id VARCHAR(255),
    label VARCHAR(255),
    name VARCHAR(10000),
    PRIMARY KEY (id)
);

LOAD DATA INFILE '/var/lib/mysql-files/Artist.csv' 
INTO TABLE musicbrainz.artist 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- Create table edges
CREATE TABLE musicbrainz.edges (
    id VARCHAR(255),
    fr0m VARCHAR(255),
    t0 VARCHAR(255),
    label VARCHAR(10000),
    PRIMARY KEY (id)
);

LOAD DATA INFILE '/var/lib/mysql-files/Edges.csv' 
INTO TABLE musicbrainz.edges
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


