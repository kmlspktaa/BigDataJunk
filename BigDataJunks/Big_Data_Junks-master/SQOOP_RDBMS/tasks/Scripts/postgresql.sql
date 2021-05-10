/* Author: Abuchi Okeke
 * Description: postgresql script to create database, tables and importing data through .csv file
 */

-- SET search_path = postgres;
-- Create database musicbrainz;

-- SET/USE Database musicbrainz
--SELECT * from public.genre22;

-- Create table genre
-- SHOW VARIABLES LIKE "secure_file_priv";

CREATE TABLE public.genre(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255),
    PRIMARY KEY (id)
);
-- SET GLOBAL local_infile=1;

-- Load data into tables tracks from the .csv file
COPY public.genre FROM '/var/lib/mysql-files/Genre.csv' WITH (FORMAT csv);


-- Create table subgenre
CREATE TABLE public.subgenre (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255),
    PRIMARY KEY (id)
);

-- Load data into tables tracks from the .csv file
COPY public.subgenre FROM '/var/lib/mysql-files/Subgenre.csv' WITH (FORMAT csv);


-- Create table year
CREATE TABLE public.year (
    id VARCHAR(255),
    label VARCHAR(255),
    name int,
    PRIMARY KEY (id)
);

-- Load data into tables tracks from the .csv file
COPY public.year FROM '/var/lib/mysql-files/Year.csv' delimiter ',' CSV HEADER ;





