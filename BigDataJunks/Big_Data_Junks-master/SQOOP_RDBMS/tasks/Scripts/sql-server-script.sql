/* Author: Abuchi Okeke
 * Description: SQL server script to create database, tables and importing data through .csv file
 */


-- Create database musicbrainz;
SELECT * from musicbrainz.Edges;

-- Create table tracks
-- SHOW VARIABLES LIKE "secure_file_priv";

CREATE TABLE musicbrainz.guest.tracks (
    Trackid INT,
    Name VARCHAR(255),
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer VARCHAR(255),
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL (10,2),
    PRIMARY KEY (Trackid)
);
-- SET GLOBAL local_infile=1;

-- Load data into tables tracks from the .csv file

BULK INSERT musicbrainz.guest.tracks 
    FROM '/var/lib/mysql-files/Track.csv' 
    WITH
    (
    FORMAT = 'CSV', 
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
    )
    
    
-- Create table playlist
CREATE TABLE musicbrainz.guest.playlist (
    id INT,
    name VARCHAR(1000),
    PRIMARY KEY (id)
);

-- Load data into tables tracks from the .csv file

BULK INSERT musicbrainz.guest.playlist 
    FROM '/var/lib/mysql-files/Playlist.csv' 
    WITH
    (
    FORMAT = 'CSV', 
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
    )
    

-- Create table playlist track
CREATE TABLE musicbrainz.guest.playlistTrack (
    Playlistid int,
    trackid int,
);

-- Load data into tables tracks from the .csv file

BULK INSERT musicbrainz.guest.playlistTrack 
    FROM '/var/lib/mysql-files/PlaylistTrack.csv' 
    WITH
    (
    FORMAT = 'CSV', 
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
    )
    


