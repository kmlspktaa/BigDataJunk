#!/bin/sh
#Author: Abuchi Okeke
#10/20/2020

#copy csv to hdfs 

hdfs dfs -put /home/fieldemployee/bin/datasets/opensource.csv /user/input/csv/opensource

hdfs dfs -put /home/fieldemployee/bin/datasets/spotify.csv /user/input/csv/spotify_api
