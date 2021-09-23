LOAD DATA 
INFILE "/var/lib/mysql-files/I80_sample.txt"
INTO TABLE raw_observation
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';