# This file is used to bulk load data from flat file into db
# In order to use:
#   1. save pandas dataframe to csv with timezones converted.
#   2. move file into an area that the db can read the file
#   3. open msql shell session and run this script, replacing tablenames and file names as appropriate
#   4. run by using 'source load_data.sql'
#   5.

DROP TABLE IF EXISTS xom_1min;
CREATE TABLE xom_1min
(
    dt timestamp NOT NULL
    , open double
    , high double
    , low double
    , close double
    , volume bigint unsigned
    , PRIMARY KEY(dt)
    ) ENGINE=InnoDB;

LOAD DATA
#     INFILE '/media/chuck/DATA/Dropbox/chuck/market_data/A.Last.txt'
    INFILE '/var/lib/mysql-files/XOM.Last_TZ_converted.txt'
    INTO TABLE xom_1min
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    (@dt, open, high, low, close, volume)
    set dt = str_to_date(@dt, '%Y-%c-%e %H:%i:%s')
;


