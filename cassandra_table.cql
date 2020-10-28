
USE rjerbaka;

DROP TABLE IF EXISTS flight_by_datetime;
CREATE TABLE flight_by_datetime
(   
    year int,
    month int,
    day int,
    dep_hour int,
    dep_min int,
    arr_hour int,
    arr_min int,
    depdelay int,
    arrdelay int,
    carrierdel int,
    weatherdel int,
    NASdel int,
    securitydel int,
    lateACdel int,
    flightnum int,
    primary key ((year, month, day), dep_hour, dep_min, flightnum)
);

DROP TABLE IF EXISTS flight_by_dayofweek;
CREATE TABLE flight_by_dayofweek
(
    year int,
    month int,
    day int,
    dayofweek int,
    dep_hour int,
    dep_min int,
    arr_hour int,
    arr_min int,
    depdelay int,
    arrdelay int,
    carrierdel int,
    weatherdel int,
    NASdel int,
    securitydel int,
    lateACdel int,
    flightnum int,
    primary key ((year, dayofweek), month, day, dep_hour, flightnum)
);

DROP TABLE IF EXISTS flight_by_dephour;
CREATE TABLE flight_by_dephour
(
    year int,
    month int,
    day int,
    dep_hour int,
    dep_min int,
    arr_hour int,
    arr_min int,
    depdelay int,
    arrdelay int,
    carrierdel int,
    weatherdel int,
    NASdel int,
    securitydel int,
    lateACdel int,
    flightnum int,
    primary key ((dep_hour, dep_min, year), month, day, flightnum)
);