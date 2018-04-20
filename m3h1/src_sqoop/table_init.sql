create database if not exists sqoop;
    use sqoop;
    create table if not exists weather
    (stationId varchar(15), dat date, tmin int, tmax int, snow int, snwd int, prcp int);
    create table if not exists weather_stage
    (stationId varchar(15), dat date, tmin int, tmax int, snow int, snwd int, prcp int);