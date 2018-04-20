CREATE DATABASE IF NOT EXISTS m2h2;

use m2h2;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.tez.java.opts=-Xmx1024m;

CREATE TABLE IF NOT EXISTS carriers (
  code varchar(5),
  description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
) 
TBLPROPERTIES ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH 'carriers.csv'  INTO TABLE carriers;

CREATE TABLE IF NOT EXISTS airports (
  iata varchar(5),
  airport string,
  city string,
  state varchar(5),
  country string,
  lat float,
  long float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
) 
TBLPROPERTIES ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH 'airports.csv'  INTO TABLE airports;

CREATE TABLE IF NOT EXISTS flight2007(Year INT, Month INT, DayofMonth INT, 
DayOfWeek INT, DepTime INT,	CRSDepTime INT,	ArrTime INT, 
CRSArrTime INT, UniqueCarrier String, FlightNum INT,
TailNum String,	ActualElapsedTime INT, CRSElapsedTime INT, 
AirTime INT, ArrDelay INT,	DepDelay INT, Origin String, 
Dest String, Distance INT, TaxiIn INT, TaxiOut INT,	Cancelled INT, 
CancellationCode INT, Diverted INT, CarrierDelay INT, WeatherDelay INT,	
NASDelay INT, SecurityDelay INT, LateAircraftDela INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
) 
TBLPROPERTIES ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '2007.csv'  INTO TABLE flight2007;


CREATE TABLE IF NOT EXISTS carriers_orc (Code String, Description String)
STORED AS ORC;
INSERT INTO carriers_orc SELECT * FROM carriers;

CREATE TABLE IF NOT EXISTS airports_orc(iata String, airport String, city String, 
state String, country String, 
lat FLOAT, long FLOAT)
CLUSTERED BY(state) SORTED BY (city ASC) INTO 52 BUCKETS
STORED AS ORC;
INSERT INTO airports_orc SELECT * FROM airports;

CREATE TABLE IF NOT EXISTS flight2007_orc(Year INT, Month INT, DayofMonth INT, 
DayOfWeek INT, DepTime INT,	CRSDepTime INT,	ArrTime INT, 
CRSArrTime INT, UniqueCarrier String, FlightNum INT,
TailNum String,	ActualElapsedTime INT, CRSElapsedTime INT, 
AirTime INT, ArrDelay INT,	DepDelay INT, Origin String, 
Dest String, Distance INT, TaxiIn INT, TaxiOut INT,	Cancelled INT, 
CancellationCode INT, Diverted INT, CarrierDelay INT, WeatherDelay INT,	
NASDelay INT, SecurityDelay INT, LateAircraftDela INT)
CLUSTERED BY(Month) SORTED BY (DayofMonth ASC) INTO 12 BUCKETS
STORED AS ORC;
INSERT INTO  flight2007_orc SELECT * FROM flight2007;


