CREATE DATABASE IF NOT EXISTS m2h3;

use m2h3;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.tez.java.opts=-Xmx1024m;

create table if not exists impression(
BidID String, Times_tamp String, LogType   INT, iPinYouID INT,
UserAgent String, IP   String, RegionID INT, CityID INT, AdExchange INT,
Domain String, URL String, AnonymousURL String, AdSlotID INT, AdSlotWidth INT,
AdSlotHeight INT, AdSlotVisibility String, AdSlotFormat String, AdSlotFloorPrice INT,
CreativeID String, BiddingPrice INT, PayingPrice INT, LandingPageURL String, 
AdvertiserID INT, UserProfileIDs String)
row format delimited fields terminated by "\t";
load data local inpath 'imp' into table impression;

create table if not exists city(
CityID INT, CityName String
)
row format delimited fields terminated by "\t"; 
load data local inpath 'city.en.txt' into table city;
