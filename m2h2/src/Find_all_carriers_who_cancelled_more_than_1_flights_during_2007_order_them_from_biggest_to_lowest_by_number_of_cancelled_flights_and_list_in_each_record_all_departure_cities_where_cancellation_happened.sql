use m2h1;
set hive.tez.java.opts=-Xmx1024m;
create temporary table if not exists cancelled_flights as
select flight2007_orc.uniquecarrier, airports_orc.city, count(1) as city_flights_count
from flight2007_orc, airports_orc
where airports_orc.iata = flight2007_orc.origin
and flight2007_orc.cancelled > 0
group by uniquecarrier, city;

select uniquecarrier, sum(city_flights_count) as count, concat_ws(', ', collect_set(city)) as cities
from cancelled_flights
group by uniquecarrier
having count(1) > 1
order by count desc;