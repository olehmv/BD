set hive.tez.java.opts=-Xmx1024m;
set hive.execution.engine=${engine};
SELECT count(*) as number_of_flights_served_in_Jun_by_NYC
FROM m2h1.flight2007_orc JOIN m2h1.airports_orc
WHERE (flight2007_orc.dest = airports_orc.iata OR flight2007_orc.origin = airports_orc.iata)
AND airports_orc.city = 'New York'
AND flight2007_orc.month = 6;