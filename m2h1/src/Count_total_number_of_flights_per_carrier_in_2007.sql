set hive.tez.java.opts=-Xmx1024m;
set hive.execution.engine=${engine};
SELECT  uniquecarrier, count(1) AS number_of_flights from m2h1.flight2007_orc 
GROUP BY uniquecarrier 
ORDER BY number_of_flights
DESC;