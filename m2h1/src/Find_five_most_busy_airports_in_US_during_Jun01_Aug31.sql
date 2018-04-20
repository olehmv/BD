set hive.tez.java.opts=-Xmx1024m;
set hive.execution.engine=${engine};
SELECT busy.airport, sum(busy.number_of_flight) as number_of_flight
FROM
(SELECT airports_orc.airport as airport, count(1) as number_of_flight
FROM m2h1.flight2007_orc, m2h1.airports_orc
WHERE flight2007_orc.dest = airports_orc.iata
AND flight2007_orc.month between 6 AND 8
GROUP BY airports_orc.airport
UNION
SELECT airports_orc.airport as airport , count(1) as number_of_flight
FROM m2h1.flight2007_orc, m2h1.airports_orc
WHERE flight2007_orc.origin = airports_orc.iata
AND flight2007_orc.month between 6 AND 8
GROUP BY airports_orc.airport
) busy
GROUP BY airport
ORDER BY number_of_flight
DESC
LIMIT 5;
