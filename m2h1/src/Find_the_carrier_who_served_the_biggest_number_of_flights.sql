set hive.tez.java.opts=-Xmx1024m;
set hive.execution.engine=${engine};
SELECT carriers_orc.description, count(1) as number_of_flight
FROM m2h1.carriers_orc, m2h1.flight2007_orc
WHERE carriers_orc.code = flight2007_orc.uniquecarrier
GROUP BY carriers_orc.description
ORDER BY number_of_flight
DESC
LIMIT 3;
