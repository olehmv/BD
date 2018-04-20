use m2h3;

set hive.tez.java.opts=-Xmx1024m;

ADD JAR m2h3-assembly-1.jar;
CREATE TEMPORARY FUNCTION useragent AS 'module2.homework3.UserAgentUDF';

select d.cityname,c.osname,c.suma 
from city d,
(select a.cityid, a.osname, a.suma from
 (select i.cityid as cityid, i.osname as osname, sum(i.counter) as suma from 
(select imp.cityid as cityid,useragent(imp.useragent).osname as osname, count(*) as counter from impression imp group by imp.cityid,imp.useragent) i  group by cityid, osname order by suma desc) a
join 
(select i.cityid as cityid, max(i.suma) as max from
(select ii.cityid as cityid, ii.osname as osname, sum(ii.counter) as suma from 
(select imp.cityid as cityid,useragent(imp.useragent).osname as osname, count(*) as counter from impression imp group by imp.cityid,imp.useragent) ii  group by cityid, osname order by suma desc)i group by cityid) b
where a.suma=b.max and a.cityid=b.cityid) c
where d.cityid=c.cityid
group by cityname,osname,suma
order by suma desc;