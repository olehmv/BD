use m2h3;

set hive.tez.java.opts=-Xmx1024m;

ADD JAR m2h3-assembly-1.jar;
CREATE TEMPORARY FUNCTION useragent AS 'module2.homework3.UserAgentUDF';

select d.cityname,c.browser,c.suma 
from city d,
(select a.cityid, a.browser, a.suma from
 (select i.cityid as cityid, i.browser as browser, sum(i.counter) as suma from 
(select imp.cityid as cityid,useragent(imp.useragent).browser as browser, count(*) as counter from impression imp group by imp.cityid,imp.useragent) i  group by cityid, browser order by suma desc) a
join 
(select i.cityid as cityid, max(i.suma) as max from
(select ii.cityid as cityid, ii.browser as browser, sum(ii.counter) as suma from 
(select imp.cityid as cityid,useragent(imp.useragent).browser as browser, count(*) as counter from impression imp group by imp.cityid,imp.useragent) ii  group by cityid, browser order by suma desc)i group by cityid) b
where a.suma=b.max and a.cityid=b.cityid) c
where d.cityid=c.cityid
group by cityname,browser,suma
order by suma desc;