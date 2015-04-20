REGISTER 'elephant-bird-pig-4.1.jar'; 
REGISTER 'elephant-bird-hadoop-compat-4.1.jar';

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

inputfile = LOAD '*gz' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (mymap);
inputfile = FILTER inputfile BY mymap#'t' IS NOT NULL;

inputfile = FOREACH inputfile GENERATE mymap#'a' AS user_agent: chararray, 
mymap#'c' AS country_code: chararray, 
mymap#'nk' AS known_user: int, 
mymap#'g' AS global_bitly_hash: chararray, 
mymap#'h' AS encoding_user_bitly_hash: chararray, 
mymap#'l' AS encoding_user_login: chararray, 
mymap#'hh' AS short_url_cname: chararray, 
mymap#'r' AS REFERRING_URL: chararray, 
mymap#'u' AS long_url: chararray, 
(long)mymap#'t' AS timestamp: long, 
mymap#'gr' AS geo_region: chararray, 
mymap#'ll' AS latlong: chararray,
 mymap#'cy' AS geo_city_name: chararray, 
mymap#'tz' AS timezone: long, mymap#'hc' AS time_hash: long, mymap#'al' AS accept_language: chararray;

inputfile = FOREACH inputfile GENERATE *, STRSPLIT(latlong, ',', 2).$0 AS latitude:chararray, STRSPLIT(latlong, ',', 2).$1 AS longitude:chararray;
inputfile = FOREACH inputfile GENERATE *, SUBSTRING(latitude, 1, (int)SIZE(latitude)), SUBSTRING(longitude, 0, (int)SIZE(latitude)-1);
inputfile = FOREACH inputfile GENERATE *, ToDate(UnixToISO(timestamp * 1000),'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') as timestamp_dt;
inputfile = FOREACH inputfile GENERATE *, GetMonth(timestamp_dt) AS month,GetYear(timestamp_dt) AS year ;

--Top ten URL’s clicked.

urlgroup = GROUP inputfile BY long_url;
urlcount = FOREACH urlgroup GENERATE group AS url, COUNT(inputfile) AS count; 
urlsort = ORDER urlcount BY count DESC;
top10url = LIMIT urlsort 10;*/
DUMP top10url;


--Top ten URL’s per month.

yearmonthurlgroup = GROUP inputfile BY (month,year,long_url);
urlcount = FOREACH yearmonthurlgroup GENERATE FLATTEN(group) AS (month,year,long_url), COUNT(inputfile) AS count; 
urlcount = ORDER urlcount BY year ASC, month ASC, count DESC;
yearmonthgrp = GROUP urlcount BY (year,month);
result = FOREACH yearmonthgrp {
	top10 = ORDER urlcount BY count DESC;
	top10 = LIMIT urlcount 10;
	GENERATE FLATTEN(top10);
} 

DUMP result;


--Top ten URL's per city.

citygroup = GROUP inputfile BY geo_city_name;
urlcount = FOREACH citygroup GENERATE group AS city, COUNT(inputfile) AS count; 
urlsort = ORDER urlcount BY count DESC;
top10url = LIMIT urlsort 10;
DUMP top10url;

