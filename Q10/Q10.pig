
--10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?


--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1bvisa/h1b_final' USING PigStorage() AS (s_no,case_status:chararray,employer_name:chararray,soc_name,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


finalh1b = FOREACH h1b GENERATE case_status,job_title;

allgrouped = GROUP finalh1b BY job_title;

allcount = FOREACH allgrouped GENERATE group as job_title,COUNT(finalh1b.case_status) as totalapplicaion;



filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

successgrouped = GROUP filterh1b BY job_title;

successcount = FOREACH successgrouped GENERATE group AS job_title,COUNT(filterh1b.case_status) as totalsuccess;



joined = JOIN allcount BY $0,successcount BY $0;

finalbag = FOREACH joined GENERATE $0,$1 as petitions, (($3*100)/$1) AS successrate;
											
filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;

finaloutput = ORDER filtersuccessrate BY $2 DESC;



--STORE finaloutput INTO '/home/hduser/out/Q10';
DUMP finaloutput;




