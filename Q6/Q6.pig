--6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.

--REGISTER /home/hduser/DatasetsandCodes/PIG/piggybank.jar;
--DEFINE pigloader org.apache.pig.piggybank.storage.CSVExcelStorage();
h1b = LOAD '/home/hduser/h1bvisa/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:long,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);
--DESCRIBE h1b;
status = FOREACH h1b GENERATE year,case_status;
--DESCRIBE status;
grouped = GROUP status BY year;
----DESCRIBE grouped;
newgroup = GROUP status by (year,case_status);
--DESCRIBE newgroup;
total = FOREACH grouped GENERATE group AS year, COUNT(status.case_status) AS totalApplication;
--DESCRIBE total;
casestatuscount = FOREACH newgroup GENERATE group,group.year,COUNT(status) AS caseTotal;
--DESCRIBE casestatuscount;
joined = JOIN casestatuscount BY $1, total BY $0;
--DESCRIBE joined;
final = FOREACH joined GENERATE FLATTEN($0), (float)($2*100)/$4 AS percentage, $2 AS caseStatusCount;
--DUMP final;
STORE final INTO '/home/hduser/out/Q6';





