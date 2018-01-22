#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B VISA APPLICATIONS**********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} Find top 5 job titles who are having highest avg growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} Which part of the US has the most Data Engineer jobs for each year?  ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} find top 5 locations in the US who have got certified visa for each year.[certified] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} Which top 5 employers file the most petitions each year? - Case Status - ALL ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year? For All Applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year? Only for Certified Applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} Create a bar graph to depict the number of applications for each year [All] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14)${MENU} Export result for question no 10 to MySql database. ${NORMAL}"
    echo -e "${MENU}************************************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT} enter to exit. ${NORMAL}"
    read opt
}

function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
start-all.sh
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
        hadoop fs -rm -r /h1bproject/Q11
        hadoop jar /home/hduser/h1b.jar dataengg.DataEngineer /user/hive/warehouse/h1b.db/h1b_final /h1bproject/Q11
        hadoop fs -cat /h1bproject/Q11/p*
	sleep 5
        show_menu;

        ;;


        2) clear;
        option_picked "1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]";
        
        pig -x local '/home/hduser/h1b project/Q1/1b.pig'
        sleep 5
        show_menu;
   
        ;;

        
       3) clear;
       option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
       
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read var
       echo  "part of the US has the most Data Engineer jobs for ${var}"
       hive -e "SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b.h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year=${var} GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;";
        sleep 5
       
       show_menu;

       ;;


       4) clear;
       option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.[certified]";
       
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read var
       echo  "Top 5 locations in the US who have got certified visa for ${var}"
       hive -e "select worksite,count(case_status) as total,year from h1b.h1b_final WHERE year=${var} and case_status='CERTIFIED' group by worksite,year order by total desc limit 5;";
       sleep 5
       show_menu;

       ;;
       

       5) clear; 
       option_picked "3) Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]";
       
        hadoop fs -rm -r /h1bproject/Q3
        hadoop jar /home/hduser/h1b.jar datascientist.DataScientist /user/hive/warehouse/h1b.db/h1b_final /h1bproject/Q3
        hadoop fs -cat /h1bproject/Q3/p*
        sleep 5
        show_menu;
       
       ;;


       6) clear;
       option_picked "4) Which top 5 employers file the most petitions each year? - Case Status - ALL";
       
       hadoop fs -rm -r /h1bproject/Q4
       hadoop jar /home/hduser/h1b.jar top5.Top5Employee /user/hive/warehouse/h1b.db/h1b_final /h1bproject/Q4
       hadoop fs -cat /h1bproject/Q4/p*
       sleep 5
       show_menu;

       ;;


       7) clear;
       option_picked "5 a) Find the most popular top 10 job positions for H1B visa applications for each year? For all the applications.";
       
       hadoop fs -rm -r /h1bproject/Q5a1
       hadoop jar /home/hduser/h1b.jar top10.Top10JobPositions /user/hive/warehouse/h1b.db/h1b_final /h1bproject/Q5a1
       hadoop fs -cat /h1bproject/Q5a1/p*
       sleep 5
       show_menu;

       ;;
     

       8) clear;
       option_picked "5 b) Find the most popular top 10 job positions for H1B visa applications for each year? For only certified applications.";
       
       hadoop fs -rm -r /h1bproject/Q5b1
       hadoop jar /home/hduser/h1b.jar top10.Top10CertifiedJobPositions /user/hive/warehouse/h1b.db/h1b_final /h1bproject/Q5b1
       hadoop fs -cat /h1bproject/Q5b1/p*
       sleep 5
       show_menu;

       ;;


       9) clear;
       option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.";
       
        pig -x local '/home/hduser/h1b project/Q6/Q6.pig'
        sleep 5
       show_menu;

       ;;


       10) clear;
       option_picked "7) Create a bar graph to depict the number of applications for each year. [All]";
       
       hive -e "select year,count(*) from h1b.h1b_final group by year;";
       sleep 5
       show_menu;

       ;;


       11) clear;
       option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]";
       
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read year
       echo -e "Enter the choice Full time / Part time. (Y/N)"
       read var
       echo  "Average Prevailing Wage for each Job for year ${year} and full time position ${var}"
       hive -e "select year,job_title ,avg(prevailing_wage) as total, full_time_position from h1b.h1b_final where year=$year and full_time_position ='$var' group by year,job_title,full_time_position order by total desc limit 10;";
       sleep 5 
       show_menu;

       ;;
   
       12) clear;
       option_picked "9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?";
       
       pig -x local '/home/hduser/h1b project/Q9/Q9.pig' 
       sleep 5 
       show_menu;

       ;;

  
       13) clear;
       option_picked "10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in   petitions (total petitions filed 1000 OR more than 1000)?";
       rm -r /home/hduser/out/Q10
       pig -x local '/home/hduser/h1b project/Q10/Q10.pig'
       sleep 5
       show_menu;

       ;;

       
       14) clear;
       option_picked "11) Export result for question no 10 to MySql database."
	mysql -u root -p -e 'DROP DATABASE retail;CREATE DATABASE IF NOT EXISTS retail;USE retail;CREATE TABLE h1b(job_title VARCHAR(255) NOT NULL,petitions INT NOT NULL,success_rate FLOAT NOT NULL);';
	sqoop export --connect jdbc:mysql://localhost/retail --username root --password soulmate --table h1b --update-mode  allowinsert --export-dir /niit/part-r-00000 --input-fields-terminated-by '\t' -m 1 ;
	echo -e '\n\n Display content from MySQL Database.\n\n'
	echo -e '\n 10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?\n\n'
	mysql -u root -p -e "use retail; select * from h1b;"

       
      # bash /home/hduser/h1b project/Q11/Q11.sh
       sleep 5
       show_menu;
      
       ;;



\n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi

done

