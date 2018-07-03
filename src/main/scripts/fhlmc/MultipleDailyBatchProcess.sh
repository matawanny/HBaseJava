#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit;
fi

start=$1
if [ -z "$start" ]
then
      echo "Must input start date!"
      exit;
fi

end=$2
if [ -z "$end" ]
then
      echo "Must input end date!"
      exit;
fi
d=$start
afterend=$(date -I -d "$end + 1 day")
while [ "$d" != "$afterend" ] ;
do
        day=$(date -d "$d" +%a)
        if [[ "$day" != "Sat" && "$day" != "Sun" ]]
        then
          year=$(echo $d|cut -d '-' -f 1)
          month=$(echo $d|cut -d '-' -f 2)
          dayofmonth=$(echo $d|cut -d '-' -f 3)
          as_of_date=$year
          as_of_date+=$month
          as_of_date+=$dayofmonth
          echo $as_of_date
          rm -rf $as_of_date
          cd /usr/book/embs
          mkdir $as_of_date
          cd $as_of_date
          echo "sh -x /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/DailyBatchProcess.sh $as_of_date"
          sh -x /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/DailyBatchProcess.sh $as_of_date
          cd /usr/book/export/fhlmc
          mkdir $as_of_date
          echo "sh -x /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date"
          sh -x /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date
        fi
        d=$(date -I -d "$d + 1 day")
done
