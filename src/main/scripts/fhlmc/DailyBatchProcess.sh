#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit;
fi	

AS_OF_DATE=$1
UPDATE_HBASE_ONLY=$2
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"

      exit;
fi

cd /usr/book/embs/fhlmc
ls $AS_OF_DATE
if [[ $? -ne 0 ]]
then
  echo "Create a folder $AS_OF_DATE"
  mkdir $AS_OF_DATE
fi
cd $AS_OF_DATE

ls FHLDLYLF.TXT
if [[ $? -ne 0 ]]
then
	ls FHLDLYLF.ZIP
	if [[ $? -ne 0 ]]
	then
		echo copy source file FHLDLYLF.ZIP, FHLDLYLF.SIG
		ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLF.ZIP
		if [[ $? -ne 0 ]]
		then
			echo 'There is no daily files FHLDLYLF.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit
		else  
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLF.ZIP .
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLDLYLF.SIG .
		fi
	fi
	unzip FHLDLYLF.ZIP
fi

ls FHLDLYLA.TXT
if [[ $? -ne 0 ]]
then
	ls FHLDLYA.ZIP
	if [[ $? -ne 0 ]]
	then
		echo copy source file FHLDLYLA.ZIP, FHLDLYLA.SIG
		ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLA.ZIP
		if [[ $? -ne 0 ]]
		then
			echo 'There is no daily FHLDLYA.ZIP files under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit
		else  
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLA.ZIP .
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLDLYLA.SIG .
		fi
	fi
	unzip FHLDLYLA.ZIP
fi

filename="FHLDLYLF.TXT"
echo "Process $filename"

echo "remove header if it has"
firstWord=$(head -n  1 $filename|cut -f 1 -d '|')
if [ "$firstWord" == "Loan Identifier" ]
then
   sed -i 1d $filename
fi

FILELINES1=$(wc -l $filename | awk  '{print $1;}') 

name=$(echo $filename|cut -f 1 -d '.'|cut -f 2 -d '_')
prefix=$(echo $filename|cut -f 1 -d '.'|cut -f 1 -d '_')

if [ "$name" == "$prefix" ]
then
	sigName=$(echo $filename|cut -f 1 -d '.')
	sigName+=".SIG"
	if [ ! -f $sigName ]
        then
		echo "missing SIG file $sigName" 
		exit;
	fi

	day=$(cat $sigName|cut -f 1 -d ' ')
	time=$(cat $sigName|cut -f 2 -d ' ')
	asOfDate=$day
	effectiveDate=$(echo ${day:0:6})
	effectiveDate+="01 00:00:00"
	echo "$day $time $asOfDate"
	echo "effectiveDate: $effectiveDate"	
else
	asOfDate=$(($name+5))
	day=$asOfDate
	effectiveDate=echo ${day:0:6}
    effectiveDate+="01 00:00:00"	
	asOfDate+=" 00:00:00"
	echo "$prefix\t\t$asOfDate"
	echo "effectiveDate: $effectiveDate"
fi

timeSec=$(date -d "$effectiveDate" +%s)
echo $timeSec
AS_OF_DATE=$timeSec

dos2unix $filename

newFileName=$(echo $filename|cut -f 1 -d '.')
newFileName+="_TS.dat"

if [ -f $newFileName ]
then
        echo 'rm -rf $newFileName'
        rm -rf $newFileName
fi

sed  "s/$/\|$timeSec\|$day/g" $filename >>$newFileName

FILESIZE1=$(stat -c%s FHLDLYLF.TXT)
FILESIZE2=$(stat -c%s FHLDLYLA.TXT)

if [ -f $newFileName3 ]
then
        echo 'rm -rf $newFileName3'
        rm -rf $newFileName3
fi

filename2="FHLDLYLA.TXT"
FILELINES2=$(wc -l $filename2 | awk  '{print $1;}') 

if [[ $FILELINES2 -ne 1 ]]
then
	echo 'Process $filename2'
	
	echo "remove header if it has"
	firstWord=$(head -n  1 $filename2|cut -f 1 -d '|')
	if [ "$firstWord" == "Loan Identifier" ]
	then
   		sed -i 1d $filename2
	fi
	
	let FILEZIZE=$FILESIZE1+$FILESIZE2

	dos2unix $filename2

	newFileName2=$(echo $filename2|cut -f 1 -d '.')
	newFileName2+="_TS.dat"
	if [ -f $newFileName2 ]
	then
	        echo 'rm -rf $newFileName2'
	        rm -rf $newFileName2
	fi
	sed  "s/$/\|$timeSec\|$day/g" $filename2 >>$newFileName2
	newFileName3="FHLDLYLFA_TS.dat"
	cat $newFileName $newFileName2 > $newFileName3

else
	let FILEZIZE=$FILESIZE1
	newFileName3=$newFileName
fi

FILELINES=$(wc -l $newFileName3 | awk  '{print $1;}') 

rm -rf fhlmc_loan_daily.csv fhlmc_arm_loan_daily.csv fhlmc_mod_loan_daily.csv

libfile=/usr/book/repository/com/yieldbook/HBaseJava/1.0-SNAPSHOT/HBaseJava-1.0-SNAPSHOT-shaded.jar
java -Xms1024m -Xmx2048m -cp $libfile com.yieldbook.mortgage.hbase.process.DailyProcess -t fhlmc -i $newFileName3

if [ -z "$UPDATE_HBASE_ONLY" ]
then
	echo "kite-dataset csv-import"
	hadoop fs -ls /user/hive/warehouse/fhlmc_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/fhlmc_loan_daily/as_of_date_copy=$day
	fi
	kite-dataset csv-import fhlmc_loan_daily.csv fhlmc_loan_daily --delimiter '|' --no-header
	
	hadoop fs -ls /user/hive/warehouse/fhlmc_arm_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/fhlmc_arm_loan_daily/as_of_date_copy=$day
	fi
	
	kite-dataset csv-import fhlmc_arm_loan_daily.csv fhlmc_arm_loan_daily --delimiter '|' --no-header
	
	hadoop fs -ls /user/hive/warehouse/fhlmc_mod_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/fhlmc_mod_loan_daily/as_of_date_copy=$day
	fi
	kite-dataset csv-import fhlmc_mod_loan_daily.csv fhlmc_mod_loan_daily --delimiter '|' --no-header
fi

echo "Clean up local files:"
rm -rf FHLDLYL*.TXT FHLDLYL*.dat *_daily.csv
rm -rf FHLDLYL?.ZIP FHLDLYL?.SIG

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "The Daily ETL process START=$START PROCESS_TIME=$DIFF(s) FILEZIZE=$FILEZIZE FILELINES=$FILELINES"
