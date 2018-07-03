#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit;
fi	

AS_OF_DATE=$1
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"

      exit;
fi

ls FHLMONLA.ZIP

if [[ $? -ne 0 ]]
then
	echo copy source file FHLMONLA.ZIP, FHLMONLF.ZIP, FHLMONLA.SIG, FHLMONLF.SIG
	ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP
	if [[ $? -ne 0 ]]
	then 
  		exit
	else	
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP .
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP .
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLMONLA.SIG .
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLMONLF.SIG .
	fi
fi

ls FHLMONLA.ZIP

if [[ $? -ne 0 ]]
then
echo 'There is no monthly files under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
exit 10

fi

ls FHLMONLA.TXT
if [[ $? -ne 0 ]]
then
	unzip FHLMONLA.ZIP
fi

ls FHLMONLF.TXT
if [[ $? -ne 0 ]]
then
	unzip FHLMONLF.ZIP
fi

FILESIZE1=$(stat -c%s FHLMONLA.TXT)
FILESIZE2=$(stat -c%s FHLMONLF.TXT)
let FILEZIZE=$FILESIZE1+FILESIZE2

hadoop fs -rm -r /user/oozie/source/output_loan
hadoop fs -rm -r /user/oozie/source/output_arm_loan
hadoop fs -rm -r /user/oozie/source/output_mod_loan

filename="FHLMONLA.TXT"
echo 'Process $filename'
FILELINES=$(wc -l $filename | awk  '{print $1;}') 
dos2unix $filename

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

	day=$(echo $sigName|cut -f 1 -d ' ')
	time=$(echo $sigName|cut -f 2 -d ' ')
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
	echo "$prefix\t\t$asOfDate"
	echo "effectiveDate: $effectiveDate"	
fi

timeSec=$(date -d "$effectiveDate" +%s)
echo $timeSec
echo "remove header if it has"
firstWord=$(head -n  1 $filename|cut -f 1 -d '|')
if [ "$firstWord" == "Loan Identifier" ]
then
   sed -i 1d $filename
fi

newFileName=$(echo $filename|cut -f 1 -d '.')
newFileName+="_TS.dat"

if [ -f $newFileName ]
then
        echo 'rm -rf $newFileName'
        rm -rf $newFileName
fi

sed  "s/$/\|$timeSec\|$asOfDate/g" $filename >>$newFileName

filename2="FHLMONLF.TXT"
echo 'Process $filename2'

FILELINES2=$(wc -l $filename2 | awk  '{print $1;}') 
dos2unix $filename2

echo "remove header if it has"
firstWord=$(head -n  1 $filename2|cut -f 1 -d '|')
if [ "$firstWord" == "Loan Identifier" ]
then
   sed -i 1d $filename2
fi

newFileName2=$(echo $filename2|cut -f 1 -d '.')
newFileName2+="_TS.dat"

if [ -f $newFileName2 ]
then
        echo 'rm -rf $newFileName2'
        rm -rf $newFileName2
fi

sed  "s/$/\|$timeSec\|$day/g" $filename2 >>$newFileName2

newFileName3="FHLMONLAF_TS.dat"
if [ -f $newFileName3 ]
then
        echo 'rm -rf $newFileName3'
        rm -rf $newFileName3
fi

cat $newFileName $newFileName2 > $newFileName3

libfile=/usr/book/repository/com/yieldbook/HBaseJava/1.0-SNAPSHOT/HBaseJava-1.0-SNAPSHOT-shaded.jar
#sed -i 's/ *| */|/g' $filename

echo "Staring ETL fhlmc_loan:"
hadoop fs -rm /user/oozie/source/$newFileName3
hadoop fs -copyFromLocal $newFileName3 /user/oozie/source/

inputPath='/user/oozie/source/'
inputPath+=$newFileName3
outputPath='/user/oozie/source/output_loan'
table='fhlmc_loan_monthly'

hadoop jar $libfile com.yieldbook.mortgage.hbase.bulkimport.FhlmcLoanDriver $inputPath $outputPath $table

echo "String ETL fhlmc_mod_loan:"

outputPath='/user/oozie/source/output_mod_loan'
table='fhlmc_mod_loan_monthly'

hadoop jar $libfile com.yieldbook.mortgage.hbase.bulkimport.FhlmcModLoanDriver $inputPath $outputPath $table

echo "String ETL fhlmc_arm_loan:"

outputPath='/user/oozie/source/output_arm_loan'
table='fhlmc_arm_loan_monthly'

hadoop jar $libfile com.yieldbook.mortgage.hbase.bulkimport.FhlmcArmLoanDriver $inputPath $outputPath $table

echo "Cean up HDFS input file:"
hadoop fs -rm $inputPath

java -Xms1024m -Xmx2048m -cp $libfile com.yieldbook.mortgage.hbase.process.MonthlyProcess -t fhlmc -i $newFileName3

echo "kite-dataset csv-import"
kite-dataset csv-import fhlmc_loan_monthly.csv fhlmc_loan_monthly --delimiter '|' --no-header
kite-dataset csv-import fhlmc_arm_loan_monthly.csv fhlmc_arm_loan_daily --delimiter '|' --no-header
kite-dataset csv-import fhlmc_mod_loan_monthly.csv fhlmc_mod_loan_daily --delimiter '|' --no-header

echo "Clean up local files:"
#rm -rf *.TXT *.dat

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "The ETL process START=$START	DIFF=$DIFF FILEZIZE=$FILEZIZE"

