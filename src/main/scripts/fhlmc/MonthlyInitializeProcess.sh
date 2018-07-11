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

cd /usr/book/embs/fhlmc
ls $AS_OF_DATE
if [[ $? -ne 0 ]]
then
  echo "Create a folder $AS_OF_DATE"
  mkdir $AS_OF_DATE
fi
cd $AS_OF_DATE

ls FHLMONLA.TXT
if [[ $? -ne 0 ]]
then
    ls FHLMONLA.ZIP
    if [[ $? -ne 0 ]]
    then
		echo copy source file FHLMONLA.ZIP, FHLMONLA.SIG
		ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP
		if [[ $? -ne 0 ]]
		then 
			echo 'There is no monthly files FHLMONLA.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit 10
		else	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP .
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLMONLA.SIG .

		fi
	fi
	unzip FHLMONLA.ZIP
fi

ls FHLMONLF.TXT
if [[ $? -ne 0 ]]
then
    ls FHLMONLF.ZIP
    if [[ $? -ne 0 ]]
    then
		echo copy source file FHLMONLF.ZIP, FHLMONLF.SIG
		ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP
		if [[ $? -ne 0 ]]
		then 
			echo 'There is no monthly files FHLMONLF.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit 10
		else	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP .
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Signal/FHLMONLF.SIG .
		fi
	fi
	unzip FHLMONLF.ZIP
fi
ls FHLMONLAF_key_sort.txt
if [[ $? -ne 0 ]]
then
	awk -F'|' '{print $1}' FHLMONLA.TXT >FHLMONLA_key.txt
	awk -F'|' '{print $1}' FHLMONLF.TXT >FHLMONLF_key.txt
	cat FHLMONLA_key.txt FHLMONLF_key.txt > FHLMONLAF_key.txt
	sort FHLMONLAF_key.txt > FHLMONLAF_key_sort.txt
fi	

FHLMONLAF_key_num=$(wc -l FHLMONLAF_key_sort.txt | awk  '{print $1;}')
echo "table fhlmc_loan"
let FHLMONLAF_key_1=FHLMONLAF_key_num/5
let FHLMONLAF_key_2=FHLMONLAF_key_num/5*2
let FHLMONLAF_key_3=FHLMONLAF_key_num/5*3
let FHLMONLAF_key_4=FHLMONLAF_key_num/5*4
key1=$(sed -n "${FHLMONLAF_key_1}p"<FHLMONLAF_key_sort.txt)
key2=$(sed -n "${FHLMONLAF_key_2}p"<FHLMONLAF_key_sort.txt)
key3=$(sed -n "${FHLMONLAF_key_3}p"<FHLMONLAF_key_sort.txt)
key4=$(sed -n "${FHLMONLAF_key_4}p"<FHLMONLAF_key_sort.txt)
key5=$(sed -n "${FHLMONLAF_key_num}p"<FHLMONLAF_key_sort.txt)

libfile=/usr/book/repository/com/yieldbook/HBaseJava/1.0-SNAPSHOT/HBaseJava-1.0-SNAPSHOT-shaded.jar
java -Xms1024m -Xmx2048m -cp $libfile com.yieldbook.mortgage.hbase.admin.FhlmcLoanInitialize $key1 $key2 $key3 $key4 $key5

echo 'table fhlmc_loan_monthly, table fhlmc_arm_loan_monthly, table fhlmc_mod_loan_monthly has been created.'

