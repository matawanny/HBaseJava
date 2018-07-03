kite-dataset delete dataset:hive://ybrdev79:9083/fhlmc_loan_daily
kite-dataset partition-config --schema fhlmc_loan_daily.avsc as_of_date:copy -o fhlmc_loan_daily.json
kite-dataset create dataset:hive://ybrdev79:9083/fhlmc_loan_daily --schema fhlmc_loan_daily.avsc --partition-by fhlmc_loan_daily.json --format parquet
kite-dataset csv-import fhlmc_loan.csv fhlmc_loan_daily --delimiter '|'

kite-dataset delete dataset:hive://ybrdev79:9083/fhlmc_arm_loan_daily
kite-dataset partition-config --schema fhlmc_arm_loan_daily.avsc as_of_date:copy -o fhlmc_arm_loan_daily.json
kite-dataset create dataset:hive://ybrdev79:9083/fhlmc_arm_loan_daily --schema fhlmc_arm_loan_daily.avsc --partition-by fhlmc_arm_loan_daily.json --format parquet
kite-dataset csv-import fhlmc_arm_loan.csv fhlmc_arm_loan_daily --delimiter '|'

kite-dataset delete dataset:hive://ybrdev79:9083/fhlmc_mod_loan_daily
kite-dataset partition-config --schema fhlmc_mod_loan_daily.avsc as_of_date:copy -o fhlmc_mod_loan_daily.json
kite-dataset create dataset:hive://ybrdev79:9083/fhlmc_mod_loan_daily --schema fhlmc_mod_loan_daily.avsc --partition-by fhlmc_mod_loan_daily.json --format parquet
kite-dataset csv-import fhlmc_mod_loan.csv fhlmc_mod_loan_daily --delimiter '|'
