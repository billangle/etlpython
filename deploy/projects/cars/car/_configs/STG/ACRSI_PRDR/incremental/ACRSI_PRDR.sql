
SELECT 
acrsi_producer.acrsi_producer_identifier AS ACRSI_PRDR_ID
,acrsi_producer.program_year AS PGM_YR
,acrsi_producer.core_customer_identifier AS CORE_CUST_ID
,cast(acrsi_producer.creation_date AS timestamp) AS CRE_DT
,acrsi_producer.creation_user_name AS CRE_USER_NM
,cast(acrsi_producer.last_change_date AS timestamp) AS LAST_CHG_DT
,acrsi_producer.last_change_user_name AS LAST_CHG_USER_NM
,acrsi_producer.data_status_code AS DATA_STAT_CD

,'' AS HASH_DIFF
,acrsi_producer.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,acrsi_producer.dart_filedate AS CDC_DT 
FROM `fsa-{env}-cars-cdc`.`acrsi_producer`

WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

