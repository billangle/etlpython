
SELECT 
business_party.business_party_identifier AS BUS_PTY_ID
,business_party.core_customer_identifier AS CORE_CUST_ID
,cast(business_party.creation_date AS timestamp) AS CRE_DT
,cast(business_party.last_change_date AS timestamp) AS LAST_CHG_DT
,business_party.last_change_user_name AS LAST_CHG_USER_NM
,business_party.data_status_code AS DATA_STAT_CD
,business_party.crop_acreage_report_identifier AS CROP_ACRG_RPT_ID
,business_party.business_party_type_code AS BUS_PTY_TYPE_CD
,crop_acreage_report.program_year AS PGM_YR
,crop_acreage_report.farm_number AS FARM_NBR
,crop_acreage_report.state_fsa_code AS ST_FSA_CD
,crop_acreage_report.county_fsa_code AS CNTY_FSA_CD

,'' AS HASH_DIFF
,business_party.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,business_party.dart_filedate AS CDC_DT
FROM `fsa-{env}-cars-cdc`.`business_party`
 LEFT JOIN  `fsa-{env}-cars`.`crop_acreage_report` ON (business_party.crop_acreage_report_identifier=crop_acreage_report.crop_acreage_report_identifier)
 
WHERE business_party.dart_filedate BETWEEN DATE '{ETL_START_TIMESTAMP}' AND DATE '{ETL_END_TIMESTAMP}'

