SELECT 
tract_business_party.tract_business_party_identifier AS TR_BUS_PTY_ID
,tract_business_party.tract_identifier AS TR_ID
,tract_business_party.business_party_identifier AS BUS_PTY_ID
,cast(tract_business_party.creation_date AS timestamp) AS CRE_DT
,cast(tract_business_party.last_change_date AS timestamp) AS LAST_CHG_DT
,tract_business_party.last_change_user_name AS LAST_CHG_USER_NM
,tract_business_party.data_status_code AS DATA_STAT_CD
,cast(tract_business_party.data_inactive_date AS timestamp) AS DATA_IACTV_DT
,tract_business_party.business_party_type_code AS BUS_PTY_TYPE_CD
,crop_acreage_report.program_year AS PGM_YR
,crop_acreage_report.farm_number AS FARM_NBR
,crop_acreage_report.state_fsa_code AS ST_FSA_CD
,crop_acreage_report.county_fsa_code AS CNTY_FSA_CD
,tract.tract_number AS TR_NBR
,business_party.core_customer_identifier AS CORE_CUST_ID

,'' AS HASH_DIFF
,tract_business_party.op AS CDC_OPER_CD
,tract_business_party.dart_filedate AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,current_timestamp AS CDC_DT
FROM `fsa-{env}-cars`.tract_business_party
 LEFT JOIN  `fsa-{env}-cars`.business_party ON (tract_business_party.business_party_identifier = business_party.business_party_identifier)
 JOIN `fsa-{env}-cars`.crop_acreage_report ON (business_party.crop_acreage_report_identifier = crop_acreage_report.crop_acreage_report_identifier)
 LEFT JOIN `fsa-{env}-cars`.tract ON ( tract_business_party.tract_identifier = tract.tract_identifier)
 
