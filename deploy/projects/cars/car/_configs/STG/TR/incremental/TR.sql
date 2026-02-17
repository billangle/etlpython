SELECT 
tract.tract_identifier AS TR_ID
,tract.crop_acreage_report_identifier AS CROP_ACRG_RPT_ID
,tract.tract_number AS TR_NBR
,cast(tract.creation_date AS timestamp) AS CRE_DT
,cast(tract.last_change_date AS timestamp) AS LAST_CHG_DT
,tract.last_change_user_name AS LAST_CHG_USER_NM
,tract.data_status_code AS DATA_STAT_CD
,tract.farmland_acreage AS FMLD_ACRG
,tract.cropland_acreage AS CPLD_ACRG
,tract.tract_description AS TR_DESC
,ltrim(rtrim(tract.location_state_fsa_code)) AS LOC_ST_FSA_CD
,ltrim(rtrim(tract.location_county_fsa_code)) AS LOC_CNTY_FSA_CD
,crop_acreage_report.program_year AS PGM_YR
,crop_acreage_report.farm_number AS FARM_NBR
,ltrim(rtrim(crop_acreage_report.state_fsa_code)) AS ST_FSA_CD
,ltrim(rtrim(crop_acreage_report.county_fsa_code)) AS CNTY_FSA_CD

,'' AS HASH_DIFF
,tract.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,COALESCE(crop_acreage_report.dart_filedate, current_timestamp) AS CDC_DT
FROM `fsa-{env}-cars-cdc`.`tract`
 LEFT JOIN  `fsa-{env}-cars`.`crop_acreage_report` ON (TRACT.crop_acreage_report_identifier = crop_acreage_report.crop_acreage_report_identifier)

WHERE tract.dart_filedate BETWEEN DATE '{ETL_START_TIMESTAMP}' AND DATE '{ETL_END_TIMESTAMP}'




