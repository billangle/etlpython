SELECT 
field.field_identifier AS FLD_ID
,cast(field.creation_date AS timestamp) AS CRE_DT
,field.creation_user_name AS CRE_USER_NM
,cast(field.last_change_date AS timestamp) AS LAST_CHG_DT
,field.last_change_user_name AS LAST_CHG_USER_NM
,field.data_status_code AS DATA_STAT_CD
,cast(field.data_inactive_date AS timestamp) AS DATA_IACTV_DT
,field.tract_identifier AS TR_ID
,ltrim(rtrim(field.field_number)) AS FLD_NBR
,field.clu_identifier AS CLU_ID
,field.clu_acreage AS CLU_ACRG
,field.location_state_fsa_code AS LOC_ST_FSA_CD
,field.location_county_fsa_code AS LOC_CNTY_FSA_CD
,field.state_ansi_code AS ST_ANSI_CD
,field.county_ansi_code AS CNTY_ANSI_CD
,crop_acreage_report.program_year AS PGM_YR
,ltrim(rtrim(crop_acreage_report.farm_number)) AS FARM_NBR
,tract.tract_number AS TR_NBR
,ltrim(rtrim(crop_acreage_report.state_fsa_code)) AS ST_FSA_CD
,ltrim(rtrim(crop_acreage_report.county_fsa_code)) AS CNTY_FSA_CD
,'' AS HASH_DIFF
,field.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,field.dart_filedate AS CDC_DT
,field.cropland_indicator AS CPLD_IND
,cast(field.native_sod_conversion_date AS timestamp) AS NTV_SOD_CVSN_DT
FROM `fsa-{env}-cars-cdc`.`field`
 LEFT JOIN  `fsa-{env}-cars`.`TRACT` ON (FIELD.tract_identifier = TRACT.tract_identifier)
 JOIN `fsa-{env}-cars`.`crop_acreage_report` ON (TRACT.crop_acreage_report_identifier = crop_acreage_report.crop_acreage_report_identifier)

WHERE field.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'


