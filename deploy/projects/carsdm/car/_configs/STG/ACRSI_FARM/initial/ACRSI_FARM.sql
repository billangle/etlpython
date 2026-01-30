
SELECT 
acrsi_farm.acrsi_farm_identifier AS ACRSI_FARM_ID
,acrsi_farm.program_year AS PGM_YR
,ltrim(rtrim(acrsi_farm.state_fsa_code)) AS ST_FSA_CD
,ltrim(rtrim(acrsi_farm.county_fsa_code)) AS CNTY_FSA_CD
,ltrim(rtrim(acrsi_farm.farm_number)) AS FARM_NBR
,cast(acrsi_farm.creation_date AS timestamp) AS CRE_DT
,acrsi_farm.creation_user_name AS CRE_USER_NM
,cast(acrsi_farm.last_change_date AS timestamp) AS LAST_CHG_DT
,acrsi_farm.last_change_user_name AS LAST_CHG_USER_NM
,acrsi_farm.data_status_code AS DATA_STAT_CD
,'' AS HASH_DIFF
,acrsi_farm.op AS CDC_OPER_CD
,dart_filedate AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,current_timestamp AS CDC_DT

FROM `fsa-{env}-cars`.acrsi_farm

