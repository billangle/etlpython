
SELECT 
crop_type.crop_type_identifier AS CROP_TYPE_ID
,ltrim(rtrim(crop_type.fsa_crop_code)) AS FSA_CROP_CD
,ltrim(rtrim(crop_type.fsa_crop_type_code)) AS FSA_CROP_TYPE_CD
,crop_type.program_year AS PGM_YR
,crop_type.cars_crop_classification_code AS CARS_CROP_CLS_CD
,cast(crop_type.creation_date AS timestamp) AS CRE_DT
,cast(crop_type.last_change_date AS timestamp) AS LAST_CHG_DT
,crop_type.last_change_user_name AS LAST_CHG_USER_NM
,crop_type.data_status_code AS DATA_STAT_CD
,ltrim(rtrim(crop_type.crop_intended_use_code)) AS CROP_INTN_USE_CD
,cast(crop_type.data_inactive_date AS timestamp) AS DATA_IACTV_DT

,'' AS HASH_DIFF
,crop_type.op AS CDC_OPER_CD
,dart_filedate AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,current_timestamp AS CDC_DT
FROM `fsa-{env}-cars`.crop_type



