SELECT 
program_year.program_year AS PGM_YR
,cast(program_year.effective_period_start_date AS timestamp) AS EFF_PRD_STRT_DT
,cast(program_year.effective_period_end_date AS timestamp) AS EFF_PRD_END_DT
,cast(program_year.creation_date AS timestamp) AS CRE_DT
,cast(program_year.last_change_date AS timestamp) AS LAST_CHG_DT
,program_year.last_change_user_name AS LAST_CHG_USER_NM
,program_year.data_status_code AS DATA_STAT_CD
,'' AS HASH_DIFF
,program_year.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,program_year.dart_filedate AS CDC_DT
,program_year.DEFAULT_PROGRAM_YEAR_INDICATOR AS DFLT_PGM_YR_IND
FROM `fsa-{env}-cars-cdc`.`program_year`

WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'



