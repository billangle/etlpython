SELECT 
limited_payment_program_identifier AS PYMT_PGM_ID
,program_short_name AS PGM_SHRT_NM
,ltrim(rtrim(program_name)) AS PGM_NM
,program_area_identifier AS PGM_AR_ID
,data_status_code AS DATA_STAT_CD
,creation_date AS CRE_DT
,creation_user_name AS CRE_USER_NM
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,op AS CDC_OPER_CD 
,current_timestamp AS load_dt
,'SBSD_STG' AS data_src_nm
,dart_filedate AS cdc_dt
FROM `fsa-{env}-sbsd-cdc`.`limited_payment_program`