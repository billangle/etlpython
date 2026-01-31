SELECT 
program_record_reference_type_identifier AS PGM_RCD_REF_TYPE_ID
,creation_date AS CRE_DT
,creation_user_name AS CRE_USER_NM
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,data_status_code AS DATA_STAT_CD
,ltrim(rtrim(accounting_program_code)) AS ACCT_PGM_CD
,limited_payment_program_identifier AS PYMT_PGM_ID
,record_reference_identification_type_code AS RCD_REF_ID_TYPE_CD
, op AS CDC_OPER_CD FROM `fsa-{env}-sbsd-cdc`.`program_record_reference_type`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'