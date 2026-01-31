SELECT 
subsidiary_period_state_identifier AS SBSD_PRD_ST_ID
,subsidiary_period_identifier AS SBSD_PRD_ID
,state_fsa_code AS ST_FSA_CD
,school_payment_limitation_indicator AS SCHL_PYMT_LMT_IND
,data_status_code AS DATA_STAT_CD
,creation_date AS CRE_DT
,creation_user_name AS CRE_USER_NM
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,op AS CDC_OPER_CD 
FROM `fsa-{env}-sbsd-cdc`.`subsidiary_period_state`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'