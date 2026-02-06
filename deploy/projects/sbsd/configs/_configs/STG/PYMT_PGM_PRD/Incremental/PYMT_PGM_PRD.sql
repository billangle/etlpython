SELECT 
limited_payment_program_period_identifier AS PYMT_PGM_PRD_ID
,limited_payment_program_identifier AS PYMT_PGM_ID
,payment_limitation_amount AS PYMT_LMT_AMT
,data_status_code AS DATA_STAT_CD
,creation_date AS CRE_DT
,creation_user_name AS CRE_USER_NM
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,subsidiary_period_start_year AS SBSD_PRD_STRT_YR
,subsidiary_period_end_year AS SBSD_PRD_END_YR
,program_midsize_name AS PGM_MSIZE_NM
,parent_limited_payment_program_identifier AS PRNT_PYMT_PGM_ID
,payment_limitation_indicator AS PYMT_LMT_IND
, op AS CDC_OPER_CD 
FROM `fsa-{env}-sbsd-cdc`.`limited_payment_program_period`