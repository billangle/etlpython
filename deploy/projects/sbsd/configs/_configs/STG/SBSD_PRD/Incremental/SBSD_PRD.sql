SELECT 
subsidiary_period_identifier AS SBSD_PRD_ID
,subsidiary_period_start_date AS SBSD_PRD_STRT_DT
,subsidiary_period_end_date AS SBSD_PRD_END_DT
,subsidiary_period_name AS SBSD_PRD_NM
,current_subsidiary_period_indicator AS CUR_SBSD_PRD_IND
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
, op AS CDC_OPER_CD 
FROM `fsa-{env}-sbsd-cdc`.`subsidiary_period`
 WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'