SELECT 
creation_date AS CRE_DT
,creation_user_name AS CRE_USER_NM
,data_status_code AS DATA_STAT_CD
,ltrim(rtrim(irs_agi_eligibility_error_category_code)) AS IRS_AGI_ELG_ERR_CAT_CD
,irs_agi_eligibility_error_type_description AS IRS_AGI_ELG_ERR_TYPE_DESC
,irs_agi_eligibility_error_type_identifier AS IRS_AGI_ELG_ERR_TYPE_ID
,ltrim(rtrim(irs_agi_eligibility_error_type_number)) AS IRS_AGI_ELG_ERR_TYPE_NBR
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
, op AS CDC_OPER_CD 
FROM `fsa-{env}-sbsd-cdc`.`irs_agi_eligibility_error_type`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'