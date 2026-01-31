SELECT 
program_year_irs_business_party_2014_identifier AS PGM_YR_IRS_BUS_PTY_2014_ID
,last_change_date AS LAST_CHG_DT
,creation_date AS CRE_DT
,last_change_user_name AS LAST_CHG_USER_NM
,creation_user_name AS CRE_USER_NM
,data_status_code AS DATA_STAT_CD
,data_received_date AS DATA_RCV_DT
,agi_consent_form_irs_input_date AS AGI_CNST_FORM_IRS_IPUT_DT
,batch_processing_status_effective_date AS BAT_PROC_STAT_EFF_DT
,batch_processing_status_code AS BAT_PROC_STAT_CD
,program_year AS PGM_YR
,ltrim(rtrim(core_customer_identifier)) AS CORE_CUST_ID
,customer_data_irs_validation_code AS CUST_DATA_IRS_VLD_CD
,irs_900k_agi_eligibility_determination_code AS IRS_900K_AGI_ELG_DTER_CD
,irs_agi_eligibility_error_type_identifier AS IRS_AGI_ELG_ERR_TYPE_ID
, op AS CDC_OPER_CD FROM `fsa-{env}-sbsd-cdc`.`program_year_irs_business_party_2014`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'