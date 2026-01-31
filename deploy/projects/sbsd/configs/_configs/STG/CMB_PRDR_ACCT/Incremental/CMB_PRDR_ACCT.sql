SELECT 
combination_dissolution_reason_code AS CMB_DSLV_RSN_CD
,combination_determination_date AS CMB_DTER_DT
,combination_determination_method_code AS CMB_DTER_MTHD_CD
,combined_producer_account_identifier AS CMB_PRDR_ACCT_ID
,combined_producer_account_number AS CMB_PRDR_ACCT_NBR
,combined_producer_reason_code AS CMB_PRDR_RSN_CD
,fsa_county_identifier AS CNTY_FSA_SVC_CTR_ID
,data_status_code AS DATA_STAT_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,parent_combined_producer_account_identifier AS PRNT_CMB_PRDR_ACCT_ID
,subsidiary_period_identifier AS SBSD_PRD_ID
, op AS CDC_OPER_CD
FROM (
SELECT combination_dissolution_reason_code
,combination_determination_date
,combination_determination_method_code
,combined_producer_account_identifier
,combined_producer_account_number
,combined_producer_reason_code
,fsa_county_identifier
,data_status_code
,last_change_date
,last_change_user_name
,parent_combined_producer_account_identifier
,subsidiary_period_identifier
, op, ROW_NUMBER() OVER (PARTITION BY combined_producer_account_identifier ORDER BY last_change_date DESC) as rnum
            FROM `fsa-{env}-sbsd-cdc`.`combined_producer_account`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') SubQry
WHERE SubQry.rnum=1