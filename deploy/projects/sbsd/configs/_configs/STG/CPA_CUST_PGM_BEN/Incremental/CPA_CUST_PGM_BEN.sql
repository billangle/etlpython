SELECT 
combined_producer_account_identifier AS CMB_PRDR_ACCT_ID
,member_allocation_percentage AS CPA_CUST_ALOC_PCT
,combined_account_member_allocation_identifier AS CPA_CUST_PGM_BEN_ID
,data_status_code AS DATA_STAT_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,limited_payment_program_period_identifier AS PYMT_PGM_PRD_ID
,subsidiary_customer_identifier AS SBSD_CUST_ID
, op AS CDC_OPER_CD
FROM (
SELECT combined_producer_account_identifier
,member_allocation_percentage
,combined_account_member_allocation_identifier
,data_status_code
,last_change_date
,last_change_user_name
,limited_payment_program_period_identifier
,subsidiary_customer_identifier
, op, ROW_NUMBER() OVER (PARTITION BY combined_account_member_allocation_identifier
ORDER BY last_change_date DESC) as rnum
            FROM `fsa-{env}-sbsd-cdc`.`combined_account_member_allocation`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') as SubQry
WHERE SubQry.rnum=1