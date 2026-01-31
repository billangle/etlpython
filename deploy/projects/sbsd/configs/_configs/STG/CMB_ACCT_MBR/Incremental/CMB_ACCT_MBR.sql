SELECT 
combined_account_join_date AS CMB_ACCT_JOIN_DT
,combined_account_leave_date AS CMB_ACCT_LV_DT
,combined_producer_account_identifier AS CMB_PRDR_ACCT_ID
,combined_account_member_identifier AS CMB_PRDR_ACCT_MBR_ID
,data_status_code AS DATA_STAT_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,subsidiary_customer_identifier AS SBSD_CUST_ID
, op AS CDC_OPER_CD
FROM (
SELECT combined_account_join_date
,combined_account_leave_date
,combined_producer_account_identifier
,combined_account_member_identifier
,data_status_code
,last_change_date
,last_change_user_name
,subsidiary_customer_identifier
, op,
ROW_NUMBER() OVER (PARTITION BY combined_account_member_identifier ORDER BY last_change_date DESC) as rnum
                       FROM `fsa-{env}-sbsd-cdc`.`combined_account_member`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') SubQry
WHERE SubQry.rnum=1