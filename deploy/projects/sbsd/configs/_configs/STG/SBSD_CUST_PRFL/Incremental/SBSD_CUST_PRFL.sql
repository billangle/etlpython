SELECT 
subsidiary_customer_profile_identifier AS SBSD_PRD_CUST_ELG_PRFL_ID
,subsidiary_customer_identifier AS SBSD_CUST_ID
,subsidiary_period_identifier AS SBSD_PRD_ID
,eligibility_period_identifier AS SBSD_PRD_CUST_ELG_DET_ID
,business_type_code AS BUS_TYPE_CD
,business_type_effective_date AS BUS_TYPE_EFF_DT
,default_eligibility_indicator AS DFLT_ELG_IND
,data_status_code AS DATA_STAT_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
, op AS CDC_OPER_CD
FROM (
SELECT subsidiary_customer_profile_identifier
,subsidiary_customer_identifier
,subsidiary_period_identifier
,eligibility_period_identifier
,business_type_code
,business_type_effective_date
,default_eligibility_indicator
,data_status_code
,last_change_date
,last_change_user_name
, op, ROW_NUMBER() OVER (PARTITION BY subsidiary_customer_profile_identifier order by last_change_date desc)as rnum
            FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer_profile`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') as SubQry
WHERE SubQry.rnum=1