SELECT 
fsa_county_identifier AS CNTY_FSA_SVC_CTR_ID
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,subsidiary_customer_profile_identifier AS SBSD_CUST_PRFL_ID
, op AS CDC_OPER_CD
FROM (
SELECT fsa_county_identifier
,last_change_date
,last_change_user_name
,subsidiary_customer_profile_identifier
, op, ROW_NUMBER() OVER (PARTITION BY CAST(subsidiary_customer_profile_identifier as VARCHAR(30))+CAST(fsa_county_identifier as VARCHAR(30)) 
ORDER BY last_change_date DESC) as rnum
            FROM `fsa-{env}-subsidiary`.`county_customer_profile_association`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') as SubQry
WHERE SubQry.rnum=1