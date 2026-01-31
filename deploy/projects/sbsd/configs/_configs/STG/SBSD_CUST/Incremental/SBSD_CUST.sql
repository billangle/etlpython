SELECT 
subsidiary_customer_identifier AS SBSD_CUST_ID
,aperiodic_eligibility_identifier AS CUST_GEN_ELG_PRFL_ID
,fsa_county_identifier AS CNTY_FSA_SVC_CTR_ID
,ltrim(rtrim(core_customer_identifier)) AS CORE_CUST_ID
,data_status_code AS DATA_STAT_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
,version_number AS VER_NBR
,tax_identification_alias AS TAX_ID_ALIAS
,tax_identification_type_code AS TAX_ID_TYPE_CD
, op AS CDC_OPER_CD
 FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer` 