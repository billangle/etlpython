SELECT 
fsa_county_identifier AS CNTY_FSA_SVC_CTR_ID
,state_county_fsa_code AS ST_CNTY_FSA_CD
,last_change_date AS LAST_CHG_DT
,last_change_user_name AS LAST_CHG_USER_NM
, op AS CDC_OPER_CD
FROM (
SELECT fsa_county_identifier
,state_county_fsa_code
,last_change_date
,last_change_user_name
, op, ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier ORDER BY last_change_date DESC)as rnum
            FROM `fsa-{env}-sbsd-cdc`.`fsa_county`
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') as SubQry
WHERE SubQry.rnum=1