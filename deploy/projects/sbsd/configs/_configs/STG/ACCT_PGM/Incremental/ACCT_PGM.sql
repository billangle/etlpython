SELECT 
accounting_program_code AS acct_pgm_cd
,creation_date AS cre_dt
,creation_user_name AS cre_user_nm
,last_change_date AS last_chg_dt
,last_change_user_name AS last_chg_user_nm
,data_status_code AS data_stat_cd
,accounting_program_description AS acct_pgm_desc
,'' AS hash_diff
,op AS cdc_oper_cd
,current_timestamp AS load_dt
,'SBSD_STG' AS data_src_nm
,dart_filedate AS cdc_dt

FROM `fsa-{env}-sbsd-cdc`.`accounting_program`

WHERE cast(dart_filedate as DATE) BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'


