SELECT 
acct_pgm.account_program_identifier AS acct_pgm_cd
,acct_pgm.creation_date AS cre_dt
,acct_pgm.creation_username AS cre_user_nm
,acct_pgm.last_change_date AS last_chg_dt
,acct_pgm.last_change_username AS last_chg_user_nm
,acct_pgm.data_status_code AS data_stat_cd
,acct_pgm.account_program_description AS acct_pgm_desc
,'' AS hash_diff
,acct_pgm.op AS cdc_oper_cd
,current_timestamp AS load_dt
,'SBSD_STG' AS data_src_nm
,dart_filedate AS cdc_dt

FROM `fsa-{env}-sbsd-cdc`.`accounting_program`

WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'


