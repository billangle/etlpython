SELECT
    domain_value_identifier AS dmn_val_id,
    domain_identifier AS dmn_id,
    LTRIM(RTRIM(domain_character_value)) AS dmn_char_val,
    LTRIM(RTRIM(default_value_indicator)) AS dflt_val_ind,
    display_sequence_number AS dply_seq_nbr,
    LTRIM(RTRIM(domain_value_name)) AS dmn_val_nm,
    LTRIM(RTRIM(domain_value_description)) AS dmn_val_desc,
    creation_date AS cre_dt,
    last_change_date AS last_chg_dt,
    LTRIM(RTRIM(last_change_user_name)) AS last_chg_user_nm,
    '' AS hash_dif,
    cdc_oper_cd AS cdc_oper_cd,      
    CURRENT_DATE AS load_dt,  
    'SQL_FARM_RCD' AS data_src_nm,
	cdc_dt
FROM farm_records_reporting.domain_value
where domain_value.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'