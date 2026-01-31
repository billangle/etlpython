INSERT INTO sql_farm_rcd_stg.dmn_val
(
    dmn_val_id, 
    dmn_id, 
    dmn_char_val, 
    dflt_val_ind, 
    dply_seq_nbr, 
    dmn_val_nm, 
    dmn_val_desc, 
    cre_dt, 
    last_chg_dt, 
    last_chg_user_nm, 
    hash_dif, 
    cdc_oper_cd, 
    load_dt, 
    data_src_nm, 
    cdc_dt
)
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
    'I' AS cdc_oper_cd,      
    CURRENT_DATE AS load_dt,  
    'SQL_FARM_RCD' AS data_src_nm,
	CURRENT_DATE - 1
FROM farm_records_reporting.domain_value
where domain_value.cdc_dt >= current_date - 1