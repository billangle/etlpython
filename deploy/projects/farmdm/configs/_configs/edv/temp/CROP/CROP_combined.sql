INSERT INTO sql_farm_rcd_stg.crop
(crop_id, pgm_abr, fsa_crop_cd, fsa_crop_abr, fsa_crop_nm, fsa_crop_type_nm, 
fsa_crop_type_cd, dply_seq_nbr, data_stat_cd, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
SELECT crop_identifier as crop_id, 
       program_abbreviation as pgm_abr, 
       fsa_crop_code as fsa_crop_cd, 
       fsa_crop_abbreviation as fsa_crop_abr, 
       fsa_crop_name as fsa_crop_nm, 
       fsa_crop_type_name as fsa_crop_type_nm, 
       fsa_crop_type_code as fsa_crop_type_cd, 
       display_sequence_number as dply_seq_nbr, 
       data_status_code as data_stat_cd, 
       creation_date as cre_dt, 
       last_change_date as last_chg_dt, 
       last_change_user_name as last_chg_user_nm,
       '' as hash_dif,
       'I' as cdc_oper_cd,
       CAST(current_date as date) as load_dt,
       'SQL_FARM_RCD' as data_src_nm,
       CAST((current_date - 1) as date) as cdc_dt
FROM farm_records_reporting.crop
where crop.cdc_dt >= current_date - 1