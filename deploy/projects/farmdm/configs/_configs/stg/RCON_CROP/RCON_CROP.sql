INSERT INTO sql_farm_rcd_stg.rcon_crop
(rcon_crop_id, rcon_id, crop_id, rcon_div_mthd_cd, pgm_abr, fsa_crop_cd, 
fsa_crop_type_cd, st_fsa_cd, cnty_fsa_cd, data_stat_cd, cre_dt, 
last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s);
