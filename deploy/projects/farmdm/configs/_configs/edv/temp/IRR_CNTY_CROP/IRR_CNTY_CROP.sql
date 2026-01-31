INSERT INTO sql_farm_rcd_stg.irr_cnty_crop
(irr_cnty_crop_id, cnty_ofc_ctl_id, st_fsa_cd, cnty_fsa_cd, 
pgm_abr, fsa_crop_cd, fsa_crop_type_cd, data_stat_cd, cre_dt, 
cre_user_nm, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s);