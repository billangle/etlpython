INSERT INTO sql_farm_rcd_stg.crop
(crop_id, pgm_abr, fsa_crop_cd, fsa_crop_abr, fsa_crop_nm, fsa_crop_type_nm, 
fsa_crop_type_cd, dply_seq_nbr, data_stat_cd, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s)