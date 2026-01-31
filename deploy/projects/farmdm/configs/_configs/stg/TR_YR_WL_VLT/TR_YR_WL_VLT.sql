INSERT INTO sql_farm_rcd_stg.tr_yr_wl_vlt
(tr_yr_wl_vlt_id, tr_yr_id, wl_vlt_type_cd, farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, 
pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);