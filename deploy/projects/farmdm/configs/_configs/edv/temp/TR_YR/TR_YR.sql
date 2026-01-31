INSERT INTO sql_farm_rcd_stg.tr_yr
(tr_yr_id, tr_id, farm_yr_id, fmld_acrg, cpld_acrg, crp_acrg, mpl_acrg, wbp_acrg, wrp_tr_acrg, 
grp_cpld_acrg, st_cnsv_acrg, ot_cnsv_acrg, sugarcane_acrg, nap_crop_acrg, ntv_sod_brk_out_acrg,
 hel_tr_cd, wl_pres_cd, farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt,
 last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt, ewp_tr_acrg)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);