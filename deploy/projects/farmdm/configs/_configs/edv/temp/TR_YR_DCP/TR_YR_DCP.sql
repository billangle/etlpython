INSERT INTO sql_farm_rcd_stg.tr_yr_dcp
(tr_yr_dcp_id, tr_yr_id, dcp_dbl_crop_acrg, dcp_cpld_acrg, dcp_aft_rdn_acrg, fav_wr_hist_ind, 
farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);