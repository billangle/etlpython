INSERT INTO sql_farm_rcd_stg.farm_yr_dcp
(farm_yr_dcp_id, farm_yr_id, dcp_dbl_crop_acrg, farm_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s);