INSERT INTO sql_farm_rcd_stg.farm_yr_crop_irr_hist
(farm_yr_crop_irr_hist_id, farm_yr_id, irr_cnty_crop_id, hist_irr_pct, st_fsa_cd, cnty_fsa_cd, 
pgm_abr, fsa_crop_cd, fsa_crop_type_cd, farm_nbr, farm_st_fsa_cd, farm_cnty_fsa_cd, pgm_yr, 
data_stat_cd, cre_dt, cre_user_nm, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);