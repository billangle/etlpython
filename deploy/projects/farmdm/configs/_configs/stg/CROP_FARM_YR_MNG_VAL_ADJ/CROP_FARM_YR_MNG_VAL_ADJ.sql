INSERT INTO sql_farm_rcd_stg.crop_farm_yr_mng_val_adj
(crop_farm_yr_mng_val_adj_id, yr_arc_plc_ptcp_elct_id, farm_yr_crop_irr_hist_id, 
crop_farm_yr_dcp_id, crop_farm_yr_mng_val_adj_ty, crop_farm_yr_mng_val_adj_rs, 
aft_adj_val, bef_adj_val, pgm_abr, fsa_crop_cd, fsa_crop_type_cd, farm_nbr, 
st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, cre_user_nm, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s,%s, %s, 
%s,%s,%s,%s,%s,%s,%s, %s);
