INSERT INTO sql_farm_rcd_stg.crop_tr_yr_dcp_adj
(crop_tr_yr_dcp_adj_id, crop_tr_yr_dcp_id, dcp_adj_type_cd, 
dcp_adj_rsn_cd, aft_adj_val, bef_adj_val, pgm_abr, fsa_crop_cd, 
fsa_crop_type_cd, farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, 
pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm,
 hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES( %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s,%s,%s);