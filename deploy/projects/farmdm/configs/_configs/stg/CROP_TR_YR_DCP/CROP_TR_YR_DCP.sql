INSERT INTO sql_farm_rcd_stg.crop_tr_yr_dcp
(crop_tr_yr_dcp_id, tr_yr_dcp_id, crop_id, crp_rdn_acrg, crp_rel_acrg, 
dcp_crop_base_acrg, fav_rdn_acrg, ccp_pymt_yld, crp_pymt_yld, 
dir_pymt_yld, fav_dir_pymt_yld, fav_ccp_pymt_yld, pgm_abr, 
fsa_crop_cd, fsa_crop_type_cd, farm_nbr, tr_nbr, st_fsa_cd, 
cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s, %s,%s, %s, %s, %s,%s,%s,%s);


