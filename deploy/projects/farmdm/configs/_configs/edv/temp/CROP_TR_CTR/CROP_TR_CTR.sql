INSERT INTO sql_farm_rcd_stg.crop_tr_ctr
(crop_tr_ctr_id, tr_id, crop_id, ctr_nbr, rdn_acrg_strt_yr, rdn_acrg, 
crop_pgm_pymt_yld, crop_pgm_alt_pymt_yld, tr_nbr, st_fsa_cd, 
cnty_fsa_cd, pgm_abr, fsa_crop_cd, fsa_crop_type_cd, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt, tr_yr_id, pgm_yr)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,
%s, %s, %s, %s, %s);


