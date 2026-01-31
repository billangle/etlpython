INSERT INTO sql_farm_rcd_stg.clu_yr
(clu_yr_id, tr_yr_id, clu_alt_id, fld_nbr, loc_cnty_fsa_cd, 
loc_st_fsa_cd, st_ansi_cd, cnty_ansi_cd, 
cong_dist_cd, land_cls_id, clu_acrg, hel_stat_id, 
cpld_ind_3cm, clu_desc, crp_ctr_nbr, crp_ctr_expr_dt, 
cnsv_prac_id, ntv_sod_cvsn_dt, sod_cvsn_crop_yr_1, 
sod_cvsn_crop_yr_2, sod_cvsn_crop_yr_3, sod_cvsn_crop_yr_4, 
farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s,%s, %s, %s,%s,%s,
%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s);
