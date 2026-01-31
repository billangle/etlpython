SELECT cre_dt, last_chg_dt, data_stat_cd, pgm_yr, 
adm_fsa_st_cnty_srgt_id, adm_fsa_st_cnty_durb_id, 
farm_srgt_id, farm_durb_id, tr_srgt_id, tr_durb_id, 
tr_loc_fsa_st_cnty_srgt_id, tr_loc_fsa_st_cnty_durb_id, 
cong_dist_srgt_id, cong_dist_durb_id, hel_stat_srgt_id, 
hel_stat_durb_id, cnsv_prac_srgt_id, cnsv_prac_durb_id, 
land_cls_srgt_id, land_cls_durb_id, clu_loc_fsa_st_cnty_srgt_id, 
clu_loc_fsa_st_cnty_durb_id, ansi_st_cnty_srgt_id, ansi_st_cnty_durb_id, 
fld_nbr, clu_yr_cre_dt, clu_yr_last_chg_dt, clu_yr_last_chg_user_nm, 
src_data_stat_cd, clu_alt_id, cpld_ind_3cm, clu_desc, 
crp_ctr_nbr, crp_ctr_expr_dt, ntv_sod_cvsn_dt, sod_cvsn_crop_yr_1, 
sod_cvsn_crop_yr_2, 
sod_cvsn_crop_yr_3, sod_cvsn_crop_yr_4, clu_acrg
FROM farm_dm_stg.clu_yr_fact
where farm_srgt_id > 0
order by farm_srgt_id asc