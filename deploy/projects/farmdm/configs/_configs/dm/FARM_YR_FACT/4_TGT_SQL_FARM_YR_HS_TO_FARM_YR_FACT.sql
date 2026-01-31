SELECT cre_dt, 
	last_chg_dt, 
	data_stat_cd, 
	pgm_yr, 
	adm_fsa_st_cnty_srgt_id, 
	adm_fsa_st_cnty_durb_id, 
	farm_srgt_id, 
	farm_durb_id, 
	farm_yr_cre_dt, 
	farm_yr_last_chg_dt, 
	farm_yr_last_chg_user_nm, 
	farm_yr_dcp_cre_dt, 
	farm_yr_dcp_last_chg_dt, 
	farm_yr_dcp_last_chg_user_nm, 
	src_data_stat_cd, 
	crp_acrg, 
	mpl_acrg, 
	dcp_dbl_crop_acrg, 
	arc_plc_elg_dter_durb_id
FROM farm_dm_stg.farm_yr_fact
where arc_plc_elg_dter_durb_id > 0
order by arc_plc_elg_dter_durb_id asc