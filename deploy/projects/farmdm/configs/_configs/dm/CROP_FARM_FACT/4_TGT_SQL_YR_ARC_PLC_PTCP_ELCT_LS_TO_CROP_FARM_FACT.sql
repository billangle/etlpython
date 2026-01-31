SELECT cre_dt, 
	 last_chg_dt,
	 data_stat_cd, 
	 farm_srgt_id, 
	 farm_durb_id, 
	 adm_fsa_st_cnty_srgt_id, 
	 adm_fsa_st_cnty_durb_id, 
	 pgm_yr, 
	 fsa_crop_srgt_id, 
	 fsa_crop_durb_id, 
	 arc_plc_elct_srgt_id, 
	 arc_plc_elct_durb_id, 
	 arc_plc_ptcp_elct_cre_dt, 
	 arc_plc_ptcp_elct_cre_user_nm, 
	 arc_plc_ptcp_elct_last_chg_dt, 
	 arc_plc_elct_last_chg_user_nm, 
	 src_data_stat_cd, 
	 arc_plc_elg_dter_durb_id
FROM farm_dm_stg.crop_farm_fact
where arc_plc_elg_dter_durb_id >0
ORDER BY arc_plc_elg_dter_durb_id asc;