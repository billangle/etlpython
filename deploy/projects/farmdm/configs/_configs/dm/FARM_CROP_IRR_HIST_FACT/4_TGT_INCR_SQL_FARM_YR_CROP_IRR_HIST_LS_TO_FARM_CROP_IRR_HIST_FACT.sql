SELECT cre_dt, last_chg_dt, 
       data_stat_cd, 
	   pgm_yr, 
	   farm_srgt_id, 
	   farm_durb_id, 
	   fsa_crop_srgt_id, 
	   fsa_crop_durb_id, 
	   pgm_abr, 
	   irr_hist_cre_dt, 
	   irr_hist_cre_user_nm, 
	   irr_hist_last_chg_dt, 
	   irr_hist_last_chg_user_nm, 
	   src_data_stat_cd,
	   hist_irr_pct
FROM farm_dm_stg.farm_crop_irr_hist_fact
where farm_srgt_id > 0
order by farm_srgt_id asc
