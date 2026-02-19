SELECT hel_stat_srgt_id, 
		hel_stat_durb_id, 
		cur_rcd_ind, 
		data_eff_strt_dt, 
		data_eff_end_dt, 
		cre_dt, 
		hel_stat_cd, 
		hel_stat_nm, 
		hel_stat_desc
FROM farm_dm_stg.hel_stat_dim
where hel_stat_srgt_id > 0
order by hel_stat_srgt_id asc;