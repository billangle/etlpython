SELECT arc_plc_elg_dter_durb_id, 
	cre_dt, 
	last_chg_dt, 
	data_stat_cd, 
	arc_plc_elg_dter_cd, 
	arc_plc_elg_dter_nm, 
	arc_plc_elg_dter_desc
FROM farm_dm_stg.arc_plc_elg_dter_dim
where arc_plc_elg_dter_durb_id > 0
order by arc_plc_elg_dter_durb_id asc;