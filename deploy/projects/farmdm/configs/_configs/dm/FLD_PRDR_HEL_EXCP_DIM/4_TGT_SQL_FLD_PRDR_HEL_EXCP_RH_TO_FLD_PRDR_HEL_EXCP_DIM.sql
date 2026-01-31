SELECT fld_prdr_hel_excp_durb_id, 
		cre_dt, 
		last_chg_dt, 
		data_stat_cd, 
		hel_excp_cd, 
		hel_excp_nm, 
		hel_excp_desc
FROM farm_dm_stg.fld_prdr_hel_excp_dim
where  FLD_PRDR_HEL_EXCP_DURB_ID >0 
Order by fld_prdr_hel_excp_durb_id Asc;