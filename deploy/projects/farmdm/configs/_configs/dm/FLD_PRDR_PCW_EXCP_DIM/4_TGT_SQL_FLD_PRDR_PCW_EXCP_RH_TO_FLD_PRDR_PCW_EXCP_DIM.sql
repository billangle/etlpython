SELECT fld_prdr_pcw_excp_durb_id, 
       cre_dt, 
	   last_chg_dt, 
	   data_stat_cd, 
	   pcw_excp_cd, 
	   pcw_excp_nm, 
	   pcw_excp_desc
FROM farm_dm_stg.fld_prdr_pcw_excp_dim
where  FLD_PRDR_PCW_EXCP_DURB_ID > 0 
Order by fld_prdr_pcw_excp_durb_id Asc