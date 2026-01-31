SELECT farm_prdr_cw_excp_durb_id, 
       cre_dt, 
	   last_chg_dt, 
	   data_stat_cd, 
	   cw_excp_cd, 
	   cw_excp_nm, 
	   cw_excp_desc
FROM farm_dm_stg.farm_prdr_cw_excp_dim
where  FARM_PRDR_CW_EXCP_DURB_ID >0 
Order by farm_prdr_cw_excp_durb_id Asc;

