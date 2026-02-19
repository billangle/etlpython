SELECT farm_prdr_pcw_excp_durb_id, 
       cre_dt, 
	   last_chg_dt, 
	   data_stat_cd, 
	   pcw_excp_cd, 
	   pcw_excp_nm, 
	   pcw_excp_desc
FROM farm_dm_stg.farm_prdr_pcw_excp_dim
WHERE  FARM_PRDR_PCW_EXCP_DURB_ID >0
ORDER BY pcw_excp_cd asc
;
 