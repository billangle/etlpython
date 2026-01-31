SELECT arc_plc_elct_srgt_id, 
       arc_plc_elct_durb_id, 
	   cur_rcd_ind, 
	   data_eff_strt_dt, 
	   data_eff_end_dt, 
	   cre_dt, 
	   arc_plc_elct_cd, 
	   arc_plc_elct_nm, 
	   arc_plc_elct_desc
FROM farm_dm_stg.arc_plc_elct_dim
WHERE arc_plc_elct_srgt_id >0
order by arc_plc_elct_srgt_id asc;
          
