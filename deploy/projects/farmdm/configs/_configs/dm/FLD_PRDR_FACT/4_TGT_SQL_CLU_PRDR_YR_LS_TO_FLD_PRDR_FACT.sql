SELECT 
-- fld_prdr_fact_id, 
cre_dt, 
last_chg_dt, 
data_stat_cd, 
src_data_stat_cd, 
clu_prdr_yr_id, 
pgm_yr, 
adm_fsa_st_cnty_srgt_id, 
adm_fsa_st_cnty_durb_id, 
farm_srgt_id, 
farm_durb_id, 
tr_srgt_id, 
tr_durb_id, 
fld_yr_srgt_id, 
fld_durb_id, 
cust_srgt_id, cust_durb_id, 
prdr_invl_srgt_id, 
prdr_invl_durb_id, 
fld_prdr_hel_excp_durb_id, 
fld_prdr_cw_excp_durb_id, 
fld_prdr_pcw_excp_durb_id, 
rma_fld_prdr_hel_excp_durb_id, 
rma_fld_prdr_cw_excp_durb_id, 
rma_fld_prdr_pcw_excp_durb_id, 
hel_apls_exhst_dt, 
cw_apls_exhst_dt, 
pcw_apls_exhst_dt
FROM farm_dm_stg.fld_prdr_fact
where clu_prdr_yr_id > 0
order by clu_prdr_yr_id asc

