INSERT INTO sql_farm_rcd_stg.tr_prdr_yr
(tr_prdr_yr_id, core_cust_id, tr_yr_id, prdr_invl_cd, prdr_invl_strt_dt, prdr_invl_end_dt, 
prdr_invl_intrpt_ind, tr_prdr_hel_excp_cd, tr_prdr_cw_excp_cd, tr_prdr_pcw_excp_cd, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, tm_prd_id, st_fsa_cd, cnty_fsa_cd, farm_id, farm_nbr, tr_nbr, pgm_yr, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt, hel_apls_exhst_dt, cw_apls_exhst_dt, pcw_apls_exhst_dt,
 tr_prdr_rma_hel_excp_cd, tr_prdr_rma_cw_excp_cd, tr_prdr_rma_pcw_excp_cd)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); 