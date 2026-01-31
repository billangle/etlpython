INSERT INTO sql_farm_rcd_stg.clu_prdr_yr
(clu_prdr_yr_id, clu_yr_id, core_cust_id, prdr_invl_cd, clu_prdr_hel_excp_cd, 
clu_prdr_cw_excp_cd, clu_prdr_pcw_excp_cd, farm_nbr, tr_nbr, st_fsa_cd, 
cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt, hel_apls_exhst_dt, 
cw_apls_exhst_dt, pcw_apls_exhst_dt, clu_prdr_rma_hel_excp_cd, 
clu_prdr_rma_cw_excp_cd, clu_prdr_rma_pcw_excp_cd)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s,%s, 
%s, %s,%s,%s,%s,%s, %s, %s,%s,%s, %s)