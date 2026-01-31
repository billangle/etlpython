INSERT INTO sql_farm_rcd_stg.tr_yr_adj
(tr_yr_adj_id, tr_yr_id, tr_yr_adj_type_cd, tr_yr_adj_rsn_cd, aft_adj_val, bef_adj_val,
 farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt, 
 last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt, prnt_tbl_phy_nm)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);