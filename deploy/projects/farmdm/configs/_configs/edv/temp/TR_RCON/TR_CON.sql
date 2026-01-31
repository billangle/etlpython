INSERT INTO sql_farm_rcd_stg.tr_rcon
(tr_rcon_id, rcon_id, prnt_tr_yr_id, rslt_tr_yr_id, data_stat_cd, prnt_tr_nbr, prnt_st_fsa_cd, 
prnt_cnty_fsa_cd, rslt_tr_nbr, rslt_st_fsa_cd, rslt_cnty_fsa_cd, pgm_yr, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);