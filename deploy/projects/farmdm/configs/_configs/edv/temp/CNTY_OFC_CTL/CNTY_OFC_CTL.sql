INSERT INTO sql_farm_rcd_stg.cnty_ofc_ctl
(cnty_ofc_ctl_id, tm_prd_id, st_fsa_cd, cnty_fsa_cd, last_asgn_farm_nbr, 
last_asgn_tr_nbr, last_asgn_rcon_seq_nbr, pgm_yr, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s);