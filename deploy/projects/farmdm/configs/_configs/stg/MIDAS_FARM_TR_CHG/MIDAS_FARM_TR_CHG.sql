INSERT INTO sql_farm_rcd_stg.midas_farm_tr_chg
(midas_farm_tr_chg_id, farm_chg_type_id, prnt_st_fsa_cd, 
prnt_cnty_fsa_cd, prnt_farm_nbr, prnt_tr_nbr, rslt_st_fsa_cd, 
rslt_cnty_fsa_cd, rslt_farm_nbr, rslt_tr_nbr, data_stat_cd, 
cre_dt, cre_user_nm, last_chg_dt, last_chg_user_nm, hash_dif, 
cdc_oper_cd, load_dt, data_src_nm, cdc_dt, tm_prd_id, 
rcon_seq_nbr, rcon_init_dt, rcon_apvl_dt, tm_prd_nm)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);