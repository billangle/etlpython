INSERT INTO sql_farm_rcd_stg.dmn_val
(dmn_val_id, dmn_id, dmn_char_val, dflt_val_ind, dply_seq_nbr, 
dmn_val_nm, dmn_val_desc, cre_dt, last_chg_dt, last_chg_user_nm, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s);