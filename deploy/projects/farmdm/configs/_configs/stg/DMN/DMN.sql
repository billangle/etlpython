INSERT INTO sql_farm_rcd_stg.dmn
(dmn_id, dmn_nm, dmn_desc, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
VALUES(%s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s);