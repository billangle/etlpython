INSERT INTO sql_farm_rcd_stg.farm (farm_id, cnty_ofc_ctl_id, farm_nbr, farm_cmn_nm, 
						rcon_pend_apvl_cd, data_lock_dt, st_fsa_cd, cnty_fsa_cd,
						data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm,
						hash_dif,	load_dt, cdc_oper_cd, data_src_nm, cdc_dt
						) VALUES (%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s ) 