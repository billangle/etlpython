INSERT INTO sql_farm_rcd_stg.tr (tr_id, cnty_ofc_ctl_id, tr_nbr, tr_desc, bia_rng_unit_nbr, 
								loc_st_fsa_cd, loc_cnty_fsa_cd, cong_dist_cd, wl_cert_cplt_cd, 
								wl_cert_cplt_yr, st_fsa_cd, cnty_fsa_cd, data_stat_cd, cre_dt, 
								last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, 
								data_src_nm, cdc_dt
								) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
								%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
