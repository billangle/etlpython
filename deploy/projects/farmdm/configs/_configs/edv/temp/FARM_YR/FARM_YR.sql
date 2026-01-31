INSERT INTO sql_farm_rcd_stg.farm_yr
(farm_yr_id, farm_id, tm_prd_id, crp_acrg, mpl_acrg, farm_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, 
data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, ver_nbr, hash_dif, cdc_oper_cd, load_dt, 
data_src_nm, cdc_dt, arc_plc_elg_dter_id, arc_plc_elg_dter_cd)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s);