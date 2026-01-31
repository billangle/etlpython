SELECT cre_dt, 
last_chg_dt, 
data_stat_cd, 
pgm_yr, 
prnt_farm_srgt_id, 
rslt_farm_srgt_id, 
prnt_farm_durb_id, 
rslt_farm_durb_id, 
prnt_fsa_st_cnty_srgt_id, 
prnt_fsa_st_cnty_durb_id, 
rslt_fsa_st_cnty_srgt_id, 
rslt_fsa_st_cnty_durb_id, 
xfr_cre_dt, 
xfr_cre_user_nm, 
xfr_last_chg_dt, 
xfr_last_chg_user_nm, 
src_data_stat_cd, 
xfr_cre_dt_id, 
xfr_cre_tm_id, 
xfr_last_chg_dt_id, 
xfr_last_chg_tm_id
FROM farm_dm_stg.farm_xfr_fact
where prnt_farm_srgt_id > 0
order by prnt_farm_srgt_id asc;

