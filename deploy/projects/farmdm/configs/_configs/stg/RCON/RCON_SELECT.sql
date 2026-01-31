select distinct 
       r.reconstitution_identifier as rcon_id, 
       r.county_office_control_identifier as cnty_ofc_ctl_id, 
       r.time_period_identifier as tm_prd_id, 
       reconstitution_prefix_code as rcon_pfx_cd, 
       reconstitution_sequence_number as rcon_seq_nbr, 
       reconstitution_type_code as rcon_type_cd, 
       reconstitution_approval_date as rcon_apvl_dt, 
       reconstitution_effective_date as rcon_eff_dt, 
       c.state_fsa_code as st_fsa_cd, 
       c.county_fsa_code as cnty_fsa_cd, 
       r.time_period_identifier + 1998 as pgm_yr, 
       r.data_status_code as data_stat_cd, 
       r.creation_date as cre_dt, 
       r.last_change_date as last_chg_dt, 
       r.last_change_user_name as last_chg_user_nm, 
       r.reconstitution_initiation_date as rcon_init_dt, 
       '' as hash_dif,
       'I' as cdc_oper_cd,
       CAST(current_date as date) as load_dt,
       'SQL_FARM_RCD' as data_src_nm,
       CAST((current_date - 1) as date) as cdc_dt,
	   c.last_assigned_farm_number as farm_nbr,
       CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description        
from farm_records_reporting.reconstitution r
left join farm_records_reporting.county_office_control c
	on r.county_office_control_identifier = c.county_office_control_identifier 


