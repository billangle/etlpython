INSERT INTO sql_farm_rcd_stg.farm (farm_id, cnty_ofc_ctl_id, farm_nbr, farm_cmn_nm, 
						rcon_pend_apvl_cd, data_lock_dt, st_fsa_cd, cnty_fsa_cd,
						data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm,
						hash_dif,	load_dt, cdc_oper_cd, data_src_nm, cdc_dt
						) 
select  distinct 
        f.farm_identifier as farm_id,
        f.county_office_control_identifier as cnty_ofc_ctl_id,
        f.farm_number as farm_nbr,
        f.farm_common_name as farm_cmn_nm,
        f.reconstitution_pending_approval_code as rcon_pend_apvl_cd,
        f.data_locked_date as data_lock_dt,
        c.state_fsa_code as st_fsa_cd, 
        c.county_fsa_code as cnty_fsa_cd,
        f.data_status_code as data_stat_cd,
        f.creation_date as cre_dt,
        f.last_change_date as last_chg_dt,
        f.last_change_user_name as last_chg_user_nm,
		''  as hash_dif,
		CAST(current_date as date) as load_dt,
        f.cdc_oper_cd as cdc_oper_cd,
        'SAP/CRM' as data_src_nm,
        f.cdc_dt as cdc_dt
from farm_records_reporting.farm f
join farm_records_reporting.county_office_control c on c.county_office_control_identifier = f.county_office_control_identifier
where f.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'