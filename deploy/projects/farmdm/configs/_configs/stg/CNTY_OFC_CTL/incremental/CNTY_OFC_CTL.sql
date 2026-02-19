INSERT INTO sql_farm_rcd_stg.cnty_ofc_ctl
(cnty_ofc_ctl_id, tm_prd_id, st_fsa_cd, cnty_fsa_cd, last_asgn_farm_nbr, 
last_asgn_tr_nbr, last_asgn_rcon_seq_nbr, pgm_yr, data_stat_cd, 
cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
select  distinct 
		coc.county_office_control_identifier as cnty_ofc_ctl_id,
        coc.time_period_identifier as tm_prd_id,
        coc.state_fsa_code as st_fsa_cd,
        coc.county_fsa_code as cnty_fsa_cd,
        coc.last_assigned_farm_number as last_asgn_farm_nbr,
        coc.last_assigned_tract_number as last_asgn_tr_nbr,
        coc.last_assigned_reconstitution_sequence_number as last_asgn_rcon_seq_nbr,
        cast(t.time_period_name as int) as pgm_yr,
        coc.data_status_code as data_stat_cd,
        coc.creation_date as cre_dt,
        coc.last_change_date as last_chg_dt,
        coc.last_change_user_name as last_chg_user_nm,
        ''  as hash_dif,
        coc.cdc_oper_cd as cdc_oper_cd,
        CAST(current_date as date) as load_dt,
        'SAP/CRM' as data_src_nm,
        coc.cdc_dt as cdc_dt              
from farm_records_reporting.county_office_control coc 
inner join farm_records_reporting.time_period t
on t.time_period_identifier  = coc.time_period_identifier
where coc.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
order by county_office_control_identifier asc
