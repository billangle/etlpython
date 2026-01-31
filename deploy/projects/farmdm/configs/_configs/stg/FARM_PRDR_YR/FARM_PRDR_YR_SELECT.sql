select  distinct 
		fpy.farm_producer_year_identifier as farm_prdr_yr_id, 
		fpy.core_customer_identifier as core_cust_id, 
		fpy.farm_year_identifier as farm_yr_id, 
		fpy.producer_involvement_code as prdr_invl_cd, 
		fpy.producer_involvement_interrupted_indicator as prdr_invl_intrpt_ind, 
		fpy.producer_involvement_start_date as prdr_invl_strt_dt, 
		fpy.producer_involvement_end_date as prdr_invl_end_dt, 
		fpy.farm_producer_hel_exception_code as farm_prdr_hel_excp_cd, 
		fpy.farm_producer_cw_exception_code as farm_prdr_cw_excp_cd, 
		fpy.farm_producer_pcw_exception_code as farm_prdr_pcw_excp_cd, 
		fpy.data_status_code as data_stat_cd, 
		fpy.creation_date as cre_dt, 
		fpy.last_change_date as last_chg_dt, 
		fpy.last_change_user_name as last_chg_user_nm, 
		fpy.time_period_identifier as tm_prd_id, 
		fpy.state_fsa_code as st_fsa_cd, 
		fpy.county_fsa_code as cnty_fsa_cd, 
		fpy.farm_identifier as farm_id, 
		fpy.farm_number as farm_nbr, 
		'' as hash_dif,
		fpy.hel_appeals_exhausted_date as hel_apls_exhst_dt, 
		fpy.cw_appeals_exhausted_date as cw_apls_exhst_dt, 
		fpy.pcw_appeals_exhausted_date as pcw_apls_exhst_dt, 
		fpy.farm_producer_rma_hel_exception_code as farm_prdr_rma_hel_excp_cd, 
		fpy.farm_producer_rma_cw_exception_code as farm_prdr_rma_cw_excp_cd, 
		fpy.farm_producer_rma_pcw_exception_code as farm_prdr_rma_pcw_excp_cd,
        f.county_office_control_identifier as cnty_ofc_ctl_id,
        f.farm_common_name as farm_cmn_nm,
		c.time_period_identifier + 1998 as pgm_yr,
        'I' as cdc_oper_cd,
        CAST(current_date as date) as load_dt,
        'SQL_FARM_RCD' as data_src_nm,
        CAST((current_date - 1) as date) as cdc_dt,
        CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description
from farm_records_reporting.county_office_control c
inner join farm_records_reporting.farm f
on f.county_office_control_identifier = c.county_office_control_identifier
-- and (DATE_PART('year', f.creation_date) - 1998) = c.time_period_identifier
and c.last_assigned_farm_number = cast(f.farm_number as INT)
INNER JOIN farm_records_reporting.farm_year fy
on f.farm_identifier = fy.farm_identifier
-- and (DATE_PART('year', f.creation_date) - 1998) = fy.time_period_identifier
INNER JOIN farm_records_reporting.farm_producer_year fpy 
on fy.farm_year_identifier = fpy.farm_year_identifier
 and f.farm_identifier = fpy.farm_identifier
 and (DATE_PART('year', f.creation_date) - 1998) = fpy.time_period_identifier


