select  distinct 
        fyd.farm_year_dcp_identifier as farm_yr_dcp_id,
		fyd.farm_year_identifier as farm_yr_id,
        f.farm_identifier as farm_id,
        f.county_office_control_identifier as cnty_ofc_ctl_id,
        f.farm_number as farm_nbr,
        f.farm_common_name as farm_cmn_nm,
        f.reconstitution_pending_approval_code as rcon_pend_apvl_cd,
        f.data_locked_date as data_lock_dt,
        c.time_period_identifier as tm_prd_id,
        c.state_fsa_code as st_fsa_cd, 
        c.county_fsa_code as cnty_fsa_cd,
        CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description,
        f.data_status_code as data_stat_cd,
        f.creation_date as cre_dt,
        f.last_change_date as last_chg_dt,
        f.last_change_user_name as last_chg_user_nm,
		c.time_period_identifier + 1998 as pgm_yr,
		dcp_double_crop_acreage as dcp_dbl_crop_acrg,
		'' as hash_dif
from farm_records_reporting.county_office_control c
inner join farm_records_reporting.farm f
on f.county_office_control_identifier = c.county_office_control_identifier
-- and (DATE_PART('year', f.creation_date) - 1998) = c.time_period_identifier
and c.last_assigned_farm_number = cast(f.farm_number as INT)
inner join farm_records_reporting.farm_year fy
on f.farm_identifier = fy.farm_identifier
-- and (DATE_PART('year', f.creation_date) - 1998) = fy.time_period_identifier
inner join farm_records_reporting.farm_year_dcp fyd
on fyd.farm_year_identifier = fy.farm_year_identifier
-- and (DATE_PART('year', fyd.creation_date) - 1998) = fy.time_period_identifier



