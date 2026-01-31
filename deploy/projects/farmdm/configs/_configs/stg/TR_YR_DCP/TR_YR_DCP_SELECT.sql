select distinct f.farm_identifier as farm_id,
        f.county_office_control_identifier as cnty_ofc_ctl_id,
        f.farm_number as farm_nbr,
        f.farm_common_name as farm_cmn_nm,
        f.reconstitution_pending_approval_code as rcon_pend_apvl_cd,
        f.data_locked_date as data_lock_dt,
        c.time_period_identifier as tm_prd_id,
        c.state_fsa_code as st_fsa_cd, 
        c.county_fsa_code as cnty_fsa_cd,
        CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description,
        tyd.data_status_code as data_stat_cd,
        tyd.creation_date as cre_dt,
        tyd.last_change_date as last_chg_dt,
        ty.last_change_user_name as last_chg_user_nm,
		t.tract_number as tr_nbr, 
		t.tract_description as tr_desc, 
		TRIM(t.bia_range_unit_number) as bia_rng_unit_nbr, 
		t.location_state_fsa_code as loc_st_fsa_cd, 
		t.location_county_fsa_code as loc_cnty_fsa_cd, 
		t.congressional_district_code as cong_dist_cd, 
		t.wl_certification_completion_code as wl_cert_cplt_cd, 
		t.wl_certification_completion_year as wl_cert_cplt_yr,
		t.tract_identifier as tr_id,
		'' as hash_dif,
		ty.farm_year_identifier as farm_yr_id,
		ty.farmland_acreage as fmld_acrg,
		ty.cropland_acreage as cpld_acrg,
		ty.crp_acreage as crp_acrg,
		ty.mpl_acreage as mpl_acrg,
		ty.wbp_acreage as wbp_acrg,
		ty.wrp_tract_acreage AS wrp_tr_acrg,
		ty.grp_cropland_acreage AS grp_cpld_acrg,
		ty.state_conservation_acreage AS st_cnsv_acrg,
		ty.other_conservation_acreage AS ot_cnsv_acrg,
		ty.sugarcane_acreage AS sugarcane_acrg,
		ty.nap_crop_acreage AS nap_crop_acrg,
		ty.native_sod_broken_out_acreage AS ntv_sod_brk_out_acrg,
		ty.hel_tract_code AS hel_tr_cd,
		ty.wl_presence_code AS wl_pres_cd,
		'' as pgm_yr,
		ty.ewp_acreage as ewp_tr_acrg,
		ty.tract_year_identifier as tr_yr_id,
		tyd.dcp_double_crop_acreage	as dcp_dbl_crop_acrg,
		tyd.dcp_cropland_acreage as dcp_cpld_acrg,
		tyd.dcp_after_reduction_acreage	as dcp_aft_rdn_acrg,
		tyd.fav_wr_history_indicator as fav_wr_hist_ind,
		tyd.tract_year_dcp_identifier as tr_yr_dcp_id,
		'I' as cdc_oper_cd,
        CAST(current_date as date) as load_dt,
        'SQL_FARM_RCD' as data_src_nm,
        CAST((current_date - 1) as date) as cdc_dt
from farm_records_reporting.county_office_control c
inner join farm_records_reporting.farm f
	on f.county_office_control_identifier = c.county_office_control_identifier
	and c.last_assigned_farm_number = cast(f.farm_number as INT)
	-- and (DATE_PART('year', f.creation_date) - 1998) = c.time_period_identifier
inner join farm_records_reporting.farm_year fy
	on f.farm_identifier = fy.farm_identifier
inner join farm_records_reporting.tract_year ty
	on fy.farm_year_identifier = ty.farm_year_identifier	
inner join farm_records_reporting.tract t
	on t.tract_identifier = ty.tract_identifier
	and f.county_office_control_identifier = t.county_office_control_identifier
inner join farm_records_reporting.tract_year_dcp tyd
	on ty.tract_year_identifier = tyd.tract_year_identifier
