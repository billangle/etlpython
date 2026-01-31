INSERT INTO sql_farm_rcd_stg.yr_arc_plc_ptcp_elct
(yr_arc_plc_ptcp_elct_id
, farm_yr_id
, crop_id
, arc_plc_elct_chc_id
, pgm_abr
, fsa_crop_cd
, fsa_crop_type_cd
, farm_nbr
, st_fsa_cd
, cnty_fsa_cd
, pgm_yr
, data_stat_cd
, cre_dt
, cre_user_nm
, last_chg_dt
, last_chg_user_nm
, cdc_oper_cd
, load_dt
, data_src_nm
, cdc_dt)
SELECT
farc.year_arc_plc_participation_election_identifier
,farc.farm_year_identifier
,farc.crop_identifier
,farc.arc_plc_election_choice_identifier
,c.program_abbreviation
,c.fsa_crop_code
,c.fsa_crop_type_code
,f.farm_number
,coc.state_fsa_code
,coc.county_fsa_code
,cast(tp.time_period_name as int)
,farc.data_status_code
,farc.creation_date
,farc.creation_user_name
,farc.last_change_date
,farc.last_change_user_name
,farc.cdc_oper_cd
,CAST(current_date as date)
,'SAP/CRM'
,CAST((current_date - 1) as date)
from farm_records_reporting.year_arc_plc_participation_election farc
	inner join farm_records_reporting.farm_year fy on farc.farm_year_identifier = fy.farm_year_identifier
	inner join farm_records_reporting.time_period tp on fy.time_period_identifier = tp.time_period_identifier
	inner join farm_records_reporting.crop c on farc.crop_identifier = c.crop_identifier
	inner join farm_records_reporting.farm f on f.farm_identifier = fy.farm_identifier
	inner join farm_records_reporting.county_office_control coc on coc.county_office_control_identifier = f.county_office_control_identifier
where farc.cdc_dt >= current_date - 1
;