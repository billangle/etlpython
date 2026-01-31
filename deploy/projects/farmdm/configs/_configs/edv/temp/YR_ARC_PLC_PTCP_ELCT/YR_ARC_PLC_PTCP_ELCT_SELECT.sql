SELECT year_arc_plc_participation_election.year_arc_plc_participation_election_identifier As YR_ARC_PLC_PTCP_ELCT_ID,
year_arc_plc_participation_election.farm_year_identifier As FARM_YR_ID,
year_arc_plc_participation_election.crop_identifier As CROP_ID,
year_arc_plc_participation_election.arc_plc_election_choice_identifier As ARC_PLC_ELCT_CHC_ID,
LTrim(RTrim(crop.program_abbreviation) ) As PGM_ABR,LTrim(RTrim(crop.fsa_crop_code) ) As FSA_CROP_CD,
LTrim(RTrim(crop.fsa_crop_type_code) ) As FSA_CROP_TYPE_CD,LTrim(RTrim(farm.farm_number) ) As FARM_NBR,
LTrim(RTrim(county_office_control.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(county_office_control.county_fsa_code) ) As CNTY_FSA_CD,
time_period.time_period_name As PGM_YR,
LTrim(RTrim(year_arc_plc_participation_election.data_status_code) ) As DATA_STAT_CD,
year_arc_plc_participation_election.creation_date As CRE_DT,
LTrim(RTrim(year_arc_plc_participation_election.creation_user_name) ) As CRE_USER_NM,
year_arc_plc_participation_election.last_change_date As LAST_CHG_DT,
LTrim(RTrim(year_arc_plc_participation_election.last_change_user_name) ) As LAST_CHG_USER_NM
FROM farm_records_reporting.year_arc_plc_participation_election
LEFT JOIN farm_records_reporting.farm_year ON ( year_arc_plc_participation_election.farm_year_identifier = farm_year.farm_year_identifier ) 
JOIN farm_records_reporting.farm ON ( farm_year.farm_identifier = farm.farm_identifier ) 
JOIN farm_records_reporting.county_office_control ON ( farm.county_office_control_identifier = county_office_control.county_office_control_identifier ) 
JOIN farm_records_reporting.crop ON ( year_arc_plc_participation_election.crop_identifier = crop.crop_identifier ) 
JOIN farm_records_reporting.time_period ON (farm_year.time_period_identifier = time_period.time_period_identifier )
