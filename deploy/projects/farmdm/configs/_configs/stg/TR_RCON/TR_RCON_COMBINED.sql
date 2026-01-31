INSERT INTO sql_farm_rcd_stg.tr_rcon
(tr_rcon_id, rcon_id, prnt_tr_yr_id, rslt_tr_yr_id, data_stat_cd, prnt_tr_nbr, prnt_st_fsa_cd, 
prnt_cnty_fsa_cd, rslt_tr_nbr, rslt_st_fsa_cd, rslt_cnty_fsa_cd, pgm_yr, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
  
SELECT distinct t.tract_reconstitution_identifier As TR_RCON_ID,
t.reconstitution_identifier As RCON_ID,
t.parent_tract_year_identifier As PRNT_TR_YR_ID,
t.resulting_tract_year_identifier As RSLT_TR_YR_ID,
LTrim(RTrim(t.data_status_code) ) As DATA_STAT_CD,
LTrim(RTrim(tract2.tract_number) ) As PRNT_TR_NBR,
LTrim(RTrim(county_office_control2.state_fsa_code) ) As PRNT_ST_FSA_CD,
LTrim(RTrim(county_office_control2.county_fsa_code) ) As PRNT_CNTY_FSA_CD,
LTrim(RTrim(tract1.tract_number) ) As RSLT_TR_NBR,
LTrim(RTrim(county_office_control1.state_fsa_code) ) As RSLT_ST_FSA_CD,
LTrim(RTrim(county_office_control1.county_fsa_code) ) As RSLT_CNTY_FSA_CD,
CAST(tp.time_period_name AS numeric(4)) as PGM_YR,
t.creation_date As CRE_DT,
t.last_change_date As LAST_CHG_DT,
LTrim(RTrim(t.last_change_user_name) ) As LAST_CHG_USER_NM,
''  as hash_dif,
t.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
CAST(current_date-1 as date) as CDC_DT
from farm_records_reporting.tract_reconstitution t   
LEFT JOIN farm_records_reporting.tract_year TY1 ON (t.resulting_tract_year_identifier = TY1.tract_year_identifier) 
JOIN farm_records_reporting.tract tract1 ON (TY1.tract_identifier = tract1.tract_identifier ) 
JOIN farm_records_reporting.county_office_control county_office_control1 ON (tract1.county_office_control_identifier = county_office_control1.county_office_control_identifier)
LEFT JOIN farm_records_reporting.tract_year TY2 ON ( t.parent_tract_year_identifier = TY2.tract_year_identifier) 
JOIN farm_records_reporting.tract tract2 ON (TY2.tract_identifier = tract2.tract_identifier) 
JOIN farm_records_reporting.county_office_control county_office_control2 ON (tract2.county_office_control_identifier = county_office_control2.county_office_control_identifier)
JOIN farm_records_reporting.farm_year fy ON (fy.farm_year_identifier = TY2.farm_year_identifier) 
JOIN farm_records_reporting.time_period tp ON (tp.time_period_identifier = fy.time_period_identifier) 
where t.cdc_dt >= current_date - 1

