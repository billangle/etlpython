SELECT distinct r.reconstitution_identifier As RCON_ID,
r.county_office_control_identifier As CNTY_OFC_CTL_ID,
r.time_period_identifier As TM_PRD_ID,
LTrim(RTrim(r.reconstitution_prefix_code)) As RCON_PFX_CD,
LTrim(RTrim(r.reconstitution_sequence_number) ) As RCON_SEQ_NBR,
LTrim(RTrim(r.reconstitution_type_code)) As RCON_TYPE_CD,
r.reconstitution_approval_date As RCON_APVL_DT,
r.reconstitution_effective_date As RCON_EFF_DT,
LTrim(RTrim(c.state_fsa_code)) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code)) As CNTY_FSA_CD,
CAST(t.time_period_name AS numeric(4)) as PGM_YR,
LTrim(RTrim(r.data_status_code)) As DATA_STAT_CD,
r.creation_date As CRE_DT,
r.last_change_date As LAST_CHG_DT,
LTrim(RTrim(r.last_change_user_name)) As LAST_CHG_USER_NM,
r.reconstitution_initiation_date As RCON_INIT_DT,
''  as hash_dif,
r.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
r.cdc_dt as CDC_DT
from farm_records_reporting.reconstitution r 
left join farm_records_reporting.county_office_control c on r.county_office_control_identifier = c.county_office_control_identifier
join farm_records_reporting.time_period t on r.time_period_identifier = t.time_period_identifier
where r.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'