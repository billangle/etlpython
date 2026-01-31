INSERT INTO sql_farm_rcd_stg.rcon
(rcon_id, cnty_ofc_ctl_id, tm_prd_id, rcon_pfx_cd, rcon_seq_nbr,
rcon_type_cd, rcon_apvl_dt, rcon_eff_dt, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt, 
last_chg_user_nm, rcon_init_dt, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)

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
CAST(current_date-1 as date) as CDC_DT
from farm_records_reporting.reconstitution r 
left join farm_records_reporting.county_office_control c on r.county_office_control_identifier = c.county_office_control_identifier
join farm_records_reporting.time_period t on r.time_period_identifier = t.time_period_identifier
where r.cdc_dt >= current_date - 1