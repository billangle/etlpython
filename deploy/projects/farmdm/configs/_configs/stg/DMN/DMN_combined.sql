INSERT INTO sql_farm_rcd_stg.dmn
(dmn_id, dmn_nm, dmn_desc, cre_dt, last_chg_dt, 
last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)
SELECT
domain_identifier As DMN_ID,
LTrim(RTrim(domain_name) ) As DMN_NM,
LTrim(RTrim(domain_description) ) As DMN_DESC,
creation_date As CRE_DT,
last_change_date As LAST_CHG_DT,
LTrim(RTrim(last_change_user_name) ) As LAST_CHG_USER_NM,
''  as hash_dif,
'I' as cdc_oper_cd,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
CAST((current_date - 1) as date) as cdc_dt
FROM farm_records_reporting.domain
where domain.cdc_dt >= current_date - 1