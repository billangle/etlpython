SELECT clu_producer_year.clu_producer_year_identifier As CLU_PRDR_YR_ID,
clu_producer_year.clu_year_identifier As CLU_YR_ID,
clu_producer_year.core_customer_identifier As CORE_CUST_ID,
clu_producer_year.producer_involvement_code As PRDR_INVL_CD,
clu_producer_year.clu_producer_hel_exception_code As CLU_PRDR_HEL_EXCP_CD,
clu_producer_year.clu_producer_cw_exception_code As CLU_PRDR_CW_EXCP_CD,
clu_producer_year.clu_producer_pcw_exception_code As CLU_PRDR_PCW_EXCP_CD,
LTrim(RTrim(farm.farm_number) ) As FARM_NBR,
LTrim(RTrim(tract.tract_number) ) As TR_NBR,
LTrim(RTrim(county_office_control.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(county_office_control.county_fsa_code) ) As CNTY_FSA_CD,
cast(time_period.time_period_name as int) As PGM_YR,
LTrim(RTrim(clu_producer_year.data_status_code) ) As DATA_STAT_CD,
clu_producer_year.creation_date As CRE_DT,
clu_producer_year.last_change_date As LAST_CHG_DT,
LTrim(RTrim(clu_producer_year.last_change_user_name) ) As LAST_CHG_USER_NM,
''  as hash_dif,
clu_producer_year.cdc_oper_cd as cdc_oper_cd,
CAST(current_date as date) as load_dt,
'SQL_FARM_RCD' as data_src_nm,
clu_producer_year.cdc_dt as cdc_dt,
clu_producer_year.hel_appeals_exhausted_date As HEL_APLS_EXHST_DT, 
clu_producer_year.cw_appeals_exhausted_date As CW_APLS_EXHST_DT,
clu_producer_year.pcw_appeals_exhausted_date As PCW_APLS_EXHST_DT,
clu_producer_year.clu_producer_rma_hel_exception_code As CLU_PRDR_RMA_HEL_EXCP_CD,
clu_producer_year.clu_producer_rma_cw_exception_code As CLU_PRDR_RMA_CW_EXCP_CD,
clu_producer_year.clu_producer_rma_pcw_exception_code As CLU_PRDR_RMA_PCW_EXCP_CD
FROM farm_records_reporting.clu_producer_year
LEFT JOIN farm_records_reporting.clu_year ON (clu_producer_year.clu_year_identifier = clu_year.clu_year_identifier) 
JOIN farm_records_reporting.tract_year ON(clu_year.tract_year_identifier =tract_year.tract_year_identifier) 
JOIN farm_records_reporting.farm_year ON (tract_year.farm_year_identifier = farm_year.farm_year_identifier) 
JOIN farm_records_reporting.farm ON (farm_year.farm_identifier=farm.farm_identifier) 
JOIN farm_records_reporting.county_office_control ON (farm.county_office_control_identifier =county_office_control.county_office_control_identifier )
JOIN farm_records_reporting.tract ON (tract_year.tract_identifier=tract.tract_identifier) 
JOIN farm_records_reporting.time_period ON (farm_year.time_period_identifier=time_period.time_period_identifier)
where clu_producer_year.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'