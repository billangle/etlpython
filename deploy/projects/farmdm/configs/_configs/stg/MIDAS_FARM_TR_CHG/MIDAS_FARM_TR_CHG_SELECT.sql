SELECT midas_farm_tract_change.creation_date As CRE_DT
   ,LTrim(RTrim(midas_farm_tract_change.creation_user_name) ) As CRE_USER_NM
   ,LTrim(RTrim(midas_farm_tract_change.data_status_code) ) As DATA_STAT_CD
   ,midas_farm_tract_change.farm_change_type_identifier As FARM_CHG_TYPE_ID
   ,midas_farm_tract_change.last_change_date As LAST_CHG_DT
   ,LTrim(RTrim(midas_farm_tract_change.last_change_user_name) ) As LAST_CHG_USER_NM
   ,midas_farm_tract_change.midas_farm_tract_change_identifier As MIDAS_FARM_TR_CHG_ID
   ,LTrim(RTrim(midas_farm_tract_change.parent_county_fsa_code) ) As PRNT_CNTY_FSA_CD
   ,LTrim(RTrim(midas_farm_tract_change.parent_farm_number) ) As PRNT_FARM_NBR
   ,LTrim(RTrim(midas_farm_tract_change.parent_state_fsa_code) ) As PRNT_ST_FSA_CD
   ,LTrim(RTrim(midas_farm_tract_change.parent_tract_number) ) As PRNT_TR_NBR
   ,LTrim(RTrim(midas_farm_tract_change.resulting_county_fsa_code) ) As RSLT_CNTY_FSA_CD
   ,LTrim(RTrim(midas_farm_tract_change.resulting_farm_number) ) As RSLT_FARM_NBR
   ,LTrim(RTrim(midas_farm_tract_change.resulting_state_fsa_code) ) As RSLT_ST_FSA_CD
   ,LTrim(RTrim(midas_farm_tract_change.resulting_tract_number) ) As RSLT_TR_NBR
   ,midas_farm_tract_change.reconstitution_approval_date   AS RCON_APVL_DT
   ,midas_farm_tract_change.reconstitution_initiation_date AS RCON_INIT_DT
   ,LTrim(RTrim(midas_farm_tract_change.reconstitution_sequence_number)) AS RCON_SEQ_NBR
   ,midas_farm_tract_change.time_period_identifier AS TM_PRD_ID
    ,LTrim(RTrim(time_period.time_period_name)) AS TM_PRD_NM
FROM farm_records_reporting.midas_farm_tract_change
JOIN farm_records_reporting.time_period ON midas_farm_tract_change.time_period_identifier = time_period.time_period_identifier
	