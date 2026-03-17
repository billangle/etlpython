SELECT DISTINCT
conservation_event_master.BUS_EVNT_NM BUS_EVNT_NM,
conservation_event_master.BUS_EVNT_DESC BUS_EVNT_DESC,
conservation_event_master.BUS_EVNT_PRPS_DESC BUS_EVNT_PRPS_DESC,
conservation_event_master.CNSV_EVNT_MST_ID CNSV_EVNT_MST_ID,
conservation_event_master.DATA_STAT_CD DATA_STAT_CD,
conservation_event_master.CRE_DT CRE_DT,
conservation_event_master.LAST_CHG_DT LAST_CHG_DT,
conservation_event_master.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM conservation_event_master
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
conservation_event_master.BUS_EVNT_NM BUS_EVNT_NM,
conservation_event_master.BUS_EVNT_DESC BUS_EVNT_DESC,
conservation_event_master.BUS_EVNT_PRPS_DESC BUS_EVNT_PRPS_DESC,
conservation_event_master.CNSV_EVNT_MST_ID CNSV_EVNT_MST_ID,
conservation_event_master.DATA_STAT_CD DATA_STAT_CD,
conservation_event_master.CRE_DT CRE_DT,
conservation_event_master.LAST_CHG_DT LAST_CHG_DT,
conservation_event_master.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM conservation_event_master
--WHERE OP = 'D'