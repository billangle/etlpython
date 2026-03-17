SELECT DISTINCT
component_group.CPNT_GRP_ID CPNT_GRP_ID,
component_group.ST_FSA_CD ST_FSA_CD,
component_group.CNTY_FSA_CD CNTY_FSA_CD,
component_group.CPNT_GRP_NM CPNT_GRP_NM,
component_group.DATA_STAT_CD DATA_STAT_CD,
component_group.CRE_DT CRE_DT,
component_group.LAST_CHG_DT LAST_CHG_DT,
component_group.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM component_group
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
component_group.CPNT_GRP_ID CPNT_GRP_ID,
component_group.ST_FSA_CD ST_FSA_CD,
component_group.CNTY_FSA_CD CNTY_FSA_CD,
component_group.CPNT_GRP_NM CPNT_GRP_NM,
component_group.DATA_STAT_CD DATA_STAT_CD,
component_group.CRE_DT CRE_DT,
component_group.LAST_CHG_DT LAST_CHG_DT,
component_group.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM component_group
--WHERE OP = 'D'