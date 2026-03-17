SELECT DISTINCT
component_category.CPNT_CAT_NM CPNT_CAT_NM,
component_category.CPNT_CAT_ID CPNT_CAT_ID,
component_category.CPNT_CAT_CD CPNT_CAT_CD,
component_category.DATA_STAT_CD DATA_STAT_CD,
component_category.CRE_DT CRE_DT,
component_category.LAST_CHG_DT LAST_CHG_DT,
component_category.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM component_category
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
component_category.CPNT_CAT_NM CPNT_CAT_NM,
component_category.CPNT_CAT_ID CPNT_CAT_ID,
component_category.CPNT_CAT_CD CPNT_CAT_CD,
component_category.DATA_STAT_CD DATA_STAT_CD,
component_category.CRE_DT CRE_DT,
component_category.LAST_CHG_DT LAST_CHG_DT,
component_category.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM component_category
--WHERE OP = 'D'