SELECT DISTINCT
component_sub_category.CPNT_SUB_CAT_CD CPNT_SUB_CAT_CD,
component_sub_category.CPNT_SUB_CAT_ID CPNT_SUB_CAT_ID,
component_sub_category.CPNT_SUB_CAT_NM CPNT_SUB_CAT_NM,
component_sub_category.DATA_STAT_CD DATA_STAT_CD,
component_sub_category.CRE_DT CRE_DT,
component_sub_category.LAST_CHG_DT LAST_CHG_DT,
component_sub_category.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM component_sub_category
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
component_sub_category.CPNT_SUB_CAT_CD CPNT_SUB_CAT_CD,
component_sub_category.CPNT_SUB_CAT_ID CPNT_SUB_CAT_ID,
component_sub_category.CPNT_SUB_CAT_NM CPNT_SUB_CAT_NM,
component_sub_category.DATA_STAT_CD DATA_STAT_CD,
component_sub_category.CRE_DT CRE_DT,
component_sub_category.LAST_CHG_DT LAST_CHG_DT,
component_sub_category.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM component_sub_category
--WHERE OP = 'D'