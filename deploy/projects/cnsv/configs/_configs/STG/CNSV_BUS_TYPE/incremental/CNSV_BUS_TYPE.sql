SELECT DISTINCT
business_type.BUS_TYPE_DESC BUS_TYPE_DESC,
business_type.BUS_TYPE_ID BUS_TYPE_ID,
business_type.BUS_TYPE_CD BUS_TYPE_CD,
business_type.ELG_RQR_IND ELG_RQR_IND,
business_type.PYMT_LMT_RQR_IND PYMT_LMT_RQR_IND,
business_type.AGI_RQR_IND AGI_RQR_IND,
business_type.CMB_ENTY_RQR_IND CMB_ENTY_RQR_IND,
business_type.DATA_STAT_CD DATA_STAT_CD,
business_type.CRE_DT CRE_DT,
business_type.LAST_CHG_DT LAST_CHG_DT,
business_type.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM business_type
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
business_type.BUS_TYPE_DESC BUS_TYPE_DESC,
business_type.BUS_TYPE_ID BUS_TYPE_ID,
business_type.BUS_TYPE_CD BUS_TYPE_CD,
business_type.ELG_RQR_IND ELG_RQR_IND,
business_type.PYMT_LMT_RQR_IND PYMT_LMT_RQR_IND,
business_type.AGI_RQR_IND AGI_RQR_IND,
business_type.CMB_ENTY_RQR_IND CMB_ENTY_RQR_IND,
business_type.DATA_STAT_CD DATA_STAT_CD,
business_type.CRE_DT CRE_DT,
business_type.LAST_CHG_DT LAST_CHG_DT,
business_type.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM business_type
--WHERE OP = 'D'