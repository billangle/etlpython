-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT94NEBI (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT94NEBI/incremental/CNSV_EWT94NEBI.sql
-- =============================================================================

SELECT DISTINCT
ewt94nebi.NTL_EBI_NM NTL_EBI_NM,
ewt94nebi.NTL_EBI_FCTR_ID NTL_EBI_FCTR_ID,
ewt94nebi.NTL_EBI_SFCTR_ID NTL_EBI_SFCTR_ID,
ewt94nebi.NTL_EBI_DESC NTL_EBI_DESC,
ewt94nebi.PRAC_BASE_IND PRAC_BASE_IND,
ewt94nebi.DATA_STAT_CD DATA_STAT_CD,
ewt94nebi.CRE_DT CRE_DT,
ewt94nebi.LAST_CHG_DT LAST_CHG_DT,
ewt94nebi.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt94nebi
-- WHERE __CDC_OPERATION <> 'D'

UNION
SELECT DISTINCT
ewt94nebi.NTL_EBI_NM NTL_EBI_NM,
ewt94nebi.NTL_EBI_FCTR_ID NTL_EBI_FCTR_ID,
ewt94nebi.NTL_EBI_SFCTR_ID NTL_EBI_SFCTR_ID,
ewt94nebi.NTL_EBI_DESC NTL_EBI_DESC,
ewt94nebi.PRAC_BASE_IND PRAC_BASE_IND,
ewt94nebi.DATA_STAT_CD DATA_STAT_CD,
ewt94nebi.CRE_DT CRE_DT,
ewt94nebi.LAST_CHG_DT LAST_CHG_DT,
ewt94nebi.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- 'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt94nebi
-- WHERE __CDC_OPERATION = 'D'
