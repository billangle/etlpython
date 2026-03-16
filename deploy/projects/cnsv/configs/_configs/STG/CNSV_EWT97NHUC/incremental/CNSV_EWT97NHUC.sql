-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT97NHUC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT97NHUC/incremental/CNSV_EWT97NHUC.sql
-- =============================================================================

SELECT DISTINCT
ewt97nhuc.NTL_HUC_CD NTL_HUC_CD,
ewt97nhuc.DATA_STAT_CD DATA_STAT_CD,
ewt97nhuc.CRE_DT CRE_DT,
ewt97nhuc.LAST_CHG_DT LAST_CHG_DT,
ewt97nhuc.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt97nhuc
--WHERE __CDC_OPERATION <> 'D'

UNION
SELECT DISTINCT
ewt97nhuc.NTL_HUC_CD NTL_HUC_CD,
ewt97nhuc.DATA_STAT_CD DATA_STAT_CD,
ewt97nhuc.CRE_DT CRE_DT,
ewt97nhuc.LAST_CHG_DT LAST_CHG_DT,
ewt97nhuc.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt97nhuc
--WHERE __CDC_OPERATION = 'D'
