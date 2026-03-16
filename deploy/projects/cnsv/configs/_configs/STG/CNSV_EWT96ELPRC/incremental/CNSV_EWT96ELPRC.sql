-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT96ELPRC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT96ELPRC/incremental/CNSV_EWT96ELPRC.sql
-- =============================================================================

SELECT DISTINCT
ewt96elprc.ST_FSA_CD ST_FSA_CD,
ewt96elprc.CNTY_FSA_CD CNTY_FSA_CD,
ewt96elprc.CNSV_PRAC_CD CNSV_PRAC_CD,
ewt96elprc.COST_SHR_ACRE_RT COST_SHR_ACRE_RT_AMT,
ewt96elprc.DATA_STAT_CD DATA_STAT_CD,
ewt96elprc.CRE_DT CRE_DT,
ewt96elprc.LAST_CHG_DT LAST_CHG_DT,
ewt96elprc.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt96elprc
--WHERE __CDC_OPERATION <> 'D'

UNION
SELECT DISTINCT
ewt96elprc.ST_FSA_CD ST_FSA_CD,
ewt96elprc.CNTY_FSA_CD CNTY_FSA_CD,
ewt96elprc.CNSV_PRAC_CD CNSV_PRAC_CD,
ewt96elprc.COST_SHR_ACRE_RT COST_SHR_ACRE_RT_AMT,
ewt96elprc.DATA_STAT_CD DATA_STAT_CD,
ewt96elprc.CRE_DT CRE_DT,
ewt96elprc.LAST_CHG_DT LAST_CHG_DT,
ewt96elprc.LAST_CHG_USER_NM LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt96elprc
-- WHERE __CDC_OPERATION = 'D'
