-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_FLAT_RT_PRAC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_FLAT_RT_PRAC/incremental/CNSV_FLAT_RT_PRAC.sql
-- =============================================================================

SELECT * FROM
(
SELECT DISTINCT FLAT_RT_PRAC_ID,
CNSV_PRAC_ID,
GVT_LVL_ID,
FLAT_RT_PRAC_AVG,
EFF_PRD_STRT_DT,
EFF_PRD_END_DT,
ST_FSA_CD,
CNTY_FSA_CD,
REG_COST_SHR_PCT,
LTD_COST_SHR_PCT,
GVT_LVL_DESC,
CNSV_PRAC_CD,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
-- op,
ROW_NUMBER() over ( partition by FLAT_RT_PRAC_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
flat_rate_practice.FLAT_RT_PRAC_ID FLAT_RT_PRAC_ID,
flat_rate_practice.CNSV_PRAC_ID CNSV_PRAC_ID,
flat_rate_practice.GOVT_LVL_ID GVT_LVL_ID,
flat_rate_practice.FLAT_RT_PRAC_AVG FLAT_RT_PRAC_AVG,
flat_rate_practice.EFF_PRD_STRT_DT EFF_PRD_STRT_DT,
flat_rate_practice.EFF_PRD_END_DT EFF_PRD_END_DT,
flat_rate_practice.ST_FSA_CD ST_FSA_CD,
flat_rate_practice.CNTY_FSA_CD CNTY_FSA_CD,
flat_rate_practice.REG_COST_SHR_PCT REG_COST_SHR_PCT,
flat_rate_practice.LTD_COST_SHR_PCT LTD_COST_SHR_PCT,
government_level.GVT_LVL_DESC GVT_LVL_DESC,
CONSERVATION_PRACTICE.CNSV_PRAC_CD CNSV_PRAC_CD,
flat_rate_practice.DATA_STAT_CD DATA_STAT_CD,
flat_rate_practice.CRE_DT CRE_DT,
flat_rate_practice.LAST_CHG_DT LAST_CHG_DT,
flat_rate_practice.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS TBL_PRIORITY
FROM "fsa-prod-cnsv".FLAT_RATE_PRACTICE  
  LEFT JOIN "fsa-prod-cnsv".GOVERNMENT_LEVEL 
  on ( FLAT_RATE_PRACTICE.GOVT_LVL_ID=GOVERNMENT_LEVEL.GOVT_LVL_ID)
  Left join "fsa-prod-cnsv".CONSERVATION_PRACTICE
  on (FLAT_RATE_PRACTICE.CNSV_PRAC_ID=CONSERVATION_PRACTICE.CNSV_PRAC_ID)
-- WHERE __CDC_OPERATION <> 'D'

UNION
SELECT 
flat_rate_practice.FLAT_RT_PRAC_ID FLAT_RT_PRAC_ID,
flat_rate_practice.CNSV_PRAC_ID CNSV_PRAC_ID,
flat_rate_practice.GOVT_LVL_ID GVT_LVL_ID,
flat_rate_practice.FLAT_RT_PRAC_AVG FLAT_RT_PRAC_AVG,
flat_rate_practice.EFF_PRD_STRT_DT EFF_PRD_STRT_DT,
flat_rate_practice.EFF_PRD_END_DT EFF_PRD_END_DT,
flat_rate_practice.ST_FSA_CD ST_FSA_CD,
flat_rate_practice.CNTY_FSA_CD CNTY_FSA_CD,
flat_rate_practice.REG_COST_SHR_PCT REG_COST_SHR_PCT,
flat_rate_practice.LTD_COST_SHR_PCT LTD_COST_SHR_PCT,
government_level.GVT_LVL_DESC GVT_LVL_DESC,
CONSERVATION_PRACTICE.CNSV_PRAC_CD CNSV_PRAC_CD,
flat_rate_practice.DATA_STAT_CD DATA_STAT_CD,
flat_rate_practice.CRE_DT CRE_DT,
flat_rate_practice.LAST_CHG_DT LAST_CHG_DT,
flat_rate_practice.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
2 AS TBL_PRIORITY
FROM "fsa-prod-cnsv".GOVERNMENT_LEVEL
  JOIN "fsa-prod-cnsv".FLAT_RATE_PRACTICE 
  on ( FLAT_RATE_PRACTICE.GOVT_LVL_ID=GOVERNMENT_LEVEL.GOVT_LVL_ID)
  Left join "fsa-prod-cnsv".CONSERVATION_PRACTICE
on (FLAT_RATE_PRACTICE.CNSV_PRAC_ID=CONSERVATION_PRACTICE.CNSV_PRAC_ID)
-- WHERE __CDC_OPERATION <> 'D'

UNION
SELECT 
flat_rate_practice.FLAT_RT_PRAC_ID FLAT_RT_PRAC_ID,
flat_rate_practice.CNSV_PRAC_ID CNSV_PRAC_ID,
flat_rate_practice.GOVT_LVL_ID GVT_LVL_ID,
flat_rate_practice.FLAT_RT_PRAC_AVG FLAT_RT_PRAC_AVG,
flat_rate_practice.EFF_PRD_STRT_DT EFF_PRD_STRT_DT,
flat_rate_practice.EFF_PRD_END_DT EFF_PRD_END_DT,
flat_rate_practice.ST_FSA_CD ST_FSA_CD,
flat_rate_practice.CNTY_FSA_CD CNTY_FSA_CD,
flat_rate_practice.REG_COST_SHR_PCT REG_COST_SHR_PCT,
flat_rate_practice.LTD_COST_SHR_PCT LTD_COST_SHR_PCT,
government_level.GVT_LVL_DESC GVT_LVL_DESC,
CONSERVATION_PRACTICE.CNSV_PRAC_CD CNSV_PRAC_CD,
flat_rate_practice.DATA_STAT_CD DATA_STAT_CD,
flat_rate_practice.CRE_DT CRE_DT,
flat_rate_practice.LAST_CHG_DT LAST_CHG_DT,
flat_rate_practice.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
3 AS TBL_PRIORITY
FROM "fsa-prod-cnsv".CONSERVATION_PRACTICE
   JOIN "fsa-prod-cnsv".FLAT_RATE_PRACTICE
   on (FLAT_RATE_PRACTICE.CNSV_PRAC_ID=CONSERVATION_PRACTICE.CNSV_PRAC_ID)
  LEFT JOIN "fsa-prod-cnsv".GOVERNMENT_LEVEL
    on ( FLAT_RATE_PRACTICE.GOVT_LVL_ID=GOVERNMENT_LEVEL.GOVT_LVL_ID)
-- WHERE __CDC_OPERATION <> 'D'
) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('2025-07-01' AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('2025-07-01' AS DATE))
)

UNION
SELECT DISTINCT
flat_rate_practice.FLAT_RT_PRAC_ID FLAT_RT_PRAC_ID,
flat_rate_practice.CNSV_PRAC_ID CNSV_PRAC_ID,
flat_rate_practice.GOVT_LVL_ID GVT_LVL_ID,
flat_rate_practice.FLAT_RT_PRAC_AVG FLAT_RT_PRAC_AVG,
flat_rate_practice.EFF_PRD_STRT_DT EFF_PRD_STRT_DT,
flat_rate_practice.EFF_PRD_END_DT EFF_PRD_END_DT,
flat_rate_practice.ST_FSA_CD ST_FSA_CD,
flat_rate_practice.CNTY_FSA_CD CNTY_FSA_CD,
flat_rate_practice.REG_COST_SHR_PCT REG_COST_SHR_PCT,
flat_rate_practice.LTD_COST_SHR_PCT LTD_COST_SHR_PCT,
NULL GVT_LVL_DESC,
NULL CNSV_PRAC_CD,
flat_rate_practice.DATA_STAT_CD DATA_STAT_CD,
flat_rate_practice.CRE_DT CRE_DT,
flat_rate_practice.LAST_CHG_DT LAST_CHG_DT,
flat_rate_practice.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- 'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-prod-cnsv".flat_rate_practice
-- WHERE __CDC_OPERATION = 'D'
