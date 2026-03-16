-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT103_CNTY_PRAC_RT_CAT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT103_CNTY_PRAC_RT_CAT/incremental/CNSV_EWT103_CNTY_PRAC_RT_CAT.sql
-- =============================================================================

SELECT * FROM
(
SELECT DISTINCT ST_FIPS_CD,
CNTY_FIPS_CD,
SGNP_NBR,
SGNP_TYPE_DESC,
SGNP_STYPE_DESC,
SGNP_STYPE_AGR_NM,
CNSV_PRAC_CD,
PRAC_RT_CAT_DESC,
PRAC_MNT_RT,
CNTY_PRAC_RT_CAT_ID,
SGNP_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
-- op,
ROW_NUMBER() over ( partition by 
CNTY_PRAC_RT_CAT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
ewt103_cnty_prac_rt_cat.ST_FIPS_CD ST_FIPS_CD,
ewt103_cnty_prac_rt_cat.CNTY_FIPS_CD CNTY_FIPS_CD,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt103_cnty_prac_rt_cat.CNSV_PRAC_CD CNSV_PRAC_CD,
ewt103_cnty_prac_rt_cat.PRAC_RT_CAT_DESC PRAC_RT_CAT_DESC,
ewt103_cnty_prac_rt_cat.PRAC_MNT_RT PRAC_MNT_RT,
ewt103_cnty_prac_rt_cat.CNTY_PRAC_RT_CAT_ID CNTY_PRAC_RT_CAT_ID,
ewt103_cnty_prac_rt_cat.SGNP_ID SGNP_ID,
ewt103_cnty_prac_rt_cat.DATA_STAT_CD DATA_STAT_CD,
ewt103_cnty_prac_rt_cat.CRE_DT CRE_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_DT LAST_CHG_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS TBL_PRIORITY
FROM "fsa-prod-cnsv".EWT103_CNTY_PRAC_RT_CAT 
LEFT JOIN "fsa-prod-cnsv".EWT14SGNP on (EWT103_CNTY_PRAC_RT_CAT.SGNP_ID = EWT14SGNP.SGNP_ID)
-- WHERE __CDC_OPERATION <> 'D'

UNION
SELECT 
ewt103_cnty_prac_rt_cat.ST_FIPS_CD ST_FIPS_CD,
ewt103_cnty_prac_rt_cat.CNTY_FIPS_CD CNTY_FIPS_CD,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt103_cnty_prac_rt_cat.CNSV_PRAC_CD CNSV_PRAC_CD,
ewt103_cnty_prac_rt_cat.PRAC_RT_CAT_DESC PRAC_RT_CAT_DESC,
ewt103_cnty_prac_rt_cat.PRAC_MNT_RT PRAC_MNT_RT,
ewt103_cnty_prac_rt_cat.CNTY_PRAC_RT_CAT_ID CNTY_PRAC_RT_CAT_ID,
ewt103_cnty_prac_rt_cat.SGNP_ID SGNP_ID,
ewt103_cnty_prac_rt_cat.DATA_STAT_CD DATA_STAT_CD,
ewt103_cnty_prac_rt_cat.CRE_DT CRE_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_DT LAST_CHG_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
2 AS TBL_PRIORITY
FROM "fsa-prod-cnsv".EWT14SGNP 
JOIN "fsa-prod-cnsv".EWT103_CNTY_PRAC_RT_CAT on (EWT14SGNP.SGNP_ID = EWT103_CNTY_PRAC_RT_CAT.SGNP_ID)
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
ewt103_cnty_prac_rt_cat.ST_FIPS_CD ST_FIPS_CD,
ewt103_cnty_prac_rt_cat.CNTY_FIPS_CD CNTY_FIPS_CD,
NULL SGNP_NBR,
NULL SGNP_TYPE_DESC,
NULL SGNP_STYPE_DESC,
NULL SGNP_STYPE_AGR_NM,
ewt103_cnty_prac_rt_cat.CNSV_PRAC_CD CNSV_PRAC_CD,
ewt103_cnty_prac_rt_cat.PRAC_RT_CAT_DESC PRAC_RT_CAT_DESC,
ewt103_cnty_prac_rt_cat.PRAC_MNT_RT PRAC_MNT_RT,
ewt103_cnty_prac_rt_cat.CNTY_PRAC_RT_CAT_ID CNTY_PRAC_RT_CAT_ID,
ewt103_cnty_prac_rt_cat.SGNP_ID SGNP_ID,
ewt103_cnty_prac_rt_cat.DATA_STAT_CD DATA_STAT_CD,
ewt103_cnty_prac_rt_cat.CRE_DT CRE_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_DT LAST_CHG_DT,
ewt103_cnty_prac_rt_cat.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- 'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt103_cnty_prac_rt_cat
-- WHERE __CDC_OPERATION = 'D'