-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT83SCCRP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT83SCCRP/incremental/CNSV_EWT83SCCRP.sql
-- =============================================================================

SELECT DISTINCT
ewt83sccrp.FSA_MULT_CROP_CD FSA_MULT_CROP_CD,
ewt83sccrp.FSA_MULT_CROP_NM FSA_MULT_CROP_NM,
ewt83sccrp.ST_FIPS_CD ST_FIPS_CD,
ewt83sccrp.CNTY_FIPS_CD CNTY_FIPS_CD,
ewt83sccrp.energy_crop_cat_cd ENERGY_CROP_CAT_CD,
ewt83sccrp.DATA_STAT_CD DATA_STAT_CD,
ewt83sccrp.CRE_DT CRE_DT,
ewt83sccrp.LAST_CHG_DT LAST_CHG_DT,
ewt83sccrp.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt83sccrp
--WHERE __CDC_OPERATION <> 'D'

UNION
SELECT DISTINCT
ewt83sccrp.FSA_MULT_CROP_CD FSA_MULT_CROP_CD,
ewt83sccrp.FSA_MULT_CROP_NM FSA_MULT_CROP_NM,
ewt83sccrp.ST_FIPS_CD ST_FIPS_CD,
ewt83sccrp.CNTY_FIPS_CD CNTY_FIPS_CD,
ewt83sccrp.energy_crop_cat_cd ENERGY_CROP_CAT_CD,
ewt83sccrp.DATA_STAT_CD DATA_STAT_CD,
ewt83sccrp.CRE_DT CRE_DT,
ewt83sccrp.LAST_CHG_DT LAST_CHG_DT,
ewt83sccrp.LAST_CHG_USER_NM LAST_CHG_USER_NM,
-- op,
1 AS row_num_part
FROM "fsa-prod-cnsv".ewt83sccrp
-- WHERE __CDC_OPERATION = 'D'