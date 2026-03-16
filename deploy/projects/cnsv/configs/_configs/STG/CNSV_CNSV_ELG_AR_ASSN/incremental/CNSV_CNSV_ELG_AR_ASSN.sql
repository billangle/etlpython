-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_ELG_AR_ASSN (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_ELG_AR_ASSN/incremental/CNSV_CNSV_ELG_AR_ASSN.sql
-- =============================================================================
SELECT * FROM
(
SELECT DISTINCT SGNP_NBR,
SGNP_TYPE_DESC,
SGNP_STYPE_DESC,
SGNP_STYPE_AGR_NM,
CNSV_ELG_AR_NM,
CNSV_ELG_AR_MIN_PCT,
SGNP_ID,
CNSV_ELG_AR_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
SGNP_ID,
CNSV_ELG_AR_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
conservation_eligibility_area.CNSV_ELG_AREA_NM CNSV_ELG_AR_NM,
conservation_eligibility_area_association.CNSV_ELG_AREA_MIN CNSV_ELG_AR_MIN_PCT,
conservation_eligibility_area_association.SGNP_ID SGNP_ID,
conservation_eligibility_area_association.CNSV_ELG_AREA_ID CNSV_ELG_AR_ID,
conservation_eligibility_area_association.DATA_STAT_CD DATA_STAT_CD,
conservation_eligibility_area_association.CRE_DT CRE_DT,
conservation_eligibility_area_association.LAST_CHG_DT LAST_CHG_DT,
conservation_eligibility_area_association.LAST_CHG_USER_NM LAST_CHG_USER_NM,
CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.op,
1 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION LEFT JOIN "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.CNSV_ELG_AREA_ID = CONSERVATION_ELIGIBILITY_AREA.CNSV_ELG_AREA_ID) LEFT JOIN "fsa-{env}-cnsv-cdc".EWT14SGNP ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.SGNP_ID = EWT14SGNP.SGNP_ID) 
WHERE CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.op <> 'D'

UNION
SELECT 
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
conservation_eligibility_area.CNSV_ELG_AREA_NM CNSV_ELG_AR_NM,
conservation_eligibility_area_association.CNSV_ELG_AREA_MIN CNSV_ELG_AR_MIN_PCT,
conservation_eligibility_area_association.SGNP_ID SGNP_ID,
conservation_eligibility_area_association.CNSV_ELG_AREA_ID CNSV_ELG_AR_ID,
conservation_eligibility_area_association.DATA_STAT_CD DATA_STAT_CD,
conservation_eligibility_area_association.CRE_DT CRE_DT,
conservation_eligibility_area_association.LAST_CHG_DT LAST_CHG_DT,
conservation_eligibility_area_association.LAST_CHG_USER_NM LAST_CHG_USER_NM,
CONSERVATION_ELIGIBILITY_AREA.op,
2 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA JOIN "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.CNSV_ELG_AREA_ID = CONSERVATION_ELIGIBILITY_AREA.CNSV_ELG_AREA_ID) LEFT JOIN "fsa-{env}-cnsv-cdc".EWT14SGNP ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.SGNP_ID = EWT14SGNP.SGNP_ID) 
WHERE CONSERVATION_ELIGIBILITY_AREA.op <> 'D'

UNION
SELECT 
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
conservation_eligibility_area.CNSV_ELG_AREA_NM CNSV_ELG_AR_NM,
conservation_eligibility_area_association.CNSV_ELG_AREA_MIN CNSV_ELG_AR_MIN_PCT,
conservation_eligibility_area_association.SGNP_ID SGNP_ID,
conservation_eligibility_area_association.CNSV_ELG_AREA_ID CNSV_ELG_AR_ID,
conservation_eligibility_area_association.DATA_STAT_CD DATA_STAT_CD,
conservation_eligibility_area_association.CRE_DT CRE_DT,
conservation_eligibility_area_association.LAST_CHG_DT LAST_CHG_DT,
conservation_eligibility_area_association.LAST_CHG_USER_NM LAST_CHG_USER_NM,
EWT14SGNP.op,
3 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc".EWT14SGNP JOIN "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.SGNP_ID = EWT14SGNP.SGNP_ID) LEFT JOIN "fsa-{env}-cnsv-cdc".CONSERVATION_ELIGIBILITY_AREA ON (CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.CNSV_ELG_AREA_ID = CONSERVATION_ELIGIBILITY_AREA.CNSV_ELG_AREA_ID) WHERE EWT14SGNP.op <> 'D'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND CONSERVATION_ELIGIBILITY_AREA_ASSOCIATION.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT DISTINCT
NULL SGNP_NBR,
NULL SGNP_TYPE_DESC,
NULL SGNP_STYPE_DESC,
NULL SGNP_STYPE_AGR_NM,
NULL CNSV_ELG_AR_NM,
conservation_eligibility_area_association.CNSV_ELG_AREA_MIN CNSV_ELG_AR_MIN_PCT,
conservation_eligibility_area_association.SGNP_ID SGNP_ID,
conservation_eligibility_area_association.CNSV_ELG_AREA_ID CNSV_ELG_AR_ID,
conservation_eligibility_area_association.DATA_STAT_CD DATA_STAT_CD,
conservation_eligibility_area_association.CRE_DT CRE_DT,
conservation_eligibility_area_association.LAST_CHG_DT LAST_CHG_DT,
conservation_eligibility_area_association.LAST_CHG_USER_NM LAST_CHG_USER_NM,
'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".conservation_eligibility_area_association
WHERE conservation_eligibility_area_association.op = 'D'