-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_AFL_INIT_CAT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_AFL_INIT_CAT/incremental/CNSV_AFL_INIT_CAT.sql
-- =============================================================================

SELECT * FROM
(
SELECT DISTINCT AFL_INIT_NM,
AFL_INIT_CAT_NM,
AFL_INIT_ID,
AFL_INIT_CAT_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
CRE_USER_NM,
LAST_CHG_USER_NM,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
AFL_INIT_CAT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
affiliated_initiative_category.afl_init_id AFL_INIT_ID,
affiliated_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
affiliated_initiative_category.data_stat_cd DATA_STAT_CD,
affiliated_initiative_category.cre_dt CRE_DT,
affiliated_initiative_category.last_chg_dt LAST_CHG_DT,
affiliated_initiative_category.cre_user_nm CRE_USER_NM,
affiliated_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
AFFILIATED_INITIATIVE_CATEGORY.op,
1 AS TBL_PRIORITY
FROM  "fsa-{env}-cnsv-cdc".AFFILIATED_INITIATIVE_CATEGORY LEFT JOIN "fsa-{env}-cnsv-cdc".AFFILIATED_INITIATIVE ON ( AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_ID = AFFILIATED_INITIATIVE.AFL_INIT_ID)
WHERE AFFILIATED_INITIATIVE_CATEGORY.op <> 'D'

UNION
SELECT 
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
affiliated_initiative_category.afl_init_id AFL_INIT_ID,
affiliated_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
affiliated_initiative_category.data_stat_cd DATA_STAT_CD,
affiliated_initiative_category.cre_dt CRE_DT,
affiliated_initiative_category.last_chg_dt LAST_CHG_DT,
affiliated_initiative_category.cre_user_nm CRE_USER_NM,
affiliated_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
AFFILIATED_INITIATIVE.op,
2 AS TBL_PRIORITY
FROM  "fsa-{env}-cnsv-cdc".AFFILIATED_INITIATIVE  JOIN "fsa-{env}-cnsv-cdc".AFFILIATED_INITIATIVE_CATEGORYON ( AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_ID = AFFILIATED_INITIATIVE.AFL_INIT_ID)
WHERE AFFILIATED_INITIATIVE.op <> 'D'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND affiliated_initiative_category.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT DISTINCT
NULL AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
affiliated_initiative_category.afl_init_id AFL_INIT_ID,
affiliated_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
affiliated_initiative_category.data_stat_cd DATA_STAT_CD,
affiliated_initiative_category.cre_dt CRE_DT,
affiliated_initiative_category.last_chg_dt LAST_CHG_DT,
affiliated_initiative_category.cre_user_nm CRE_USER_NM,
affiliated_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".affiliated_initiative_category
WHERE affiliated_initiative_category.op = 'D'