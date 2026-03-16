SELECT CNSV_PYMT_TYPE_NM,
PYMT_IMPL_ID,
PGM_HRCH_LVL_ID,
CNSV_PYMT_TYPE_DESC,
OO_CLS_FULL_NM,
PGM_SHRT_NM,
HRCH_LVL_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_USER_NM,
LAST_CHG_DT,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT FROM ( SELECT * FROM 
(
SELECT DISTINCT CNSV_PYMT_TYPE_NM,
PYMT_IMPL_ID,
PGM_HRCH_LVL_ID,
CNSV_PYMT_TYPE_DESC,
OO_CLS_FULL_NM,
PGM_SHRT_NM,
HRCH_LVL_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_USER_NM,
LAST_CHG_DT,
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
PYMT_IMPL_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
pymt_impl.cnsv_pymt_type_nm CNSV_PYMT_TYPE_NM,
pymt_impl.pymt_impl_id PYMT_IMPL_ID,
pymt_impl.pgm_hrch_lvl_id PGM_HRCH_LVL_ID,
pymt_impl.cnsv_pymt_type_desc CNSV_PYMT_TYPE_DESC,
pymt_impl.oo_cls_full_nm OO_CLS_FULL_NM,
pymt_impl.PGM_SHRT_NM PGM_SHRT_NM,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
pymt_impl.data_stat_cd DATA_STAT_CD,
pymt_impl.cre_dt CRE_DT,
pymt_impl.last_chg_user_nm LAST_CHG_USER_NM,
pymt_impl.last_chg_dt LAST_CHG_DT,
PYMT_IMPL.OP,
1 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."PYMT_IMPL" PYMT_IMPL 
LEFT JOIN "fsa-{env}-cnsv".PROGRAM_HIERARCHY_LEVEL 
ON (PYMT_IMPL.pgm_hrch_lvl_id = PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID)
WHERE PYMT_IMPL.OP <> 'D' AND
PYMT_IMPL.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT 
pymt_impl.cnsv_pymt_type_nm CNSV_PYMT_TYPE_NM,
pymt_impl.pymt_impl_id PYMT_IMPL_ID,
pymt_impl.pgm_hrch_lvl_id PGM_HRCH_LVL_ID,
pymt_impl.cnsv_pymt_type_desc CNSV_PYMT_TYPE_DESC,
pymt_impl.oo_cls_full_nm OO_CLS_FULL_NM,
pymt_impl.PGM_SHRT_NM PGM_SHRT_NM,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
pymt_impl.data_stat_cd DATA_STAT_CD,
pymt_impl.cre_dt CRE_DT,
pymt_impl.last_chg_user_nm LAST_CHG_USER_NM,
pymt_impl.last_chg_dt LAST_CHG_DT,
PROGRAM_HIERARCHY_LEVEL.OP,
2 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."PROGRAM_HIERARCHY_LEVEL" PROGRAM_HIERARCHY_LEVEL 
 JOIN "fsa-{env}-cnsv".PYMT_IMPL 
ON (PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID = PYMT_IMPL.pgm_hrch_lvl_id)
WHERE PROGRAM_HIERARCHY_LEVEL.OP <> 'D' AND
PROGRAM_HIERARCHY_LEVEL.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
SELECT DISTINCT
pymt_impl.cnsv_pymt_type_nm CNSV_PYMT_TYPE_NM,
pymt_impl.pymt_impl_id PYMT_IMPL_ID,
pymt_impl.pgm_hrch_lvl_id PGM_HRCH_LVL_ID,
pymt_impl.cnsv_pymt_type_desc CNSV_PYMT_TYPE_DESC,
pymt_impl.oo_cls_full_nm OO_CLS_FULL_NM,
pymt_impl.PGM_SHRT_NM PGM_SHRT_NM,
NULL HRCH_LVL_NM,
pymt_impl.data_stat_cd DATA_STAT_CD,
pymt_impl.cre_dt CRE_DT,
pymt_impl.last_chg_user_nm LAST_CHG_USER_NM,
pymt_impl.last_chg_dt LAST_CHG_DT,
'D' OP,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".pymt_impl pymt_impl
WHERE OP = 'D'
AND dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
)