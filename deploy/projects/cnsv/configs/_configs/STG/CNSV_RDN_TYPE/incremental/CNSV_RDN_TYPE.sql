SELECT PYMT_YR,
HRCH_LVL_NM,
RDN_TYPE_CD,
RDN_TYPE_ID,
RDN_TYPE_DESC,
PGM_HRCH_LVL_ID,
PRTY_NBR,
NTL_PYMT_RDN_PCT,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT FROM ( SELECT * FROM 
(
SELECT DISTINCT PYMT_YR,
HRCH_LVL_NM,
RDN_TYPE_CD,
RDN_TYPE_ID,
RDN_TYPE_DESC,
PGM_HRCH_LVL_ID,
PRTY_NBR,
NTL_PYMT_RDN_PCT,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
RDN_TYPE_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
reduction_type.pymt_yr PYMT_YR,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
reduction_type.rdn_type_cd RDN_TYPE_CD,
reduction_type.rdn_type_id RDN_TYPE_ID,
reduction_type.rdn_type_desc RDN_TYPE_DESC,
reduction_type.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
reduction_type.prty_nbr PRTY_NBR,
reduction_type.ntl_pymt_rdn_pct NTL_PYMT_RDN_PCT,
reduction_type.data_stat_cd DATA_STAT_CD,
reduction_type.cre_dt CRE_DT,
reduction_type.last_chg_dt LAST_CHG_DT,
reduction_type.last_chg_user_nm LAST_CHG_USER_NM,
reduction_type.OP,
1 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."reduction_type" reduction_type 
LEFT JOIN "fsa-{env}-cnsv".PROGRAM_HIERARCHY_LEVEL 
ON (reduction_type.pgm_hrch_lvl_id = PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID)
WHERE reduction_type.OP <> 'D' AND
reduction_type.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT 
reduction_type.pymt_yr PYMT_YR,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
reduction_type.rdn_type_cd RDN_TYPE_CD,
reduction_type.rdn_type_id RDN_TYPE_ID,
reduction_type.rdn_type_desc RDN_TYPE_DESC,
reduction_type.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
reduction_type.prty_nbr PRTY_NBR,
reduction_type.ntl_pymt_rdn_pct NTL_PYMT_RDN_PCT,
reduction_type.data_stat_cd DATA_STAT_CD,
reduction_type.cre_dt CRE_DT,
reduction_type.last_chg_dt LAST_CHG_DT,
reduction_type.last_chg_user_nm LAST_CHG_USER_NM,
PROGRAM_HIERARCHY_LEVEL.OP,
2 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."PROGRAM_HIERARCHY_LEVEL" PROGRAM_HIERARCHY_LEVEL 
 JOIN "fsa-{env}-cnsv".reduction_type 
ON (PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID = reduction_type.pgm_hrch_lvl_id)
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
reduction_type.pymt_yr PYMT_YR,
NULL HRCH_LVL_NM,
reduction_type.rdn_type_cd RDN_TYPE_CD,
reduction_type.rdn_type_id RDN_TYPE_ID,
reduction_type.rdn_type_desc RDN_TYPE_DESC,
reduction_type.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
reduction_type.prty_nbr PRTY_NBR,
reduction_type.ntl_pymt_rdn_pct NTL_PYMT_RDN_PCT,
reduction_type.data_stat_cd DATA_STAT_CD,
reduction_type.cre_dt CRE_DT,
reduction_type.last_chg_dt LAST_CHG_DT,
reduction_type.last_chg_user_nm LAST_CHG_USER_NM,
'D' OP,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".reduction_type reduction_type
WHERE OP = 'D'
AND dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
)