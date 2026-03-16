SELECT CNSV_PRAC_CD,
AFL_INIT_NM,
AFL_INIT_CAT_NM,
EFF_PRD_STRT_DT,
EFF_PRD_END_DT,
PRAC_INIT_CAT_ID,
AFL_INIT_CAT_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
CRE_USER_NM,
LAST_CHG_USER_NM,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT FROM ( SELECT * FROM 
(
SELECT DISTINCT CNSV_PRAC_CD,
AFL_INIT_NM,
AFL_INIT_CAT_NM,
EFF_PRD_STRT_DT,
EFF_PRD_END_DT,
PRAC_INIT_CAT_ID,
AFL_INIT_CAT_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
CRE_USER_NM,
LAST_CHG_USER_NM,
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
PRAC_INIT_CAT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
practice_initiative_category.cnsv_prac_cd CNSV_PRAC_CD,
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
practice_initiative_category.eff_prd_strt_dt EFF_PRD_STRT_DT,
practice_initiative_category.eff_prd_end_dt EFF_PRD_END_DT,
practice_initiative_category.prac_init_ctg_id PRAC_INIT_CAT_ID,
practice_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
practice_initiative_category.data_stat_cd DATA_STAT_CD,
practice_initiative_category.cre_dt CRE_DT,
practice_initiative_category.last_chg_dt LAST_CHG_DT,
practice_initiative_category.cre_user_nm CRE_USER_NM,
practice_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
PRACTICE_INITIATIVE_CATEGORY.OP,
1 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."PRACTICE_INITIATIVE_CATEGORY" PRACTICE_INITIATIVE_CATEGORY  
LEFT JOIN "fsa-{env}-cnsv".AFFILIATED_INITIATIVE_CATEGORY AFFILIATED_INITIATIVE_CATEGORY ON ( PRACTICE_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID ) 
LEFT JOIN "fsa-{env}-cnsv".AFFILIATED_INITIATIVE AFFILIATED_INITIATIVE ON ( AFFILIATED_INITIATIVE.AFL_INIT_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_ID ) 
WHERE PRACTICE_INITIATIVE_CATEGORY.OP <> 'D' AND
PRACTICE_INITIATIVE_CATEGORY.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT 
practice_initiative_category.cnsv_prac_cd CNSV_PRAC_CD,
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
practice_initiative_category.eff_prd_strt_dt EFF_PRD_STRT_DT,
practice_initiative_category.eff_prd_end_dt EFF_PRD_END_DT,
practice_initiative_category.prac_init_ctg_id PRAC_INIT_CAT_ID,
practice_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
practice_initiative_category.data_stat_cd DATA_STAT_CD,
practice_initiative_category.cre_dt CRE_DT,
practice_initiative_category.last_chg_dt LAST_CHG_DT,
practice_initiative_category.cre_user_nm CRE_USER_NM,
practice_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
AFFILIATED_INITIATIVE.OP,
2 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."AFFILIATED_INITIATIVE" AFFILIATED_INITIATIVE 
LEFT JOIN "fsa-{env}-cnsv".AFFILIATED_INITIATIVE_CATEGORY AFFILIATED_INITIATIVE_CATEGORY ON (AFFILIATED_INITIATIVE.AFL_INIT_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_ID) 
JOIN "fsa-{env}-cnsv".PRACTICE_INITIATIVE_CATEGORY PRACTICE_INITIATIVE_CATEGORY ON (PRACTICE_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID)
WHERE AFFILIATED_INITIATIVE.OP <> 'D' AND
AFFILIATED_INITIATIVE.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT 
practice_initiative_category.cnsv_prac_cd CNSV_PRAC_CD,
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative_category.afl_init_ctg_nm AFL_INIT_CAT_NM,
practice_initiative_category.eff_prd_strt_dt EFF_PRD_STRT_DT,
practice_initiative_category.eff_prd_end_dt EFF_PRD_END_DT,
practice_initiative_category.prac_init_ctg_id PRAC_INIT_CAT_ID,
practice_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
practice_initiative_category.data_stat_cd DATA_STAT_CD,
practice_initiative_category.cre_dt CRE_DT,
practice_initiative_category.last_chg_dt LAST_CHG_DT,
practice_initiative_category.cre_user_nm CRE_USER_NM,
practice_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
AFFILIATED_INITIATIVE_CATEGORY.OP,
3 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc"."AFFILIATED_INITIATIVE_CATEGORY" AFFILIATED_INITIATIVE_CATEGORY   
LEFT JOIN "fsa-{env}-cnsv".AFFILIATED_INITIATIVE AFFILIATED_INITIATIVE ON (AFFILIATED_INITIATIVE.AFL_INIT_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_ID) 
JOIN "fsa-{env}-cnsv".PRACTICE_INITIATIVE_CATEGORY PRACTICE_INITIATIVE_CATEGORY ON (PRACTICE_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID = AFFILIATED_INITIATIVE_CATEGORY.AFL_INIT_CTG_ID) 
WHERE AFFILIATED_INITIATIVE_CATEGORY.OP <> 'D' AND 
AFFILIATED_INITIATIVE_CATEGORY.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

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
practice_initiative_category.cnsv_prac_cd CNSV_PRAC_CD,
NULL AFL_INIT_NM,
NULL AFL_INIT_CAT_NM,
practice_initiative_category.eff_prd_strt_dt EFF_PRD_STRT_DT,
practice_initiative_category.eff_prd_end_dt EFF_PRD_END_DT,
practice_initiative_category.prac_init_ctg_id PRAC_INIT_CAT_ID,
practice_initiative_category.afl_init_ctg_id AFL_INIT_CAT_ID,
practice_initiative_category.data_stat_cd DATA_STAT_CD,
practice_initiative_category.cre_dt CRE_DT,
practice_initiative_category.last_chg_dt LAST_CHG_DT,
practice_initiative_category.cre_user_nm CRE_USER_NM,
practice_initiative_category.last_chg_user_nm LAST_CHG_USER_NM,
'D' OP,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".practice_initiative_category practice_initiative_category
WHERE OP = 'D'
AND dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
)