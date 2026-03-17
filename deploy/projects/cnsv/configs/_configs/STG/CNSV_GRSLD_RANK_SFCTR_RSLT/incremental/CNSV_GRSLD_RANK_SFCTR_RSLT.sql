-- Author Unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_GRSLD_RANK_SFCTR_USER_RSLT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CNSV_GRSLD_RANK_SFCTR_USER_RSLT (Incremental)/incremental/CNSV_GRSLD_RANK_SFCTR_USER_RSLT (Incremental).sql
-- Cynthia Singh edited code with changes for Athena and PySpark 20260204
-- =======================================================================================

SELECT * FROM
(
SELECT PGM_YR,
ADM_ST_FSA_CD,
ADM_CNTY_FSA_CD,
SGNP_TYPE_DESC,
SGNP_STYPE_DESC,
SGNP_NBR,
SGNP_STYPE_AGR_NM,
TR_NBR,
OFR_SCNR_NM,
GRSLD_RANK_FCTR_IDN,
GRSLD_RANK_SFCTR_IDN,
RESP_SRC_CD,
RANK_PNT_CT,
GRSLD_RANK_SFCTR_RSLT_ID,
OFR_SCNR_ID,
GRSLD_RANK_SFCTR_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
CRE_USER_NM,
LAST_CHG_USER_NM,
CDC_OPER_CD,
LOAD_DT,
DATA_SRC_NM,
CDC_DT,
''  HASH_DIF,
current_timestamp() as LOAD_DT,
'CNSV_GRSLD_RANK_SFCTR_RSLT' as DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT FROM ( SELECT * FROM 
(
SELECT DISTINCT PGM_YR,
ADM_ST_FSA_CD,
ADM_CNTY_FSA_CD,
SGNP_TYPE_DESC,
SGNP_STYPE_DESC,
SGNP_NBR,
SGNP_STYPE_AGR_NM,
TR_NBR,
OFR_SCNR_NM,
GRSLD_RANK_FCTR_IDN,
GRSLD_RANK_SFCTR_IDN,
RESP_SRC_CD,
RANK_PNT_CT,
GRSLD_RANK_SFCTR_RSLT_ID,
OFR_SCNR_ID,
GRSLD_RANK_SFCTR_ID,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
CRE_USER_NM,
LAST_CHG_USER_NM,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
GRSLD_RANK_SFCTR_RSLT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
GRASSLAND_RANKING_SUBFACTOR.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
grassland_ranking_subfactor_result.op as CDC_OPER_CD,
1 AS TBL_PRIORITY
FROM    `fsa-{env}-cnsv-cdc`.`GRASSLAND_RANKING_SUBFACTOR_RESULT` grassland_ranking_subfactor_result
LEFT JOIN    `fsa-{env}-cnsv`.`GRASSLAND_RANKING_SUBFACTOR` 
ON grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID = GRASSLAND_RANKING_SUBFACTOR.GRSLD_RANK_SFCTR_ID
LEFT JOIN    `fsa-{env}-cnsv`.`grassland_ranking_factor` grassland_ranking_factor 
ON GRASSLAND_RANKING_SUBFACTOR.GRSLD_RANK_FCTR_ID = grassland_ranking_factor.GRSLD_RANK_FCTR_ID
LEFT JOIN    `fsa-{env}-cnsv`.`ewt14sgnp` 
ON grassland_ranking_factor.SGNP_ID = ewt14sgnp.SGNP_ID
LEFT JOIN   `fsa-{env}-cnsv`.`CRP_GRASSLAND_OFFER_SCENARIO` 
ON grassland_ranking_subfactor_result.OFR_SCNR_ID = CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt40ofrsc`  
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = ewt40ofrsc.OFR_SCNR_ID
WHERE grassland_ranking_subfactor_result.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and grassland_ranking_subfactor_result.op <> 'D'

UNION
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
GRASSLAND_RANKING_SUBFACTOR.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
GRASSLAND_RANKING_SUBFACTOR.op as CDC_OPER_CD,
2 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`GRASSLAND_RANKING_SUBFACTOR` grassland_ranking_subfactor
JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
ON grassland_ranking_subfactor.GRSLD_RANK_SFCTR_ID = grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID
LEFT JOIN    `fsa-{env}-cnsv`.`grassland_ranking_factor`  
ON grassland_ranking_subfactor.GRSLD_RANK_FCTR_ID = grassland_ranking_factor.GRSLD_RANK_FCTR_ID
LEFT JOIN    `fsa-{env}-cnsv`.`ewt14sgnp`
ON grassland_ranking_factor.SGNP_ID = ewt14sgnp.SGNP_ID
LEFT JOIN   `fsa-{env}-cnsv`.`CRP_GRASSLAND_OFFER_SCENARIO` 
ON grassland_ranking_subfactor_result.OFR_SCNR_ID = CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt40ofrsc`  
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = ewt40ofrsc.OFR_SCNR_ID
WHERE grassland_ranking_subfactor.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and grassland_ranking_subfactor.op <> 'D'

UNION
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
grassland_ranking_subfactor.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
grassland_ranking_factor.op as CDC_OPER_CD,
3 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`GRASSLAND_RANKING_FACTOR` grassland_ranking_factor 
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor` 
ON grassland_ranking_factor.GRSLD_RANK_FCTR_ID = grassland_ranking_subfactor.GRSLD_RANK_FCTR_ID
JOIN    `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
ON grassland_ranking_subfactor.GRSLD_RANK_SFCTR_ID = grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt14sgnp` 
ON grassland_ranking_factor.SGNP_ID = ewt14sgnp.SGNP_ID
LEFT JOIN   `fsa-{env}-cnsv`.`CRP_GRASSLAND_OFFER_SCENARIO` 
ON grassland_ranking_subfactor_result.OFR_SCNR_ID = CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt40ofrsc`  
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = ewt40ofrsc.OFR_SCNR_ID
WHERE grassland_ranking_factor.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and grassland_ranking_factor.op <> 'D'

UNION
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
grassland_ranking_subfactor.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
ewt14sgnp.op as CDC_OPER_CD,
4 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`EWT14SGNP` ewt14sgnp
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
ON ewt14sgnp.SGNP_ID = grassland_ranking_factor.SGNP_ID
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
ON grassland_ranking_factor.GRSLD_RANK_FCTR_ID = grassland_ranking_subfactor.GRSLD_RANK_FCTR_ID
JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
ON grassland_ranking_subfactor.GRSLD_RANK_SFCTR_ID = grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`CRP_GRASSLAND_OFFER_SCENARIO` 
ON grassland_ranking_subfactor_result.OFR_SCNR_ID = CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt40ofrsc`  
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = ewt40ofrsc.OFR_SCNR_ID
WHERE ewt14sgnp.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and ewt14sgnp.op <> 'D'

UNION
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
grassland_ranking_subfactor.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
ewt40ofrsc.op as CDC_OPER_CD,
5 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`EWT40OFRSC` ewt40ofrsc 
LEFT JOIN   `fsa-{env}-cnsv`.`CRP_GRASSLAND_OFFER_SCENARIO` 
ON ewt40ofrsc.OFR_SCNR_ID = CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID
JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = grassland_ranking_subfactor_result.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor` 
ON grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID = grassland_ranking_subfactor.GRSLD_RANK_SFCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
ON grassland_ranking_subfactor.GRSLD_RANK_FCTR_ID = grassland_ranking_factor.GRSLD_RANK_FCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt14sgnp`
ON grassland_ranking_factor.SGNP_ID = ewt14sgnp.SGNP_IDWHERE ewt40ofrsc.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and ewt40ofrsc.op <> 'D'

UNION
SELECT 
ewt40ofrsc.PGM_YR PGM_YR,
ewt40ofrsc.ADM_ST_FSA_CD ADM_ST_FSA_CD,
ewt40ofrsc.ADM_CNTY_FSA_CD ADM_CNTY_FSA_CD,
ewt14sgnp.SGNP_TYPE_DESC SGNP_TYPE_DESC,
ewt14sgnp.SGNP_STYPE_DESC SGNP_STYPE_DESC,
ewt14sgnp.SGNP_NBR SGNP_NBR,
ewt14sgnp.SGNP_STYPE_AGR_NM SGNP_STYPE_AGR_NM,
ewt40ofrsc.TR_NBR TR_NBR,
ewt40ofrsc.OFR_SCNR_NM OFR_SCNR_NM,
grassland_ranking_factor.GRSLD_RANK_FCTR_IDN GRSLD_RANK_FCTR_IDN,
grassland_ranking_subfactor.GRSLD_RANK_SFCTR_IDN GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
CRP_GRASSLAND_OFFER_SCENARIO.op as CDC_OPER_CD,
6 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`CRP_GRASSLAND_OFFER_SCENARIO` crp_grassland_offer_scenario
LEFT JOIN   `fsa-{env}-cnsv`.`ewt40ofrsc`  
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = ewt40ofrsc.OFR_SCNR_ID
JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
ON CRP_GRASSLAND_OFFER_SCENARIO.OFR_SCNR_ID = grassland_ranking_subfactor_result.OFR_SCNR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`  
ON grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID = grassland_ranking_subfactor.GRSLD_RANK_SFCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
ON grassland_ranking_subfactor.GRSLD_RANK_FCTR_ID = grassland_ranking_factor.GRSLD_RANK_FCTR_ID
LEFT JOIN   `fsa-{env}-cnsv`.`ewt14sgnp` 
ON grassland_ranking_factor.SGNP_ID = ewt14sgnp.SGNP_ID
WHERE CRP_GRASSLAND_OFFER_SCENARIO.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and CRP_GRASSLAND_OFFER_SCENARIO.op <> 'D'

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
NULL PGM_YR,
NULL ADM_ST_FSA_CD,
NULL ADM_CNTY_FSA_CD,
NULL SGNP_TYPE_DESC,
NULL SGNP_STYPE_DESC,
NULL SGNP_NBR,
NULL SGNP_STYPE_AGR_NM,
NULL TR_NBR,
NULL OFR_SCNR_NM,
NULL GRSLD_RANK_FCTR_IDN,
NULL GRSLD_RANK_SFCTR_IDN,
grassland_ranking_subfactor_result.RESP_SRC_CD RESP_SRC_CD,
grassland_ranking_subfactor_result.RANK_PNT_CT RANK_PNT_CT,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_RSLT_ID GRSLD_RANK_SFCTR_RSLT_ID,
grassland_ranking_subfactor_result.OFR_SCNR_ID OFR_SCNR_ID,
grassland_ranking_subfactor_result.GRSLD_RANK_SFCTR_ID GRSLD_RANK_SFCTR_ID,
grassland_ranking_subfactor_result.DATA_STAT_CD DATA_STAT_CD,
grassland_ranking_subfactor_result.CRE_DT CRE_DT,
grassland_ranking_subfactor_result.LAST_CHG_DT LAST_CHG_DT,
grassland_ranking_subfactor_result.CRE_USER_NM CRE_USER_NM,
grassland_ranking_subfactor_result.LAST_CHG_USER_NM LAST_CHG_USER_NM,
grassland_ranking_subfactor_result.op = 'D',
1 AS row_num_part
FROM `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_result` grassland_ranking_subfactor_result
WHERE grassland_ranking_subfactor_result.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and grassland_ranking_subfactor_result.op = 'D'
)