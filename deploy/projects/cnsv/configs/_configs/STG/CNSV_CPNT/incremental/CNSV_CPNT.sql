-- Unknown Author edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CPNT (incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CNSV_CPNT/incremental/CNSV_CPNT.sql
-- Cynthia Singh edited code with changes for Athena and PySpark 20260204
-- =============================================================================

SELECT * FROM
(
SELECT
CPNT_CD,
CPNT_ID,
UOM_ID,
CPNT_DESC,
UOM_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
GENERIC_IND,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp() as LOAD_DT,
'CNSV_CPNT'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT 
FROM
(
SELECT * FROM
(
SELECT DISTINCT CPNT_CD,
CPNT_ID,
UOM_ID,
CPNT_DESC,
UOM_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
GENERIC_IND,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
CPNT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
component.CPNT_CD CPNT_CD,
component.CPNT_ID CPNT_ID,
component.UOM_ID UOM_ID,
component.CPNT_DESC CPNT_DESC,
unit_of_measure.UOM_NM UOM_NM,
component.DATA_STAT_CD DATA_STAT_CD,
component.CRE_DT CRE_DT,
component.LAST_CHG_DT LAST_CHG_DT,
component.LAST_CHG_USER_NM LAST_CHG_USER_NM,
component.GENERIC_IND GENERIC_IND,
component.op as CDC_OPER_CD,
1 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`COMPONENT` component

LEFT JOIN `fsa-{env}-cnsv`.`UNIT_OF_MEASURE` unit_of_measure

ON(component.UOM_ID = unit_of_measure.UOM_ID) 
WHERE component.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND component.op <> 'D'

UNION
SELECT 
component.CPNT_CD CPNT_CD,
component.CPNT_ID CPNT_ID,
component.UOM_ID UOM_ID,
component.CPNT_DESC CPNT_DESC,
unit_of_measure.UOM_NM UOM_NM,
component.DATA_STAT_CD DATA_STAT_CD,
component.CRE_DT CRE_DT,
component.LAST_CHG_DT LAST_CHG_DT,
component.LAST_CHG_USER_NM LAST_CHG_USER_NM,
component.GENERIC_IND GENERIC_IND,
unit_of_measure.op as CDC_OPER_CD,
2 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv-cdc`.`UNIT_OF_MEASURE` unit_of_measure

JOIN `fsa-{env}-cnsv`.`COMPONENT` component

ON(component.UOM_ID = unit_of_measure.UOM_ID) 
WHERE unit_of_measure.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND unit_of_measure.op <> 'D'

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
component.CPNT_CD CPNT_CD,
component.CPNT_ID CPNT_ID,
component.UOM_ID UOM_ID,
component.CPNT_DESC CPNT_DESC,
NULL UOM_NM,
component.DATA_STAT_CD DATA_STAT_CD,
component.CRE_DT CRE_DT,
component.LAST_CHG_DT LAST_CHG_DT,
component.LAST_CHG_USER_NM LAST_CHG_USER_NM,
component.GENERIC_IND GENERIC_IND,
component.op = 'D',
1 AS row_num_part
FROM `fsa-{env}-cnsv-cdc`.`component` component
WHERE component.op = 'D'