INSERT into
EDV.FARM_H(FARM_H_ID, 
ST_FSA_CD, 
CNTY_FSA_CD,
FARM_NBR,
LOAD_DT,
DATA_SRC_NM)
(
SELECT sub.*
FROM (
SELECT distinct 
stg.FARM_H_ID,stg.ST_FSA_CD,stg.CNTY_FSA_CD,stg.FARM_NBR,stg.LOAD_DT,stg.DATA_SRC_NM
from (
select * from (
select MD5(upper(coalesce(trim(both ST_FSA_CD), '--')) ||'~~'||upper(coalesce(trim(both CNTY_FSA_CD), '--')) 
||'~~'||upper(coalesce(trim(both FARM_NBR), '--')) ) FARM_H_ID,
coalesce(trim(both ST_FSA_CD),'--') ST_FSA_CD, coalesce(trim(both CNTY_FSA_CD),'--') CNTY_FSA_CD, 
coalesce(trim(both FARM_NBR),'--') FARM_NBR,
LOAD_DT, DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY ST_FSA_CD,CNTY_FSA_CD,FARM_NBR 
    ORDER BY CDC_DT DESC, LOAD_DT DESC) STG_EFF_DT_RANK
from (
SELECT ST_FSA_CD, CNTY_FSA_CD, FARM_NBR,
    LOAD_DT, DATA_SRC_NM, CDC_DT
    from SQL_FARM_RCD_STG.FARM

UNION

select PRNT_ST_FSA_CD ST_FSA_CD, PRNT_CNTY_FSA_CD CNTY_FSA_CD, PRNT_FARM_NBR FARM_NBR,
    LOAD_DT, DATA_SRC_NM, CDC_DT
    from SQL_FARM_RCD_STG.MIDAS_FARM_TR_CHG 

UNION

select RSLT_ST_FSA_CD ST_FSA_CD, RSLT_CNTY_FSA_CD CNTY_FSA_CD, RSLT_FARM_NBR FARM_NBR,
    LOAD_DT, DATA_SRC_NM, CDC_DT
    from SQL_FARM_RCD_STG.MIDAS_FARM_TR_CHG 

UNION

SELECT ST_FSA_CD, CNTY_FSA_CD, FARM_NBR,
    LOAD_DT, DATA_SRC_NM, CDC_DT
    from SQL_FARM_RCD_STG.FARM_YR
 ) alias19 
 ) alias20 where STG_EFF_DT_RANK = 1
) stg 
LEFT JOIN  EDV.FARM_H dv
 ON stg.FARM_H_ID = dv.FARM_H_ID
where dv.FARM_H_ID IS NULL
) sub where Not exists (select 1 from EDV.FARM_H f
                where f.FARM_H_ID = sub.FARM_H_ID
				and f.ST_FSA_CD = sub.ST_FSA_CD
				and f.CNTY_FSA_CD = sub.CNTY_FSA_CD
				and f.FARM_NBR = sub.FARM_NBR
				and f.LOAD_DT = sub.LOAD_DT
				and f.DATA_SRC_NM = sub.DATA_SRC_NM
			   )
);