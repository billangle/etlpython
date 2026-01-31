INSERT into
EDV.FARM_RCON_L(FARM_RCON_L_ID ,
PRNT_FARM_H_ID,
RSLT_FARM_H_ID,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.FARM_RCON_L_ID,
stg.PRNT_FARM_H_ID,
stg.RSLT_FARM_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
MD5(upper(coalesce( FARM_RCON.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_FARM_NBR,'--'))) PRNT_FARM_H_ID,
MD5(upper(coalesce( FARM_RCON.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_FARM_NBR,'--'))) RSLT_FARM_H_ID,
FARM_RCON.LOAD_DT LOAD_DT,
FARM_RCON.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce( FARM_RCON.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_FARM_NBR,'--'))),
MD5(upper(coalesce( FARM_RCON.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_FARM_NBR,'--'))) ORDER BY FARM_RCON.CDC_DT desc, FARM_RCON.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce( FARM_RCON.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.PRNT_FARM_NBR,'--')))||'~~'||MD5(upper(coalesce( FARM_RCON.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(FARM_RCON.RSLT_FARM_NBR,'--')))) FARM_RCON_L_ID
FROM SQL_FARM_RCD_STG.FARM_RCON 
where FARM_RCON.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.FARM_RCON_L dv
 ON (
stg.FARM_RCON_L_ID = dv.FARM_RCON_L_ID
)
where (
dv.FARM_RCON_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1 
UNION
 
select 
stg.FARM_RCON_L_ID,
stg.PRNT_FARM_H_ID,
stg.RSLT_FARM_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_FARM_NBR,'--'))) PRNT_FARM_H_ID,
MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_FARM_NBR,'--'))) RSLT_FARM_H_ID,
MIDAS_FARM_TR_CHG.LOAD_DT LOAD_DT,
MIDAS_FARM_TR_CHG.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_FARM_NBR,'--'))),
MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_FARM_NBR,'--'))) ORDER BY MIDAS_FARM_TR_CHG.CDC_DT desc, MIDAS_FARM_TR_CHG.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.PRNT_FARM_NBR,'--')))||'~~'||MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--') ||'~~'|| coalesce(MIDAS_FARM_TR_CHG.RSLT_FARM_NBR,'--')))) FARM_RCON_L_ID
FROM SQL_FARM_RCD_STG.MIDAS_FARM_TR_CHG 
where MIDAS_FARM_TR_CHG.cdc_oper_cd IN ('I','UN','D')
AND MIDAS_FARM_TR_CHG.FARM_CHG_TYPE_ID in (1141 , 1142)
) stg 
LEFT JOIN  EDV.FARM_RCON_L dv
 ON (
stg.FARM_RCON_L_ID = dv.FARM_RCON_L_ID
)
where (
dv.FARM_RCON_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;