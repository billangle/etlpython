INSERT  into
EDV.FLD_H(FLD_H_ID ,
ST_FSA_CD,
CNTY_FSA_CD,
FARM_NBR,
TR_NBR,
FLD_NBR,
DATA_SRC_NM,
LOAD_DT)
(
select sub.*
from 
(
SELECT distinct stg.FLD_H_ID,
stg.ST_FSA_CD,
stg.CNTY_FSA_CD,
stg.FARM_NBR,
cast(stg.TR_NBR as int4),
stg.FLD_NBR,
stg.DATA_SRC_NM,
stg.LOAD_DT
from (
SELECT temp.*,
ROW_NUMBER() OVER (PARTITION BY temp.FLD_H_ID ORDER BY temp.CDC_DT desc, temp.LOAD_DT desc) STG_EFF_DT_RANK
FROM (
SELECT DISTINCT
coalesce(trim(both CLU_YR.ST_FSA_CD), '--') ST_FSA_CD,
coalesce(trim(both CLU_YR.CNTY_FSA_CD), '--') CNTY_FSA_CD,
coalesce(trim(both lpad(trim(both CLU_YR.FARM_NBR), 7,'0')), '--') FARM_NBR,
coalesce(trim(both (CLU_YR.TR_NBR)), '-1') TR_NBR,
coalesce(trim(both ltrim(trim(both CLU_YR.FLD_NBR),'0')), '--') FLD_NBR,
MD5(upper(coalesce(trim(both CLU_YR.ST_FSA_CD), '--')) ||'~~'||upper(coalesce(trim(both CLU_YR.CNTY_FSA_CD), '--')) ||'~~'||upper(coalesce(trim(both Lpad(trim(both CLU_YR.FARM_NBR),7,'0')), '--')) ||'~~'||upper(coalesce(trim(both (CLU_YR.TR_NBR)), '-1')) ||'~~'||upper(coalesce(trim(both ltrim(trim(both CLU_YR.FLD_NBR),'0')), '--')) ) FLD_H_ID,
CLU_YR.DATA_SRC_NM DATA_SRC_NM,
CLU_YR.LOAD_DT LOAD_DT,
CLU_YR.CDC_DT
FROM SQL_FARM_RCD_STG.CLU_YR
where CLU_YR.cdc_oper_cd IN ('I', 'UN')
) temp 
) stg 
LEFT JOIN  EDV.FLD_H dv
 ON (
stg.FLD_H_ID = dv.FLD_H_ID
)
where (
dv.FLD_H_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
) sub
where not exists (select 1 from EDV.FLD_H h
				  where sub.FLD_H_ID = h.FLD_H_ID
				  and sub.st_fsa_cd = h.st_fsa_cd	
				  and sub.cnty_fsa_cd = h.cnty_fsa_cd
				  and sub.tr_nbr = h.tr_nbr
				  and sub.farm_nbr = h.farm_nbr
				  and sub.fld_nbr = h.fld_nbr
			)
);