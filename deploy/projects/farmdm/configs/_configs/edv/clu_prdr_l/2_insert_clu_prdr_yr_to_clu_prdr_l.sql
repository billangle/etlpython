INSERT into
EDV.CLU_PRDR_L(CLU_PRDR_L_ID ,
CORE_CUST_ID,
FARM_H_ID,
TR_H_ID,
LOAD_DT,
DATA_SRC_NM)
(

select sub.*
from edv.farm_h fh 
inner join (
		select 
		stg.CLU_PRDR_L_ID,
		stg.CORE_CUST_ID,
		stg.FARM_H_ID,
		stg.TR_H_ID,
		stg.LOAD_DT,
		stg.DATA_SRC_NM
		from (
		SELECT DISTINCT
		coalesce(CLU_PRDR_YR.CORE_CUST_ID, '-1') CORE_CUST_ID,
		MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.FARM_NBR,'--'))) FARM_H_ID,
		MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce((CLU_PRDR_YR.TR_NBR)::numeric ,-1))) TR_H_ID,
		CLU_PRDR_YR.LOAD_DT LOAD_DT,
		CLU_PRDR_YR.DATA_SRC_NM DATA_SRC_NM,
		ROW_NUMBER() OVER (PARTITION BY coalesce(CLU_PRDR_YR.CORE_CUST_ID, '-1') ,
		MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.FARM_NBR,'--'))),
		MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce((CLU_PRDR_YR.TR_NBR)::numeric ,-1))) ORDER BY CLU_PRDR_YR.CDC_DT desc, CLU_PRDR_YR.LOAD_DT desc) STG_EFF_DT_RANK,
		MD5(coalesce(CLU_PRDR_YR.CORE_CUST_ID, '-1') ||'~~'||MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.FARM_NBR,'--')))||'~~'||MD5(upper(coalesce(CLU_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(CLU_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce((CLU_PRDR_YR.TR_NBR)::numeric ,-1)))) CLU_PRDR_L_ID
		FROM SQL_FARM_RCD_STG.CLU_PRDR_YR
		where CLU_PRDR_YR.cdc_oper_cd IN ('I','UN','D')
		) stg 
		LEFT JOIN  EDV.CLU_PRDR_L dv
		 ON (
		stg.CLU_PRDR_L_ID = dv.CLU_PRDR_L_ID
		)
		where (
		dv.CLU_PRDR_L_ID IS NULL
		)
		and stg.STG_EFF_DT_RANK = 1
) sub
on  fh.FARM_H_ID = sub.FARM_H_ID
INNER JOIN edv.TR_H tr
ON tr.TR_H_ID = sub.TR_H_ID
)
;