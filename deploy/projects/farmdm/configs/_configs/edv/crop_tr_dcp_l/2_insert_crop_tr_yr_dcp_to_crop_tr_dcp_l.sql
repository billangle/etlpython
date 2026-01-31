with cte as (
select 
	CTYD.PGM_ABR
	, CTYD.FSA_CROP_CD
	, CTYD.FSA_CROP_TYPE_CD
	, CTYD.ST_FSA_CD
	, CTYD.CNTY_FSA_CD
	, CTYD.FARM_NBR
	, CTYD.TR_NBR
	, CTYD.LOAD_DT
	, CTYD.DATA_SRC_NM
	, CTYD.CDC_DT
	, (MD5(upper(coalesce(CTYD.ST_FSA_CD,'--')||'~~'||coalesce(CTYD.CNTY_FSA_CD,'--')||'~~'||coalesce(CTYD.FARM_NBR,'--')))) as st_cnty_farm_nbr
	, (MD5(upper(coalesce(CTYD.ST_FSA_CD ,'--')||'~~'||coalesce(CTYD.CNTY_FSA_CD,'--')||'~~'||coalesce((CTYD.TR_NBR)::numeric  ,-1)))) as st_cnty_tr_nbr
	, (MD5(UPPER(COALESCE(TRIM(PGM_ABR), '--')) || '~~' || UPPER(COALESCE(TRIM(FSA_CROP_CD), '--')) || '~~' || UPPER(COALESCE(TRIM(FSA_CROP_TYPE_CD), '--')) || '~~' || MD5(UPPER(COALESCE(ST_FSA_CD, '--') || '~~' || COALESCE(CNTY_FSA_CD, '--') || '~~' || COALESCE(FARM_NBR, '--'))) || '~~' || MD5(UPPER(COALESCE(ST_FSA_CD, '--') || '~~' || COALESCE(CNTY_FSA_CD, '--') || '~~' || COALESCE((CTYD.TR_NBR)::numeric  ,'-1'))))) AS CROP_TR_DCP_L_ID
FROM SQL_FARM_RCD_STG.CROP_TR_YR_DCP CTYD 
where cdc_oper_cd IN ('I','UN','D')
),
cte1 as (
select 
	cte.PGM_ABR
	, cte.FSA_CROP_CD
	, cte.FSA_CROP_TYPE_CD
	, cte.ST_FSA_CD
	, cte.CNTY_FSA_CD
	, cte.FARM_NBR
	, cte.TR_NBR
	, TH.TR_H_ID
	, cte.LOAD_DT
	, cte.DATA_SRC_NM
	, cte.CDC_DT
	, FH.FARM_H_ID
	, cte.st_cnty_farm_nbr
	, cte.st_cnty_tr_nbr
	, cte.CROP_TR_DCP_L_ID
FROM cte
LEFT JOIN EDV.FARM_H FH
ON cte.st_cnty_farm_nbr = FH.FARM_H_ID
LEFT JOIN EDV.TR_H TH
ON cte.st_cnty_tr_nbr = TH.TR_H_ID
),
cte2 as (
SELECT 
	CROP_TR_DCP_L_ID
    , COALESCE(PGM_ABR, '--') AS PGM_ABR
    , COALESCE(FSA_CROP_CD, '--') AS FSA_CROP_CD
    , COALESCE(FSA_CROP_TYPE_CD, '--') AS FSA_CROP_TYPE_CD
    , case 
         WHEN cte1.FARM_H_ID IS NOT NULL THEN cte1.st_cnty_farm_nbr
         ELSE MD5('--' || '~~' || '--' || '~~' || '--')
     END AS FARM_H_ID
    , CASE
        WHEN cte1.TR_H_ID IS NOT NULL THEN cte1.st_cnty_tr_nbr
        ELSE MD5('--' || '~~' || '--' || '~~' || '-1')
     END AS TR_H_ID
    , LOAD_DT AS LOAD_DT
    , DATA_SRC_NM AS DATA_SRC_NM
    , ROW_NUMBER() OVER (
         PARTITION BY 
            COALESCE(PGM_ABR, '--')
            , COALESCE(FSA_CROP_CD, '--')
            , COALESCE(FSA_CROP_TYPE_CD, '--')
            , cte1.st_cnty_farm_nbr
            , cte1.st_cnty_tr_nbr
         ORDER BY CDC_DT DESC, LOAD_DT DESC) AS STG_EFF_DT_RANK
from cte1
),
cte3 as (
	select distinct * 
	from cte2
	where cte2.STG_EFF_DT_RANK = 1
)
INSERT  into
EDV.CROP_TR_DCP_L (
	CROP_TR_DCP_L_ID 
	, PGM_ABR
	, FSA_CROP_CD
	, FSA_CROP_TYPE_CD
	, FARM_H_ID
	, TR_H_ID
	, LOAD_DT
	, DATA_SRC_NM
)
(
select 
	cte3.CROP_TR_DCP_L_ID
	, cte3.PGM_ABR
	, cte3.FSA_CROP_CD
	, cte3.FSA_CROP_TYPE_CD
	, cte3.FARM_H_ID
	, cte3.TR_H_ID
	, cte3.LOAD_DT
	, cte3.DATA_SRC_NM
from cte3
LEFT JOIN  EDV.CROP_TR_DCP_L dv
ON cte3.CROP_TR_DCP_L_ID = dv.CROP_TR_DCP_L_ID
where dv.CROP_TR_DCP_L_ID IS null
)
;