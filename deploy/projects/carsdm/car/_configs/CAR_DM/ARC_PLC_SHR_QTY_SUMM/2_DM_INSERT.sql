INSERT into CAR_DM_STG.ARC_PLC_SHR_QTY_SUMM
(
PGM_YR
, ST_FSA_CD
, CNTY_FSA_CD
, farm_nbr
, CORE_CUST_ID
, CROP_FLD_RPT_QTY
, CROP_FLD_DTER_QTY
, CPLD_IND
, FSA_CROP_CD
)
SELECT  appd.PGM_YR
	,	appd.ST_FSA_CD
	,	appd.CNTY_FSA_CD
    ,   appd.farm_nbr
    ,   cd.CORE_CUST_ID
    ,   COALESCE(appd.CROP_FLD_RPT_QTY * ( appbpf.CROP_SHR_PCT / 100 ), 0) as CROP_FLD_RPT_QTY
    ,   COALESCE(appd.CROP_FLD_DTER_QTY * ( appbpf.CROP_SHR_PCT / 100 ), 0) as CROP_FLD_DTER_QTY
    ,   appd.CPLD_IND
    ,   appd.FSA_CROP_CD
FROM CAR_DM_STG.AG_PROD_PLAN_BUS_PTY_FACT appbpf
    INNER JOIN CAR_DM_STG.AG_PROD_PLAN_DIM appd
        ON      appbpf.AG_PROD_PLAN_DURB_ID = appd.AG_PROD_PLAN_DURB_ID
            and appd.CUR_RCD_IND = 1
            AND appbpf.DATA_STAT_CD = 'A'
            AND appd.SRC_DATA_STAT_CD = 'A'
            AND appbpf.CROP_SHR_PCT	> 0
            AND ( appd.CROP_FLD_RPT_QTY > 0 OR appd.CROP_FLD_DTER_QTY > 0 )
            AND appd.CERT_DT IS NOT NULL
--            AND appd.PGM_YR = :programYear
--            and appd.ST_FSA_CD = :stateCode
--            and appd.CNTY_FSA_CD = :countyCode
            AND appd.CPLD_IND = 'Y'
    INNER JOIN CAR_DM_STG.AG_PROD_PLAN_FACT appf
        ON      appd.SRC_AG_PROD_PLAN_ID = appf.SRC_AG_PROD_PLAN_ID
    INNER JOIN CAR_DM_STG.CROP_ACRG_RPT_DIM card
        ON      appf.CROP_ACRG_RPT_DURB_ID = card.CROP_ACRG_RPT_DURB_ID
            AND card.CUR_RCD_IND = 1
            AND card.SRC_DATA_STAT_CD = 'A'
    INNER JOIN cmn_dim_dm_stg.CUST_DIM cd
        ON      appbpf.CUST_DURB_ID = cd.CUST_DURB_ID
            AND cd.CUR_RCD_IND = 1
