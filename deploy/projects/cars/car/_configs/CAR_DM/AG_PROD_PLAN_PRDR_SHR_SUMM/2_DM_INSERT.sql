INSERT INTO CAR_DM_STG.AG_PROD_PLAN_PRDR_SHR_SUMM (PGM_YR, FARM_NBR, ST_FSA_CD, CNTY_FSA_CD, FSA_CROP_CD, FSA_CROP_TYPE_CD, INTN_USE_CD, PLNT_PRIM_STAT_CD, PLNT_SCND_STAT_CD, CERT_DT, CPLD_IND, CORE_CUST_ID, CROP_SHR_PCT, CROP_FLD_RPT_QTY, CROP_FLD_DTER_QTY, SRC_AG_PROD_PLAN_ID, MULT_CROP_INTN_USE_STAT_CD)
    (SELECT /*+ PARALLEL(16) */
        appbpf.pgm_yr,
        appd.farm_nbr,
        appd.ST_FSA_CD,
        appd.CNTY_FSA_CD,
        appd.FSA_CROP_CD,
        appd.fsa_crop_type_cd,
        appd.INTN_USE_CD,
        appd.plnt_prim_stat_cd,
        appd.plnt_scnd_stat_cd,
        appd.CERT_DT,
        appd.CPLD_IND,
        cd.CORE_CUST_ID,
        appbpf.CROP_SHR_PCT,
        appd.CROP_FLD_RPT_QTY,
        appd.CROP_FLD_DTER_QTY,
        appd.SRC_AG_PROD_PLAN_ID,
        appd.MULT_CROP_INTN_USE_STAT_CD
    FROM CAR_DM_STG.AG_PROD_PLAN_BUS_PTY_FACT  appbpf
        INNER JOIN CAR_DM_STG.AG_PROD_PLAN_DIM appd
            ON     appbpf.AG_PROD_PLAN_DURB_ID = appd.AG_PROD_PLAN_DURB_ID
                AND appd.CUR_RCD_IND = 1
                AND appbpf.DATA_STAT_CD = 'A'
                AND appbpf.BUS_PTY_SRC_DATA_STAT_CD = 'A'
                AND appd.SRC_DATA_STAT_CD = 'A'
                AND appbpf.CROP_SHR_PCT > 0
        INNER JOIN CAR_DM_STG.AG_PROD_PLAN_FACT appf
            ON appd.SRC_AG_PROD_PLAN_ID = appf.SRC_AG_PROD_PLAN_ID
        INNER JOIN CAR_DM_STG.CROP_ACRG_RPT_DIM card
            ON     appf.CROP_ACRG_RPT_DURB_ID = card.CROP_ACRG_RPT_DURB_ID
                AND card.CUR_RCD_IND = 1
                AND card.RQR_PLNT_STAT_CPLT_IND = 1
                AND appd.SRC_DATA_STAT_CD = 'A'
        INNER JOIN cmn_dim_dm_stg.CUST_DIM cd
            ON     appbpf.CUST_DURB_ID = cd.CUST_DURB_ID
                AND cd.CUR_RCD_IND = 1)