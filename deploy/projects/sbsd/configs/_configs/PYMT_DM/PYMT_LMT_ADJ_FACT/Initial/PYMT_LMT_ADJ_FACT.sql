SELECT      DISTINCT DM.DATA_EFF_STRT_DT,
            DM.CUST_SRGT_ID,
            DM.CUST_DURB_ID,
            DM.PYMT_LMT_ADJ_TYPE_SRGT_ID,
            DM.PYMT_LMT_ADJ_TYPE_DURB_ID,
            DM.PGM_YR,
            DM.LTD_PYMT_PGM_SRGT_ID,
            DM.LTD_PYMT_PGM_DURB_ID,
            DM.FSA_ST_CNTY_DURB_ID,
            DM.FSA_ST_CNTY_SRGT_ID,
            DM.SRC_LAST_CHG_DT,
            DM.PYMT_LMT_ADJ_NOTE_TXT,
            DM.PYMT_LMT_ADJ_AMT,
            DM.DATA_STAT_CD,
            DM.SRC_LAST_CHG_USER_NM
FROM        (
                SELECT              /*+ PARALLEL(2) */ DISTINCT PRDR_PGM_PYMT_LMT_ADJ_LS.DATA_EFF_STRT_DT,
                                    (CUST_DIM.CUST_SRGT_ID, -3) AS CUST_SRGT_ID,
                                    (CUST_DIM.CUST_DURB_ID, -3) AS CUST_DURB_ID,
                                    (PYMT_LMT_ADJ_TYPE_DIM.PYMT_LMT_ADJ_TYPE_SRGT_ID, -3) AS PYMT_LMT_ADJ_TYPE_SRGT_ID,
                                    (PYMT_LMT_ADJ_TYPE_DIM.PYMT_LMT_ADJ_TYPE_DURB_ID, -3) AS PYMT_LMT_ADJ_TYPE_DURB_ID,
                                    PRDR_PGM_PYMT_LMT_ADJ_L.SBSD_PRD_STRT_YR AS PGM_YR,
                                    (LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_SRGT_ID, -3) AS LTD_PYMT_PGM_SRGT_ID,
                                    (LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_DURB_ID, -3) AS LTD_PYMT_PGM_DURB_ID,
                                    (FSA_ST_CNTY_DIM.FSA_ST_CNTY_DURB_ID, -3) AS FSA_ST_CNTY_DURB_ID,
                                    (FSA_ST_CNTY_DIM.FSA_ST_CNTY_SRGT_ID, -3) AS FSA_ST_CNTY_SRGT_ID,
                                    DT_DIM.DT_ID AS SRC_LAST_CHG_DT,
                                    PRDR_PGM_PYMT_LMT_ADJ_LS.NOTE_TXT AS PYMT_LMT_ADJ_NOTE_TXT,
                                    PRDR_PGM_PYMT_LMT_ADJ_LS.PYMT_LMT_ADJ_AMT AS PYMT_LMT_ADJ_AMT,
                                    PRDR_PGM_PYMT_LMT_ADJ_LS.DATA_STAT_CD AS DATA_STAT_CD,
                                    PRDR_PGM_PYMT_LMT_ADJ_LS.SRC_LAST_CHG_USER_NM,
                                    ROW_NUMBER() OVER
                                    (
                                        PARTITION BY    CUST_DURB_ID, 
                                                        LTD_PYMT_PGM_DURB_ID, 
                                                        PRDR_PGM_PYMT_LMT_ADJ_L.SBSD_PRD_STRT_YR, 
                                                        PYMT_LMT_ADJ_TYPE_DURB_ID
                                        ORDER BY        CUST_DURB_ID, 
                                                        LTD_PYMT_PGM_DURB_ID, 
                                                        PRDR_PGM_PYMT_LMT_ADJ_L.SBSD_PRD_STRT_YR, 
                                                        PYMT_LMT_ADJ_TYPE_DURB_ID,
                                                        PRDR_PGM_PYMT_LMT_ADJ_LS.DATA_EFF_STRT_DT DESC  
                                    ) AS ROW_NUM_PART
                FROM                EDV.PRDR_PGM_PYMT_LMT_ADJ_LS
                                    LEFT OUTER JOIN EDV.PRDR_PGM_PYMT_LMT_ADJ_L ON PRDR_PGM_PYMT_LMT_ADJ_LS.PRDR_PGM_PYMT_LMT_ADJ_L_ID = PRDR_PGM_PYMT_LMT_ADJ_L.PRDR_PGM_PYMT_LMT_ADJ_L_ID
                                    LEFT OUTER JOIN cmn_dim_dm_stg.CUST_DIM ON ( PRDR_PGM_PYMT_LMT_ADJ_L.CORE_CUST_ID = CUST_DIM.CORE_CUST_ID AND  CUST_DIM.CUR_RCD_IND = 1 )
                                    LEFT OUTER JOIN EDV.CORE_CUST_RPT_LOC_AREA_L ON PRDR_PGM_PYMT_LMT_ADJ_L.CORE_CUST_ID = cast (CORE_CUST_RPT_LOC_AREA_L.CORE_CUST_ID as numeric)
                                    LEFT OUTER JOIN EDV.CORE_CUST_RPT_LOC_AREA_LS ON CORE_CUST_RPT_LOC_AREA_L.CORE_CUST_RPT_LOC_AREA_L_ID = CORE_CUST_RPT_LOC_AREA_LS.CORE_CUST_RPT_LOC_AREA_L_ID
                                    LEFT OUTER JOIN EDV.SBSD_LOC_AREA_RS ON (CORE_CUST_RPT_LOC_AREA_LS.CNTY_FSA_SVC_CTR_ID) = cast(SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID as numeric)
                                    LEFT OUTER JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM  ON CONCAT(SBSD_LOC_AREA_RS.ST_CNTY_FSA_CD, 'X') = CONCAT(FSA_ST_CNTY_DIM.ST_FSA_CD, FSA_ST_CNTY_DIM.CNTY_FSA_CD) AND FSA_ST_CNTY_DIM.CUR_RCD_IND = 1
                                    LEFT OUTER JOIN EDV.PYMT_PGM_RH ON PRDR_PGM_PYMT_LMT_ADJ_L.LTD_PYMT_PGM_NM = PYMT_PGM_RH.PYMT_PGM_NM
                                    LEFT OUTER JOIN pymt_dm_stg.LTD_PYMT_PGM_DIM ON ( LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_DURB_ID = PYMT_PGM_RH.DURB_ID  AND  LTD_PYMT_PGM_DIM.CUR_RCD_IND = 1 )
                                    LEFT OUTER JOIN EDV.PRDR_PGM_PYMT_LMT_ADJ_TYPE_RH ON PRDR_PGM_PYMT_LMT_ADJ_L.PRDR_PYMT_LMT_ADJ_TYPE_CD = PRDR_PGM_PYMT_LMT_ADJ_TYPE_RH.PRDR_PYMT_LMT_ADJ_TYPE_CD
                                    LEFT OUTER JOIN pymt_dm_stg.PYMT_LMT_ADJ_TYPE_DIM ON (PRDR_PGM_PYMT_LMT_ADJ_TYPE_RH.DURB_ID = PYMT_LMT_ADJ_TYPE_DIM.PYMT_LMT_ADJ_TYPE_DURB_ID  AND  PYMT_LMT_ADJ_TYPE_DIM.CUR_RCD_IND = 1 )
                                    LEFT OUTER JOIN cmn_dim_dm_stg.DT_DIM ON cast(TO_CHAR(PRDR_PGM_PYMT_LMT_ADJ_LS.SRC_LAST_CHG_DT,'YYYYMMDD') as numeric) = DT_DIM.DT_ID
                WHERE               PRDR_PGM_PYMT_LMT_ADJ_LS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
                        AND         (PRDR_PGM_PYMT_LMT_ADJ_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')
                        AND         CUST_DURB_ID IS NOT NULL
                        AND         LTD_PYMT_PGM_DURB_ID IS NOT NULL
                        AND         PRDR_PGM_PYMT_LMT_ADJ_L.SBSD_PRD_STRT_YR IS NOT NULL
                        AND         PYMT_LMT_ADJ_TYPE_DURB_ID IS NOT NULL
            ) DM
WHERE       DM.ROW_NUM_PART = 1