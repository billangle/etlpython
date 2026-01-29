WITH vault AS
  (WITH SUBQUERY AS
     (SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS
      WHERE DATE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                                 'YYYY-MM-DD')
        AND DATE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                                                'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.AG_PROD_PLAN_HS
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (AG_PROD_PLAN_HS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        OR DATE (AG_PROD_PLAN_HS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                  'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.CARS_TR_HS
      JOIN edv.AG_PROD_PLAN_HS ON (AG_PROD_PLAN_HS.CLU_TR_ID = CARS_TR_HS.TR_ID)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (CARS_TR_HS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                               'YYYY-MM-DD')
        OR DATE (CARS_TR_HS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                             'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.TR_LOC_FSA_CNTY_LS
      JOIN edv.TR_LOC_FSA_CNTY_L ON (TR_LOC_FSA_CNTY_L.TR_LOC_FSA_CNTY_L_ID = TR_LOC_FSA_CNTY_LS.TR_LOC_FSA_CNTY_L_ID)
      JOIN edv.TR_H ON (TR_H.TR_H_ID = TR_LOC_FSA_CNTY_L.TR_H_ID)
      JOIN edv.AG_PROD_PLAN_HS ON (COALESCE (AG_PROD_PLAN_HS.ST_FSA_CD,
                                             '--') = TR_H.ST_FSA_CD
                                   AND COALESCE (AG_PROD_PLAN_HS.CNTY_FSA_CD,
                                                 '--') = TR_H.CNTY_FSA_CD
                                   AND COALESCE (AG_PROD_PLAN_HS.TR_NBR,
                                                 '-1') = TR_H.TR_NBR
                                   AND AG_PROD_PLAN_HS.PGM_YR = TR_LOC_FSA_CNTY_LS.PGM_YR)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
        OR DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                     'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.LOC_AREA_MRT_SRC_RS loc_1
      JOIN edv.TR_LOC_FSA_CNTY_L ON (TR_LOC_FSA_CNTY_L.LOC_ST_FSA_CD = loc_1.CTRY_DIV_MRT_CD
                                     AND TR_LOC_FSA_CNTY_L.LOC_CNTY_FSA_CD = loc_1.LOC_AREA_MRT_CD
                                     AND loc_1.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
      JOIN edv.TR_LOC_FSA_CNTY_LS ON (TR_LOC_FSA_CNTY_L.TR_LOC_FSA_CNTY_L_ID = TR_LOC_FSA_CNTY_LS.TR_LOC_FSA_CNTY_L_ID)
      JOIN edv.TR_H ON (TR_H.TR_H_ID = TR_LOC_FSA_CNTY_L.TR_H_ID)
      JOIN edv.AG_PROD_PLAN_HS ON (COALESCE (AG_PROD_PLAN_HS.ST_FSA_CD,
                                             '--') = TR_H.ST_FSA_CD
                                   AND COALESCE (AG_PROD_PLAN_HS.CNTY_FSA_CD,
                                                 '--') = TR_H.CNTY_FSA_CD
                                   AND COALESCE (AG_PROD_PLAN_HS.TR_NBR,
                                                 '-1') = TR_H.TR_NBR
                                   AND AG_PROD_PLAN_HS.PGM_YR = TR_LOC_FSA_CNTY_LS.PGM_YR)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (loc_1.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                          'YYYY-MM-DD')
        OR DATE (loc_1.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                        'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.LOC_AREA_MRT_SRC_RS loc_2
      JOIN edv.AG_PROD_PLAN_HS ON (AG_PROD_PLAN_HS.ST_FSA_CD = loc_2.CTRY_DIV_MRT_CD
                                   AND AG_PROD_PLAN_HS.CNTY_FSA_CD = loc_2.LOC_AREA_MRT_CD
                                   AND loc_2.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (loc_2.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                          'YYYY-MM-DD')
        OR DATE (loc_2.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                        'YYYY-MM-DD')
      UNION SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.TR_HS
      JOIN edv.TR_H ON (TR_H.TR_H_ID = TR_HS.TR_H_ID)
      JOIN edv.CARS_TR_HS ON (COALESCE (CARS_TR_HS.TR_H_ID,
                                        '6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID)
      JOIN edv.AG_PROD_PLAN_HS ON (AG_PROD_PLAN_HS.CLU_TR_ID = CARS_TR_HS.TR_ID)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID)
      WHERE DATE (TR_HS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                          'YYYY-MM-DD')
        OR DATE (TR_HS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                        'YYYY-MM-DD')
      UNION SELECT DISTINCT CROP_ACRG_RPT_BUS_PTY_SHR_LS.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.CONT_PLAN_CERT_ELCT_LS CONT_PLAN_CERT_ELCT_LS
      JOIN edv.CONT_PLAN_CERT_ELCT_L CONT_PLAN_CERT_ELCT_L ON (CONT_PLAN_CERT_ELCT_L.CONT_PLAN_CERT_ELCT_L_ID = CONT_PLAN_CERT_ELCT_LS.CONT_PLAN_CERT_ELCT_L_ID)
      JOIN edv.CROP_ACRG_RPT_HS CROP_ACRG_RPT_HS ON (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_H_ID = CONT_PLAN_CERT_ELCT_L.CROP_ACRG_RPT_H_ID)
      JOIN edv.CARS_TR_HS CARS_TR_HS ON (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID = CARS_TR_HS.CROP_ACRG_RPT_ID)
      JOIN edv.AG_PROD_PLAN_HS AG_PROD_PLAN_HS ON (AG_PROD_PLAN_HS.FSA_CROP_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_CD
                                                   AND AG_PROD_PLAN_HS.FSA_CROP_TYPE_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_TYPE_CD
                                                   AND AG_PROD_PLAN_HS.INTN_USE_CD=CONT_PLAN_CERT_ELCT_L.INTN_USE_CD
                                                   AND COALESCE (AG_PROD_PLAN_HS.CLU_TR_ID,
                                                                 '-1') = CARS_TR_HS.TR_ID)
      JOIN edv.AG_PROD_PLAN_H AG_PROD_PLAN_H ON (AG_PROD_PLAN_H.AG_PROD_PLAN_H_ID = AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L CROP_ACRG_RPT_BUS_PTY_SHR_L ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID,
                                                                                     '6bb61e3b7bce0931da574d19d1d82c88') = AG_PROD_PLAN_H.AG_PROD_PLAN_H_ID
                                                                           AND CROP_ACRG_RPT_BUS_PTY_SHR_L.CORE_CUST_ID = CONT_PLAN_CERT_ELCT_L.CORE_CUST_ID)
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS CROP_ACRG_RPT_BUS_PTY_SHR_LS ON (CROP_ACRG_RPT_BUS_PTY_SHR_LS.BUS_PTY_ID=CONT_PLAN_CERT_ELCT_LS.BUS_PTY_ID
                                                                             AND CROP_ACRG_RPT_BUS_PTY_SHR_LS.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID)
      WHERE DATE (CONT_PLAN_CERT_ELCT_LS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                           'YYYY-MM-DD')
        OR DATE (CONT_PLAN_CERT_ELCT_LS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                         'YYYY-MM-DD')
      UNION SELECT DISTINCT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
      FROM edv.CROP_ACRG_RPT_BUS_PTY_LS
      JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS ON (CROP_ACRG_RPT_BUS_PTY_LS.BUS_PTY_ID = CROP_ACRG_RPT_BUS_PTY_SHR_LS.BUS_PTY_ID)
      WHERE DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                             'YYYY-MM-DD')
        AND DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                                            'YYYY-MM-DD') ) SELECT DISTINCT dm.AG_PROD_PLAN_SRGT_ID,
                                                                                                            dm.AG_PROD_PLAN_DURB_ID,
                                                                                                            dm.PGM_YR,
                                                                                                            dm.ADM_FSA_ST_CNTY_SRGT_ID,
                                                                                                            dm.ADM_FSA_ST_CNTY_DURB_ID,
                                                                                                            dm.FARM_SRGT_ID,
                                                                                                            dm.FARM_DURB_ID,
                                                                                                            dm.TR_SRGT_ID,
                                                                                                            dm.TR_DURB_ID,
                                                                                                            dm.LOC_FSA_ST_CNTY_SRGT_ID,
                                                                                                            dm.LOC_FSA_ST_CNTY_DURB_ID,
                                                                                                            dm.CONG_DIST_SRGT_ID,
                                                                                                            dm.CONG_DIST_DURB_ID,
                                                                                                            dm.CUST_SRGT_ID,
                                                                                                            dm.CUST_DURB_ID,
                                                                                                            dm.PRDR_INVL_SRGT_ID,
                                                                                                            dm.PRDR_INVL_DURB_ID,
                                                                                                            dm.AG_PROD_PLAN_BUS_PTY_CRE_DT,
                                                                                                            dm.AG_PROD_PLAN_PTY_LAST_CHG_DT,
                                                                                                            dm.AG_PROD_PLAN_PTY_LAST_CHG_USER,
                                                                                                            dm.LAND_UNIT_RMA_NBR,
                                                                                                            dm.CROP_SHR_PCT,
                                                                                                            dm.SRC_DATA_STAT_CD,
                                                                                                            dm.CONT_PLAN_CERT_ELCT_DT,
                                                                                                            dm.HEMP_LIC_NBR,
                                                                                                            dm.FLD_YR_SRGT_ID,
                                                                                                            dm.FLD_DURB_ID,
                                                                                                            dm.BUS_PTY_SRC_DATA_STAT_CD,
                                                                                                            dm.DATA_EFF_STRT_DT
   FROM
     (SELECT dm_sub.AG_PROD_PLAN_SRGT_ID,
             dm_sub.AG_PROD_PLAN_DURB_ID,
             dm_sub.PGM_YR,
             dm_sub.ADM_FSA_ST_CNTY_SRGT_ID,
             dm_sub.ADM_FSA_ST_CNTY_DURB_ID,
             dm_sub.FARM_SRGT_ID,
             dm_sub.FARM_DURB_ID,
             dm_sub.TR_SRGT_ID,
             dm_sub.TR_DURB_ID,
             dm_sub.LOC_FSA_ST_CNTY_SRGT_ID,
             dm_sub.LOC_FSA_ST_CNTY_DURB_ID,
             dm_sub.CONG_DIST_SRGT_ID,
             dm_sub.CONG_DIST_DURB_ID,
             dm_sub.CUST_SRGT_ID,
             dm_sub.CUST_DURB_ID,
             dm_sub.PRDR_INVL_SRGT_ID,
             dm_sub.PRDR_INVL_DURB_ID,
             dm_sub.AG_PROD_PLAN_BUS_PTY_CRE_DT,
             dm_sub.AG_PROD_PLAN_PTY_LAST_CHG_DT,
             dm_sub.AG_PROD_PLAN_PTY_LAST_CHG_USER,
             dm_sub.LAND_UNIT_RMA_NBR,
             dm_sub.CROP_SHR_PCT,
             dm_sub.SRC_DATA_STAT_CD,
             dm_sub.CONT_PLAN_CERT_ELCT_DT,
             dm_sub.HEMP_LIC_NBR,
             dm_sub.FLD_YR_SRGT_ID,
             dm_sub.FLD_DURB_ID,
             dm_sub.BUS_PTY_SRC_DATA_STAT_CD,
             dm_sub.DATA_EFF_STRT_DT,
             ROW_NUMBER () OVER (PARTITION BY dm_sub.AG_PROD_PLAN_DURB_ID,
                                              dm_sub.CUST_DURB_ID
                                 ORDER BY DV_DR_DT DESC, AG_PROD_PLAN_HS_DT DESC, CARS_TR_HS_DT DESC, TR_HS_DT DESC, TR_LOC_FSA_CNTY_LS_DT DESC, LOC_1_DT DESC, LOC_2_DT DESC) AS Row_Num_Part
      FROM
        (SELECT AG_PROD_PLAN_DIM.AG_PROD_PLAN_SRGT_ID AG_PROD_PLAN_SRGT_ID,
                AG_PROD_PLAN_DIM.AG_PROD_PLAN_DURB_ID AG_PROD_PLAN_DURB_ID,
                COALESCE (PGM_YR_DIM.PGM_YR,
                          0) PGM_YR,
                         COALESCE (dim2.FSA_ST_CNTY_SRGT_ID,
                                   '-3') ADM_FSA_ST_CNTY_SRGT_ID,
                                  COALESCE (dim2.FSA_ST_CNTY_DURB_ID,
                                            '-3') ADM_FSA_ST_CNTY_DURB_ID,
                                           COALESCE (FARM_DIM.FARM_SRGT_ID,
                                                     '-3') FARM_SRGT_ID,
                                                    COALESCE (FARM_DIM.FARM_DURB_ID,
                                                              '-3') FARM_DURB_ID,
                                                             COALESCE (TR_DIM.TR_SRGT_ID,
                                                                       '-3') TR_SRGT_ID,
                                                                      COALESCE (TR_DIM.TR_DURB_ID,
                                                                                '-3') TR_DURB_ID,
                                                                               COALESCE (dim1.FSA_ST_CNTY_SRGT_ID,
                                                                                         '-3') LOC_FSA_ST_CNTY_SRGT_ID,
                                                                                        COALESCE (dim1.FSA_ST_CNTY_DURB_ID,
                                                                                                  '-3') LOC_FSA_ST_CNTY_DURB_ID,
                                                                                                 COALESCE (CONG_DIST_DIM.CONG_DIST_SRGT_ID,
                                                                                                           '-3') CONG_DIST_SRGT_ID,
                                                                                                          COALESCE (CONG_DIST_DIM.CONG_DIST_DURB_ID,
                                                                                                                    '-3') CONG_DIST_DURB_ID,
                                                                                                                   CUST_DIM.CUST_SRGT_ID CUST_SRGT_ID,
                                                                                                                   CUST_DIM.CUST_DURB_ID CUST_DURB_ID,
                                                                                                                   COALESCE (PRDR_INVL_DIM.PRDR_INVL_SRGT_ID,
                                                                                                                             '-3') PRDR_INVL_SRGT_ID,
                                                                                                                            COALESCE (PRDR_INVL_DIM.PRDR_INVL_DURB_ID,
                                                                                                                                      '-3') PRDR_INVL_DURB_ID,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.SRC_CRE_DT AG_PROD_PLAN_BUS_PTY_CRE_DT,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.SRC_LAST_CHG_DT AG_PROD_PLAN_PTY_LAST_CHG_DT,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.SRC_LAST_CHG_USER_NM AG_PROD_PLAN_PTY_LAST_CHG_USER,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.LAND_UNIT_RMA_NBR LAND_UNIT_RMA_NBR,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.CROP_SHR_PCT CROP_SHR_PCT,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_STAT_CD SRC_DATA_STAT_CD,
                                                                                                                                     CASE
                                                                                                                                         WHEN CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD = 'A' THEN COALESCE (TO_NUMBER (TO_CHAR (CONT_PLAN_CERT_ELCT_LS.ELCT_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                       -1)
                                                                                                                                         ELSE -1
                                                                                                                                     END CONT_PLAN_CERT_ELCT_DT,
                                                                                                                                     CROP_ACRG_RPT_BUS_PTY_SHR_LS.HEMP_LIC_NBR,
                                                                                                                                     COALESCE (FLD_DM.FLD_YR_SRGT_ID,
                                                                                                                                               -3) FLD_YR_SRGT_ID,
                                                                                                                                              COALESCE (FLD_DM.FLD_DURB_ID,
                                                                                                                                                        -3) FLD_DURB_ID,
                                                                                                                                                       CROP_ACRG_RPT_BUS_PTY_LS.DATA_STAT_CD BUS_PTY_SRC_DATA_STAT_CD,
                                                                                                                                                       greatest (COALESCE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (AG_PROD_PLAN_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CARS_TR_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (TR_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (loc_1.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (loc_2.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD'))) DATA_EFF_STRT_DT,
                                                                                                                                                       row_number () OVER (PARTITION BY AG_PROD_PLAN_DIM.AG_PROD_PLAN_DURB_ID,
                                                                                                                                                                                        CUST_DIM.CUST_DURB_ID
                                                                                                                                                                           ORDER BY COALESCE (DATE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_STRT_DT),
                                                                                                                                                                                              TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                            'YYYY-MM-DD')) DESC, COALESCE (DATE (AG_PROD_PLAN_HS.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                           TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                         'YYYY-MM-DD')) DESC, COALESCE (DATE (TR_HS.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                                                                        TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                                                                      'YYYY-MM-DD')) DESC, COALESCE (DATE (CARS_TR_HS.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                                                                                                                     TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                                                                                                                   'YYYY-MM-DD')) DESC, COALESCE (DATE (loc_1.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                                                                                                                                                                  TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                                                                                                                                                                'YYYY-MM-DD')) DESC, COALESCE (DATE (loc_2.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                                                                                                                                                                                                               TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                                                                                                                                                                                                             'YYYY-MM-DD')) DESC, COALESCE (DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                            TO_TIMESTAMP ('1111-12-31',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'YYYY-MM-DD')) DESC, TR_LOC_FSA_CNTY_LS.data_stat_cd ASC,TR_LOC_FSA_CNTY_LS.src_last_chg_dt DESC, CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD ASC NULLS FIRST) AS rn,
                                                                                                                                                                          CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_STRT_DT DV_DR_DT,
                                                                                                                                                                          AG_PROD_PLAN_HS.DATA_EFF_STRT_DT AG_PROD_PLAN_HS_DT,
                                                                                                                                                                          CARS_TR_HS.DATA_EFF_STRT_DT CARS_TR_HS_DT,
                                                                                                                                                                          TR_HS.DATA_EFF_STRT_DT TR_HS_DT,
                                                                                                                                                                          TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT TR_LOC_FSA_CNTY_LS_DT,
                                                                                                                                                                          loc_1.DATA_EFF_STRT_DT LOC_1_DT,
                                                                                                                                                                          loc_2.DATA_EFF_STRT_DT LOC_2_DT
         FROM
           (SELECT all_dr_LS.*
            FROM
              (SELECT ls.*,
                      row_number () OVER (PARTITION BY CROP_ACRG_RPT_BUS_PTY_SHR_L_ID,
                                                       data_eff_strt_dt
                                          ORDER BY data_stat_cd) AS rn_dr
               FROM edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS ls
               WHERE CROP_ACRG_RPT_BUS_PTY_SHR_L_ID IN
                   (SELECT CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
                    FROM subquery) ) all_dr_LS
            WHERE 1 = 1
              AND rn_dr = 1 ) CROP_ACRG_RPT_BUS_PTY_SHR_LS
         LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L ON (CROP_ACRG_RPT_BUS_PTY_SHR_LS.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID = CROP_ACRG_RPT_BUS_PTY_SHR_L.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID)
         LEFT JOIN edv.AG_PROD_PLAN_H ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_SHR_L.AG_PROD_PLAN_H_ID,
                                                    '6bb61e3b7bce0931da574d19d1d82c88') = AG_PROD_PLAN_H.AG_PROD_PLAN_H_ID)
         LEFT JOIN ebv.PRDR_INVL_RH ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.BUS_PTY_TYPE_CD,
                                                       '[NULL IN SOURCE]') = PRDR_INVL_RH.PRDR_INVL_CD)
         LEFT JOIN edv.AG_PROD_PLAN_HS AG_PROD_PLAN_HS ON (AG_PROD_PLAN_H.AG_PROD_PLAN_H_ID = AG_PROD_PLAN_HS.AG_PROD_PLAN_H_ID
                                                           AND AG_PROD_PLAN_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                               'YYYY-MM-DD')
                                                           AND DATE (AG_PROD_PLAN_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                        'YYYY-MM-DD'))
         LEFT JOIN edv.TR_LOC_FSA_CNTY_LS TR_LOC_FSA_CNTY_LS ON (COALESCE (AG_PROD_PLAN_HS.PGM_YR,
                                                                           '-3') = COALESCE (TR_LOC_FSA_CNTY_LS.PGM_YR,
                                                                                             '-3')
                                                                 AND COALESCE (AG_PROD_PLAN_HS.CLU_TR_ID,
                                                                               '-1') = TR_LOC_FSA_CNTY_LS.TR_ID
                                                                 AND TR_LOC_FSA_CNTY_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                        'YYYY-MM-DD')
                                                                 AND DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                                 'YYYY-MM-DD'))
         LEFT JOIN edv.CORE_CUST_H ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_SHR_L.CORE_CUST_ID,
                                                 '-1') = CORE_CUST_H.CORE_CUST_ID)
         LEFT JOIN edv.TR_H ON (COALESCE (AG_PROD_PLAN_HS.ST_FSA_CD,
                                          '--') = TR_H.ST_FSA_CD
                                AND COALESCE (AG_PROD_PLAN_HS.CNTY_FSA_CD,
                                              '--') = TR_H.CNTY_FSA_CD
                                AND COALESCE (AG_PROD_PLAN_HS.TR_NBR,
                                              '-1') = TR_H.TR_NBR)
         LEFT JOIN edv.FARM_H ON (COALESCE (AG_PROD_PLAN_HS.ST_FSA_CD,
                                            '--') = FARM_H.ST_FSA_CD
                                  AND COALESCE (AG_PROD_PLAN_HS.CNTY_FSA_CD,
                                                '--') = FARM_H.CNTY_FSA_CD
                                  AND COALESCE (AG_PROD_PLAN_HS.FARM_NBR,
                                                '--') = FARM_H.FARM_NBR)
         LEFT JOIN edv.CARS_TR_HS CARS_TR_HS ON (COALESCE (AG_PROD_PLAN_HS.CLU_TR_ID,
                                                           '-1') = CARS_TR_HS.TR_ID
                                                 AND CARS_TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                'YYYY-MM-DD')
                                                 AND DATE (CARS_TR_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                         'YYYY-MM-DD'))
         LEFT JOIN edv.TR_H CARS_TR_H ON (COALESCE (CARS_TR_HS.TR_H_ID,
                                                    '6de912369edfb89b50859d8f305e4f72') = CARS_TR_H.TR_H_ID)
         LEFT JOIN edv.TR_HS ON (CARS_TR_H.TR_H_ID = TR_HS.TR_H_ID
                                 AND TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                           'YYYY-MM-DD')
                                 AND DATE (TR_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                    'YYYY-MM-DD'))
         JOIN edv.TR_LOC_FSA_CNTY_L TR_LOC_FSA_CNTY_L ON (TR_LOC_FSA_CNTY_LS.TR_LOC_FSA_CNTY_L_ID = TR_LOC_FSA_CNTY_L.TR_LOC_FSA_CNTY_L_ID
                                                          AND TR_LOC_FSA_CNTY_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                 'YYYY-MM-DD')
                                                          AND DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                          'YYYY-MM-DD')
                                                          AND (TR_H.TR_H_ID) = COALESCE (TR_LOC_FSA_CNTY_L.TR_H_ID,
                                                                                         '--'))
         LEFT JOIN edv.LOC_AREA_MRT_SRC_RS loc_1 ON (COALESCE (TR_LOC_FSA_CNTY_L.LOC_ST_FSA_CD,
                                                               '-1') = loc_1.CTRY_DIV_MRT_CD
                                                     AND COALESCE (TR_LOC_FSA_CNTY_L.LOC_CNTY_FSA_CD,
                                                                   '-1') = loc_1.LOC_AREA_MRT_CD
                                                     AND loc_1.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                     AND loc_1.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                               'YYYY-MM-DD')
                                                     AND DATE (loc_1.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                        'YYYY-MM-DD'))
         LEFT JOIN edv.LOC_AREA_MRT_SRC_RS loc_2 ON (AG_PROD_PLAN_HS.ST_FSA_CD = loc_2.CTRY_DIV_MRT_CD
                                                     AND AG_PROD_PLAN_HS.CNTY_FSA_CD = loc_2.LOC_AREA_MRT_CD
                                                     AND loc_2.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                     AND loc_2.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                               'YYYY-MM-DD')
                                                     AND DATE (loc_2.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                        'YYYY-MM-DD'))
         LEFT JOIN edv.LOC_AREA_RH loc_ar1 ON (COALESCE (loc_1.LOC_AREA_CAT_NM,
                                                         '[NULL IN SOURCE]') = loc_ar1.LOC_AREA_CAT_NM
                                               AND COALESCE (loc_1.LOC_AREA_NM,
                                                             '[NULL IN SOURCE]') = loc_ar1.LOC_AREA_NM
                                               AND COALESCE (loc_1.CTRY_DIV_NM,
                                                             '[NULL IN SOURCE]') = loc_ar1.CTRY_DIV_NM)
         LEFT JOIN edv.LOC_AREA_RH loc_ar2 ON (COALESCE (loc_2.LOC_AREA_CAT_NM,
                                                         '[NULL IN SOURCE]') = loc_ar2.LOC_AREA_CAT_NM
                                               AND COALESCE (loc_2.LOC_AREA_NM,
                                                             '[NULL IN SOURCE]') = loc_ar2.LOC_AREA_NM
                                               AND COALESCE (loc_2.CTRY_DIV_NM,
                                                             '[NULL IN SOURCE]') = loc_ar2.CTRY_DIV_NM)
         LEFT JOIN cmn_dim_dm_stg.PGM_YR_DIM ON (COALESCE (AG_PROD_PLAN_HS.PGM_YR,
                                                           '-1') = PGM_YR_DIM.PGM_YR
                                                 AND PGM_YR_DIM.DATA_STAT_CD='A')
         LEFT JOIN cmn_dim_dm_stg.CONG_DIST_DIM CONG_DIST_DIM ON (TR_LOC_FSA_CNTY_L.LOC_ST_FSA_CD = CONG_DIST_DIM.ST_FSA_CD
                                                                  AND TR_HS.CONG_DIST_CD = CONG_DIST_DIM.CONG_DIST_CD
                                                                  AND CONG_DIST_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.AG_PROD_PLAN_DIM ON (AG_PROD_PLAN_H.DURB_ID = AG_PROD_PLAN_DIM.AG_PROD_PLAN_DURB_ID
                                               AND AG_PROD_PLAN_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.CUST_DIM ON (CORE_CUST_H.DURB_ID = CUST_DIM.CUST_DURB_ID
                                               AND CUST_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.FARM_DIM ON (FARM_H.DURB_ID = FARM_DIM.FARM_DURB_ID
                                               AND FARM_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM dim1 ON (loc_ar1.DURB_ID = dim1.FSA_ST_CNTY_DURB_ID
                                                           AND dim1.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM dim2 ON (loc_ar2.DURB_ID = dim2.FSA_ST_CNTY_DURB_ID
                                                           AND dim2.CUR_RCD_IND = 1)
         LEFT JOIN FARM_DM_STG.PRDR_INVL_DIM ON (PRDR_INVL_RH.DURB_ID = PRDR_INVL_DIM.PRDR_INVL_DURB_ID
                                                  AND PRDR_INVL_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.TR_DIM ON (TR_H.DURB_ID = TR_DIM.TR_DURB_ID
                                             AND TR_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.CROP_ACRG_RPT_HS CROP_ACRG_RPT_HS ON (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID = CARS_TR_HS.CROP_ACRG_RPT_ID
                                                             AND CROP_ACRG_RPT_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                  'YYYY-MM-DD')
                                                             AND DATE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                           'YYYY-MM-DD'))
         LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_LS CROP_ACRG_RPT_BUS_PTY_LS ON (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID = CROP_ACRG_RPT_BUS_PTY_LS.CROP_ACRG_RPT_ID
                                                                             AND CROP_ACRG_RPT_BUS_PTY_SHR_LS.BUS_PTY_ID = CROP_ACRG_RPT_BUS_PTY_LS.BUS_PTY_ID
                                                                             AND CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                                          'YYYY-MM-DD')
                                                                             AND DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                                                   'YYYY-MM-DD'))
         LEFT JOIN edv.CONT_PLAN_CERT_ELCT_L CONT_PLAN_CERT_ELCT_L ON (AG_PROD_PLAN_HS.FSA_CROP_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_CD
                                                                       AND AG_PROD_PLAN_HS.FSA_CROP_TYPE_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_TYPE_CD
                                                                       AND AG_PROD_PLAN_HS.INTN_USE_CD=CONT_PLAN_CERT_ELCT_L.INTN_USE_CD
                                                                       AND CROP_ACRG_RPT_BUS_PTY_SHR_L.CORE_CUST_ID = CONT_PLAN_CERT_ELCT_L.CORE_CUST_ID
                                                                       AND CROP_ACRG_RPT_HS.CROP_ACRG_RPT_H_ID = CONT_PLAN_CERT_ELCT_L.CROP_ACRG_RPT_H_ID)
         LEFT JOIN edv.CONT_PLAN_CERT_ELCT_LS CONT_PLAN_CERT_ELCT_LS ON (CROP_ACRG_RPT_BUS_PTY_SHR_LS.BUS_PTY_ID=CONT_PLAN_CERT_ELCT_LS.BUS_PTY_ID
                                                                         AND CONT_PLAN_CERT_ELCT_L.CONT_PLAN_CERT_ELCT_L_ID = CONT_PLAN_CERT_ELCT_LS.CONT_PLAN_CERT_ELCT_L_ID
                                                                         AND CONT_PLAN_CERT_ELCT_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                                    'YYYY-MM-DD')
                                                                         AND DATE (CONT_PLAN_CERT_ELCT_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                                             'YYYY-MM-DD'))
         LEFT JOIN edv.FLD_HS FHS ON (COALESCE (AG_PROD_PLAN_HS.CLU_TR_ID,
                                                -1) = COALESCE (FHS.TR_ID,
                                                                -1)
                                      AND COALESCE (AG_PROD_PLAN_HS.FLD_NBR,
                                                    '--') = COALESCE (FHS.FLD_NBR,
                                                                      '--')
                                      AND FHS.DATA_STAT_CD = 'A'
                                      AND FHS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                              'YYYY-MM-DD')
                                      AND DATE (FHS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                       'YYYY-MM-DD'))
         LEFT JOIN edv.FLD_H FH ON (COALESCE (FHS.FLD_H_ID,
                                              '-1') = COALESCE (FH.FLD_H_ID,
                                                              '-1'))
         LEFT JOIN cmn_dim_dm_stg.FLD_YR_DIM FLD_DM ON (COALESCE (FH.DURB_ID,
                                                                  -1) = COALESCE (FLD_DM.FLD_DURB_ID,
                                                                                  -1)
                                                        AND COALESCE (AG_PROD_PLAN_HS.PGM_YR,
                                                                      0) = COALESCE (FLD_DM.PGM_YR,
                                                                                     0)
                                                        AND DATE (AG_PROD_PLAN_HS.DATA_EFF_END_DT) = TO_TIMESTAMP ('9999-12-31',
                                                                                                                   'YYYY-MM-DD')
                                                        AND FLD_DM.CUR_RCD_IND = 1)
         WHERE CUST_DIM.CUST_DURB_ID IS NOT NULL
           AND AG_PROD_PLAN_DIM.AG_PROD_PLAN_DURB_ID IS NOT NULL
           AND COALESCE (DATE (CROP_ACRG_RPT_BUS_PTY_SHR_LS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (AG_PROD_PLAN_HS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (CARS_TR_HS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (TR_HS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (loc_1.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (loc_2.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD') ) dm_sub
      WHERE dm_sub.rn = 1 ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.AG_PROD_PLAN_DURB_ID
  FROM CAR_DM_STG.AG_PROD_PLAN_BUS_PTY_FACT mart
  JOIN vault
  ON vault.AG_PROD_PLAN_DURB_ID = mart.AG_PROD_PLAN_DURB_ID
  WHERE vault.CUST_DURB_ID = mart.CUST_DURB_ID
     AND vault.ag_prod_plan_srgt_id IS NOT NULL AND vault.ag_prod_plan_durb_id IS NOT NULL AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS NOT NULL AND vault.tr_srgt_id IS NOT NULL AND vault.tr_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_srgt_id IS NOT NULL AND vault.loc_fsa_st_cnty_durb_id IS NOT NULL AND vault.cong_dist_srgt_id IS NOT NULL AND vault.cong_dist_durb_id IS NOT NULL AND vault.cust_srgt_id IS NOT NULL AND vault.cust_durb_id IS NOT NULL AND vault.prdr_invl_srgt_id IS NOT NULL AND vault.prdr_invl_durb_id IS NOT NULL
     )
INSERT INTO CAR_DM_STG.AG_PROD_PLAN_BUS_PTY_FACT (CRE_DT, LAST_CHG_DT, DATA_STAT_CD, AG_PROD_PLAN_SRGT_ID, AG_PROD_PLAN_DURB_ID, PGM_YR, ADM_FSA_ST_CNTY_SRGT_ID, ADM_FSA_ST_CNTY_DURB_ID, FARM_SRGT_ID, FARM_DURB_ID, TR_SRGT_ID, TR_DURB_ID, LOC_FSA_ST_CNTY_SRGT_ID, LOC_FSA_ST_CNTY_DURB_ID, CONG_DIST_SRGT_ID, CONG_DIST_DURB_ID, CUST_SRGT_ID, CUST_DURB_ID, PRDR_INVL_SRGT_ID, PRDR_INVL_DURB_ID, AG_PROD_PLAN_BUS_PTY_CRE_DT, AG_PROD_PLAN_PTY_LAST_CHG_DT, AG_PROD_PLAN_PTY_LAST_CHG_USER, LAND_UNIT_RMA_NBR, CROP_SHR_PCT, SRC_DATA_STAT_CD, CONT_PLAN_CERT_ELCT_DT, HEMP_LIC_NBR, FLD_YR_SRGT_ID, FLD_DURB_ID, BUS_PTY_SRC_DATA_STAT_CD)
SELECT  CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'A',
        vault.AG_PROD_PLAN_SRGT_ID,   
        vault.AG_PROD_PLAN_DURB_ID,   
        vault.PGM_YR,
        vault.ADM_FSA_ST_CNTY_SRGT_ID,
        vault.ADM_FSA_ST_CNTY_DURB_ID,
        vault.FARM_SRGT_ID,
        vault.FARM_DURB_ID,
        vault.TR_SRGT_ID,
        vault.TR_DURB_ID,
        vault.LOC_FSA_ST_CNTY_SRGT_ID,
        vault.LOC_FSA_ST_CNTY_DURB_ID,
        vault.CONG_DIST_SRGT_ID,      
        vault.CONG_DIST_DURB_ID,      
        vault.CUST_SRGT_ID,
        vault.CUST_DURB_ID,
        vault.PRDR_INVL_SRGT_ID,      
        vault.PRDR_INVL_DURB_ID,
        vault.AG_PROD_PLAN_BUS_PTY_CRE_DT,
        vault.AG_PROD_PLAN_PTY_LAST_CHG_DT,
        vault.AG_PROD_PLAN_PTY_LAST_CHG_USER,
        vault.LAND_UNIT_RMA_NBR,
        vault.CROP_SHR_PCT,
        vault.SRC_DATA_STAT_CD,
        vault.CONT_PLAN_CERT_ELCT_DT,
        vault.HEMP_LIC_NBR,
        vault.FLD_YR_SRGT_ID,
        vault.FLD_DURB_ID,
        vault.BUS_PTY_SRC_DATA_STAT_CD
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.AG_PROD_PLAN_DURB_ID = mart.AG_PROD_PLAN_DURB_ID
       AND vault.CUST_DURB_ID = mart.CUST_DURB_ID )
    AND vault.ag_prod_plan_srgt_id IS NOT NULL AND vault.ag_prod_plan_durb_id IS NOT NULL AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS NOT NULL AND vault.tr_srgt_id IS NOT NULL AND vault.tr_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_srgt_id IS NOT NULL AND vault.loc_fsa_st_cnty_durb_id IS NOT NULL AND vault.cong_dist_srgt_id IS NOT NULL AND vault.cong_dist_durb_id IS NOT NULL AND vault.cust_srgt_id IS NOT NULL AND vault.cust_durb_id IS NOT NULL AND vault.prdr_invl_srgt_id IS NOT NULL AND vault.prdr_invl_durb_id IS NOT NULL