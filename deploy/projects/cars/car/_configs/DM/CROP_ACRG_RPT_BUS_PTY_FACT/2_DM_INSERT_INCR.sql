WITH vault AS
  (SELECT DISTINCT dm.CROP_ACRG_RPT_SRGT_ID,
                   dm.CROP_ACRG_RPT_DURB_ID,
                   dm.PGM_YR,
                   dm.ADM_FSA_ST_CNTY_SRGT_ID,
                   dm.ADM_FSA_ST_CNTY_DURB_ID,
                   dm.FARM_SRGT_ID,
                   dm.FARM_DURB_ID,
                   dm.CUST_SRGT_ID,
                   dm.CUST_DURB_ID,
                   dm.PRDR_INVL_SRGT_ID,
                   dm.PRDR_INVL_DURB_ID,
                   dm.CROP_ACRG_RPT_BUS_PTY_CRE_DT,
                   dm.CROP_ACRG_RPT_PTY_LAST_CHG_DT,
                   dm.CROP_ACRG_PTY_LAST_CHG_USER_NM,
                   dm.SRC_DATA_STAT_CD,
                   dm.DATA_EFF_STRT_DT
   FROM
     (SELECT CROP_ACRG_RPT_DIM.CROP_ACRG_RPT_SRGT_ID CROP_ACRG_RPT_SRGT_ID,
             CROP_ACRG_RPT_DIM.CROP_ACRG_RPT_DURB_ID CROP_ACRG_RPT_DURB_ID,
             COALESCE (PGM_YR_DIM.PGM_YR,
                       0) PGM_YR,
                      COALESCE (FSA_ST_CNTY_DIM.FSA_ST_CNTY_SRGT_ID,
                                '-3') ADM_FSA_ST_CNTY_SRGT_ID,
                               COALESCE (FSA_ST_CNTY_DIM.FSA_ST_CNTY_DURB_ID,
                                         '-3') ADM_FSA_ST_CNTY_DURB_ID,
                                        COALESCE (FARM_DIM.FARM_SRGT_ID,
                                                  '-3') FARM_SRGT_ID,
                                                 COALESCE (FARM_DIM.FARM_DURB_ID,
                                                           '-3') FARM_DURB_ID,
                                                          CUST_DIM.CUST_SRGT_ID CUST_SRGT_ID,
                                                          CUST_DIM.CUST_DURB_ID CUST_DURB_ID,
                                                          COALESCE (PRDR_INVL_DIM.PRDR_INVL_DURB_ID,
                                                                    '-3') PRDR_INVL_DURB_ID,
                                                                   COALESCE (PRDR_INVL_DIM.PRDR_INVL_SRGT_ID,
                                                                             '-3') PRDR_INVL_SRGT_ID,
                                                                            CROP_ACRG_RPT_BUS_PTY_LS.SRC_CRE_DT CROP_ACRG_RPT_BUS_PTY_CRE_DT,
                                                                            CROP_ACRG_RPT_BUS_PTY_LS.SRC_LAST_CHG_DT CROP_ACRG_RPT_PTY_LAST_CHG_DT,
                                                                            CROP_ACRG_RPT_BUS_PTY_LS.SRC_LAST_CHG_USER_NM CROP_ACRG_PTY_LAST_CHG_USER_NM,
                                                                            CROP_ACRG_RPT_BUS_PTY_LS.DATA_STAT_CD SRC_DATA_STAT_CD,
                                                                            CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_STRT_DT DATA_EFF_STRT_DT,
                                                                            ROW_NUMBER () OVER (PARTITION BY CROP_ACRG_RPT_DIM.CROP_ACRG_RPT_DURB_ID,
                                                                                                             CUST_DIM.CUST_DURB_ID
                                                                                                ORDER BY CROP_ACRG_RPT_BUS_PTY_LS.SRC_LAST_CHG_DT DESC) AS Row_Num_Part
      FROM edv.CROP_ACRG_RPT_BUS_PTY_LS
      LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_L ON (CROP_ACRG_RPT_BUS_PTY_LS.CROP_ACRG_RPT_BUS_PTY_L_ID = CROP_ACRG_RPT_BUS_PTY_L.CROP_ACRG_RPT_BUS_PTY_L_ID)
      LEFT JOIN edv.CROP_ACRG_RPT_H ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_L.CROP_ACRG_RPT_H_ID,
                                                  '1cc552b48373871758d99e3ecfe05b70') = CROP_ACRG_RPT_H.CROP_ACRG_RPT_H_ID)
      LEFT JOIN ebv.PRDR_INVL_RH ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_LS.BUS_PTY_TYPE_CD,
                                                    '[NULL IN SOURCE]') = PRDR_INVL_RH.PRDR_INVL_CD)
      LEFT JOIN edv.FARM_H ON (COALESCE (CROP_ACRG_RPT_H.ST_FSA_CD,
                                         '--') = FARM_H.ST_FSA_CD
                               AND COALESCE (CROP_ACRG_RPT_H.CNTY_FSA_CD,
                                             '--') = FARM_H.CNTY_FSA_CD
                               AND COALESCE (CROP_ACRG_RPT_H.FARM_NBR,
                                             '--') = FARM_H.FARM_NBR)
      LEFT JOIN edv.CORE_CUST_H ON (COALESCE (CROP_ACRG_RPT_BUS_PTY_L.CORE_CUST_ID,
                                              '-1') = CORE_CUST_H.CORE_CUST_ID)
      LEFT JOIN edv.LOC_AREA_MRT_SRC_RS ON (CROP_ACRG_RPT_H.ST_FSA_CD = LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD
                                            AND CROP_ACRG_RPT_H.CNTY_FSA_CD = LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD
                                            AND LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                            AND LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                    'YYYY-MM-DD')
                                            AND DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                             'YYYY-MM-DD'))
      LEFT JOIN edv.LOC_AREA_RH ON (COALESCE (LOC_AREA_MRT_SRC_RS.LOC_AREA_CAT_NM,
                                              '[NULL IN SOURCE]') = LOC_AREA_RH.LOC_AREA_CAT_NM
                                    AND COALESCE (LOC_AREA_MRT_SRC_RS.LOC_AREA_NM,
                                                  '[NULL IN SOURCE]') = LOC_AREA_RH.LOC_AREA_NM
                                    AND COALESCE (LOC_AREA_MRT_SRC_RS.CTRY_DIV_NM,
                                                  '[NULL IN SOURCE]') = LOC_AREA_RH.CTRY_DIV_NM
                                    AND LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                    AND LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                            'YYYY-MM-DD')
                                    AND DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                     'YYYY-MM-DD'))
      LEFT JOIN cmn_dim_dm_stg.PGM_YR_DIM ON (COALESCE (CROP_ACRG_RPT_H.PGM_YR,
                                                        '-1') = PGM_YR_DIM.PGM_YR
                                              AND PGM_YR_DIM.DATA_STAT_CD='A')
      LEFT JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM ON (LOC_AREA_RH.DURB_ID = FSA_ST_CNTY_DIM.FSA_ST_CNTY_DURB_ID
                                                   AND FSA_ST_CNTY_DIM.CUR_RCD_IND = 1)
      LEFT JOIN FARM_DM_STG.PRDR_INVL_DIM ON (PRDR_INVL_RH.DURB_ID = PRDR_INVL_DIM.PRDR_INVL_DURB_ID
                                               AND PRDR_INVL_DIM.CUR_RCD_IND = 1)
      LEFT JOIN cmn_dim_dm_stg.CUST_DIM ON (CORE_CUST_H.DURB_ID = CUST_DIM.CUST_DURB_ID
                                            AND CUST_DIM.CUR_RCD_IND = 1)
      LEFT JOIN cmn_dim_dm_stg.FARM_DIM ON (FARM_H.DURB_ID = FARM_DIM.FARM_DURB_ID
                                            AND FARM_DIM.CUR_RCD_IND = 1)
      LEFT JOIN CAR_DM_STG.CROP_ACRG_RPT_DIM ON (CROP_ACRG_RPT_H.DURB_ID = CROP_ACRG_RPT_DIM.CROP_ACRG_RPT_DURB_ID
                                             AND CROP_ACRG_RPT_DIM.CUR_RCD_IND = 1)
      WHERE ((DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                               'YYYY-MM-DD')
              AND DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                                                  'YYYY-MM-DD'))
             OR (DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                             'YYYY-MM-DD')
                 OR DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                               'YYYY-MM-DD')))
        AND COALESCE (DATE (CROP_ACRG_RPT_BUS_PTY_LS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND CROP_ACRG_RPT_DIM.CROP_ACRG_RPT_DURB_ID IS NOT NULL
        AND CUST_DIM.CUST_DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.CROP_ACRG_RPT_DURB_ID
          , mart.CUST_DURB_ID
  FROM CAR_DM_STG.CROP_ACRG_RPT_BUS_PTY_FACT mart
   JOIN vault
   ON vault.CROP_ACRG_RPT_DURB_ID = mart.CROP_ACRG_RPT_DURB_ID
   WHERE vault.CUST_DURB_ID = mart.CUST_DURB_ID
     AND vault.crop_acrg_rpt_srgt_id IS NOT NULL AND vault.crop_acrg_rpt_durb_id IS NOT NULL 
AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS 
NOT NULL AND vault.cust_srgt_id IS NOT NULL AND vault.cust_durb_id IS NOT NULL AND vault.prdr_invl_srgt_id IS NOT NULL AND vault.prdr_invl_durb_id IS NOT NULL
     )
INSERT INTO CAR_DM_STG.CROP_ACRG_RPT_BUS_PTY_FACT (CRE_DT, LAST_CHG_DT, DATA_STAT_CD, CROP_ACRG_RPT_SRGT_ID, CROP_ACRG_RPT_DURB_ID, PGM_YR, ADM_FSA_ST_CNTY_SRGT_ID, ADM_FSA_ST_CNTY_DURB_ID, FARM_SRGT_ID, FARM_DURB_ID, CUST_SRGT_ID, CUST_DURB_ID, PRDR_INVL_SRGT_ID, PRDR_INVL_DURB_ID, CROP_ACRG_RPT_BUS_PTY_CRE_DT, CROP_ACRG_RPT_PTY_LAST_CHG_DT, CROP_ACRG_PTY_LAST_CHG_USER_NM, SRC_DATA_STAT_CD)
SELECT  CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'A',
        vault.CROP_ACRG_RPT_SRGT_ID,
        vault.CROP_ACRG_RPT_DURB_ID,
        vault.PGM_YR,
        vault.ADM_FSA_ST_CNTY_SRGT_ID,
        vault.ADM_FSA_ST_CNTY_DURB_ID,
        vault.FARM_SRGT_ID,
        vault.FARM_DURB_ID,
        vault.CUST_SRGT_ID,
        vault.CUST_DURB_ID,
        vault.PRDR_INVL_SRGT_ID,
        vault.PRDR_INVL_DURB_ID,
        vault.CROP_ACRG_RPT_BUS_PTY_CRE_DT,
        vault.CROP_ACRG_RPT_PTY_LAST_CHG_DT,
        vault.CROP_ACRG_PTY_LAST_CHG_USER_NM,
        vault.SRC_DATA_STAT_CD
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.CROP_ACRG_RPT_DURB_ID = mart.CROP_ACRG_RPT_DURB_ID
       AND vault.CUST_DURB_ID = mart.CUST_DURB_ID )
    AND vault.crop_acrg_rpt_srgt_id IS NOT NULL AND vault.crop_acrg_rpt_durb_id IS NOT NULL 
AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS 
NOT NULL AND vault.cust_srgt_id IS NOT NULL AND vault.cust_durb_id IS NOT NULL AND vault.prdr_invl_srgt_id IS NOT NULL AND vault.prdr_invl_durb_id IS NOT NULL