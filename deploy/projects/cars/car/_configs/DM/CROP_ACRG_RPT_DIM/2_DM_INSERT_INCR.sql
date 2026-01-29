WITH vault AS
  (SELECT DISTINCT DM.CROP_ACRG_RPT_DURB_ID,
                   DM.DATA_EFF_STRT_DT,
                   DM.PGM_YR,
                   DM.ST_FSA_CD,
                   DM.CNTY_FSA_CD,
                   DM.FARM_NBR,
                   DM.ST_FSA_NM,
                   DM.CNTY_FSA_NM,
                   DM.CROP_ACRG_RPT_CRE_DT,
                   DM.CROP_ACRG_RPT_LAST_CHG_DT,
                   DM.CROP_ACRG_RPT_LAST_CHG_USER_NM,
                   DM.SRC_DATA_STAT_CD,
                   DM.CROP_ACRG_RPT_IACTV_DT,
                   DM.RPT_DT,
                   DM.DOC_CERT_STAT_CD,
                   DM.DOC_CERT_STAT_DESC,
                   DM.RQR_PLNT_STAT_CPLT_IND,
                   DM.RQR_PLNT_STAT_CPLT_DT,
                   DM.ACRG_RPT_CNTNT_EXST_IND,
                   DM.CPLD_TOT_ACRG,
                   DM.CPLD_TOT_CERT_ACRG,
                   DM.ACRG_RPT_LAST_MOD_DT,
                   DM.FMLD_TOT_ACRG,
                   DM.DCP_TOT_ACRG,
                   DM.LAST_OVRRD_CHG_DT,
                   DM.LAST_OVRRD_CHG_USER_NM,
                   DM.BAT_PROC_CD,
                   DM.FILE_FARM_SEQ_NBR,
                   DM.FSA_578_ORGN_ID
   FROM
     (SELECT DM_sub.CROP_ACRG_RPT_DURB_ID,
             DM_sub.DATA_EFF_STRT_DT,
             DM_sub.PGM_YR,
             DM_sub.ST_FSA_CD,
             DM_sub.CNTY_FSA_CD,
             DM_sub.FARM_NBR,
             DM_sub.ST_FSA_NM,
             DM_sub.CNTY_FSA_NM,
             DM_sub.CROP_ACRG_RPT_CRE_DT,
             DM_sub.CROP_ACRG_RPT_LAST_CHG_DT,
             DM_sub.CROP_ACRG_RPT_LAST_CHG_USER_NM,
             DM_sub.SRC_DATA_STAT_CD,
             DM_sub.CROP_ACRG_RPT_IACTV_DT,
             DM_sub.RPT_DT,
             DM_sub.DOC_CERT_STAT_CD,
             DM_sub.DOC_CERT_STAT_DESC,
             DM_sub.RQR_PLNT_STAT_CPLT_IND,
             DM_sub.RQR_PLNT_STAT_CPLT_DT,
             DM_sub.ACRG_RPT_CNTNT_EXST_IND,
             DM_sub.CPLD_TOT_ACRG,
             DM_sub.CPLD_TOT_CERT_ACRG,
             DM_sub.ACRG_RPT_LAST_MOD_DT,
             DM_sub.FMLD_TOT_ACRG,
             DM_sub.DCP_TOT_ACRG,
             DM_sub.LAST_OVRRD_CHG_DT,
             DM_sub.LAST_OVRRD_CHG_USER_NM,
             DM_sub.BAT_PROC_CD,
             DM_sub.FILE_FARM_SEQ_NBR,
             DM_sub.FSA_578_ORGN_ID,
             ROW_NUMBER () OVER (PARTITION BY DM_sub.CROP_ACRG_RPT_DURB_ID
                                 ORDER BY CROP_ACRG_RPT_HS_END_DATE DESC NULLS LAST,
                                                                         SRC_DATA_STAT_CD ASC NULLS LAST,
                                                                                              CASE
                                                                                                  WHEN SRC_DATA_STAT_CD = 'D' THEN CROP_ACRG_RPT_CRE_DT
                                                                                                  ELSE CROP_ACRG_RPT_HS_DATE
                                                                                              END DESC NULLS LAST,
                                                                                                       DOC_CERT_STAT_RS_DATE DESC NULLS LAST,
                                                                                                                                  LOC_AREA_MRT_SRC_RS_DATE DESC NULLS LAST) AS ROW_NUM_PART
      FROM
        (SELECT DISTINCT CROP_ACRG_RPT_H.DURB_ID CROP_ACRG_RPT_DURB_ID,
                         CASE
                             WHEN CROP_ACRG_RPT_HS.ACRG_RPT_CNTNT_EXST_IND = 'Y' THEN 1
                             ELSE 0
                         END ACRG_RPT_CNTNT_EXST_IND,
                         CROP_ACRG_RPT_HS.ACRG_RPT_LAST_MOD_DT ACRG_RPT_LAST_MOD_DT,
                         CROP_ACRG_RPT_HS.BAT_PROC_CD BAT_PROC_CD,
                         COALESCE (CROP_ACRG_RPT_H.CNTY_FSA_CD,
                                   '-4') CNTY_FSA_CD,
                                  edv.LOC_AREA_MRT_SRC_RS.LOC_AREA_NM CNTY_FSA_NM,
                                  CROP_ACRG_RPT_HS.SRC_CRE_DT CROP_ACRG_RPT_CRE_DT,
                                  CROP_ACRG_RPT_HS.DATA_IACTV_DT CROP_ACRG_RPT_IACTV_DT,
                                  CROP_ACRG_RPT_HS.SRC_LAST_CHG_DT CROP_ACRG_RPT_LAST_CHG_DT,
                                  CROP_ACRG_RPT_HS.SRC_LAST_CHG_USER_NM CROP_ACRG_RPT_LAST_CHG_USER_NM,
                                  CROP_ACRG_RPT_HS.CPLD_TOT_ACRG CPLD_TOT_ACRG,
                                  CROP_ACRG_RPT_HS.CPLD_TOT_CERT_ACRG CPLD_TOT_CERT_ACRG,
                                  CROP_ACRG_RPT_HS.DCP_TOT_ACRG DCP_TOT_ACRG,
                                  CROP_ACRG_RPT_HS.DOC_CERT_STAT_CD DOC_CERT_STAT_CD,
                                  DOC_CERT_STAT_RS.DOC_CERT_STAT_DESC DOC_CERT_STAT_DESC,
                                  COALESCE (CROP_ACRG_RPT_H.FARM_NBR,
                                            '-4') FARM_NBR,
                                           CROP_ACRG_RPT_HS.FMLD_TOT_ACRG FMLD_TOT_ACRG,
                                           CROP_ACRG_RPT_HS.FILE_FARM_SEQ_NBR FILE_FARM_SEQ_NBR,
                                           CROP_ACRG_RPT_HS.FSA_578_ORGN_ID FSA_578_ORGN_ID,
                                           CROP_ACRG_RPT_HS.LAST_OVRRD_CHG_DT LAST_OVRRD_CHG_DT,
                                           CROP_ACRG_RPT_HS.LAST_OVRRD_CHG_USER_NM LAST_OVRRD_CHG_USER_NM,
                                           COALESCE (CROP_ACRG_RPT_H.PGM_YR,
                                                     0) PGM_YR,
                                                    CROP_ACRG_RPT_HS.RPT_DT RPT_DT,
                                                    CROP_ACRG_RPT_HS.RQR_PLNT_STAT_CPLT_DT RQR_PLNT_STAT_CPLT_DT,
                                                    CASE
                                                        WHEN CROP_ACRG_RPT_HS.RQR_PLNT_STAT_CPLT_IND = 'Y' THEN 1
                                                        ELSE 0
                                                    END RQR_PLNT_STAT_CPLT_IND,
                                                    CROP_ACRG_RPT_HS.DATA_STAT_CD SRC_DATA_STAT_CD,
                                                    COALESCE (CROP_ACRG_RPT_H.ST_FSA_CD,
                                                              '-4') ST_FSA_CD,
                                                             edv.LOC_AREA_MRT_SRC_RS.CTRY_DIV_NM ST_FSA_NM,
                                                             LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT LOC_AREA_MRT_SRC_RS_DATE,
                                                             DOC_CERT_STAT_RS.DATA_EFF_STRT_DT DOC_CERT_STAT_RS_DATE,
                                                             CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT CROP_ACRG_RPT_HS_DATE,
                                                             CROP_ACRG_RPT_HS.DATA_EFF_END_DT CROP_ACRG_RPT_HS_END_DATE,
                                                             GREATEST (COALESCE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (DOC_CERT_STAT_RS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD'))) DATA_EFF_STRT_DT,
                                                             ROW_NUMBER () OVER (PARTITION BY CROP_ACRG_RPT_H.DURB_ID
                                                                                 ORDER BY CROP_ACRG_RPT_HS.DATA_EFF_END_DT DESC NULLS LAST,
                                                                                                                                CROP_ACRG_RPT_HS.DATA_STAT_CD ASC NULLS LAST,
                                                                                                                                                                  CASE
                                                                                                                                                                      WHEN CROP_ACRG_RPT_HS.DATA_STAT_CD = 'D' THEN CROP_ACRG_RPT_HS.SRC_CRE_DT
                                                                                                                                                                      ELSE CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT
                                                                                                                                                                  END DESC NULLS LAST,
                                                                                                                                                                           DOC_CERT_STAT_RS.DATA_EFF_STRT_DT DESC NULLS LAST,
                                                                                                                                                                                                                  LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT DESC NULLS LAST) AS RNUM
         FROM edv.CROP_ACRG_RPT_HS
         LEFT JOIN edv.CROP_ACRG_RPT_H ON (COALESCE (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_H_ID,
                                                     '1cc552b48373871758d99e3ecfe05b70') = CROP_ACRG_RPT_H.CROP_ACRG_RPT_H_ID)
         LEFT JOIN edv.LOC_AREA_MRT_SRC_RS ON (CROP_ACRG_RPT_H.ST_FSA_CD = LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD
                                               AND CROP_ACRG_RPT_H.CNTY_FSA_CD = LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD
                                               AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
         LEFT JOIN edv.DOC_CERT_STAT_RS ON (CROP_ACRG_RPT_HS.DOC_CERT_STAT_CD = DOC_CERT_STAT_RS.DOC_CERT_STAT_CD)
         WHERE ((DATE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                          'YYYY-MM-DD')
                 AND DATE (CROP_ACRG_RPT_HS.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                                             'YYYY-MM-DD'))
                OR (DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                                'YYYY-MM-DD')
                    OR DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                                  'YYYY-MM-DD'))
                OR (DATE (DOC_CERT_STAT_RS.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                             'YYYY-MM-DD')
                    OR DATE (DOC_CERT_STAT_RS.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                                               'YYYY-MM-DD')))
           AND COALESCE (DATE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND COALESCE (DATE (DOC_CERT_STAT_RS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND CROP_ACRG_RPT_H.DURB_ID IS NOT NULL ) DM_sub
      WHERE DM_sub.RNUM = 1 ) DM
   WHERE DM.ROW_NUM_PART = 1 ),
     remainder AS
  (SELECT mart.CROP_ACRG_RPT_DURB_ID
  FROM CAR_DM_STG.CROP_ACRG_RPT_DIM mart
  JOIN vault
   ON vault.CROP_ACRG_RPT_DURB_ID = mart.CROP_ACRG_RPT_DURB_ID
    WHERE mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     )
INSERT INTO CAR_DM_STG.CROP_ACRG_RPT_DIM (CROP_ACRG_RPT_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, PGM_YR, ST_FSA_CD, CNTY_FSA_CD, FARM_NBR, ST_FSA_NM, CNTY_FSA_NM, CROP_ACRG_RPT_CRE_DT, CROP_ACRG_RPT_LAST_CHG_DT, CROP_ACRG_RPT_LAST_CHG_USER_NM, SRC_DATA_STAT_CD, CROP_ACRG_RPT_IACTV_DT, RPT_DT, DOC_CERT_STAT_CD, DOC_CERT_STAT_DESC, RQR_PLNT_STAT_CPLT_IND, RQR_PLNT_STAT_CPLT_DT, ACRG_RPT_CNTNT_EXST_IND, CPLD_TOT_ACRG, CPLD_TOT_CERT_ACRG, ACRG_RPT_LAST_MOD_DT, FMLD_TOT_ACRG, DCP_TOT_ACRG, LAST_OVRRD_CHG_DT, LAST_OVRRD_CHG_USER_NM, BAT_PROC_CD, FILE_FARM_SEQ_NBR, FSA_578_ORGN_ID)
SELECT vault.CROP_ACRG_RPT_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.PGM_YR,
                                     vault.ST_FSA_CD,
                                     vault.CNTY_FSA_CD,
                                     vault.FARM_NBR,
                                     vault.ST_FSA_NM,
                                     vault.CNTY_FSA_NM,
                                     vault.CROP_ACRG_RPT_CRE_DT,
                                     vault.CROP_ACRG_RPT_LAST_CHG_DT,
                                     vault.CROP_ACRG_RPT_LAST_CHG_USER_NM,
                                     vault.SRC_DATA_STAT_CD,
                                     vault.CROP_ACRG_RPT_IACTV_DT,
                                     vault.RPT_DT,
                                     vault.DOC_CERT_STAT_CD,
                                     vault.DOC_CERT_STAT_DESC,
                                     vault.RQR_PLNT_STAT_CPLT_IND,
                                     vault.RQR_PLNT_STAT_CPLT_DT,
                                     vault.ACRG_RPT_CNTNT_EXST_IND,
                                     vault.CPLD_TOT_ACRG,
                                     vault.CPLD_TOT_CERT_ACRG,
                                     vault.ACRG_RPT_LAST_MOD_DT,
                                     vault.FMLD_TOT_ACRG,
                                     vault.DCP_TOT_ACRG,
                                     vault.LAST_OVRRD_CHG_DT,
                                     vault.LAST_OVRRD_CHG_USER_NM,
                                     vault.BAT_PROC_CD,
                                     vault.FILE_FARM_SEQ_NBR,
                                     vault.FSA_578_ORGN_ID
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.CROP_ACRG_RPT_DURB_ID = mart.CROP_ACRG_RPT_DURB_ID )