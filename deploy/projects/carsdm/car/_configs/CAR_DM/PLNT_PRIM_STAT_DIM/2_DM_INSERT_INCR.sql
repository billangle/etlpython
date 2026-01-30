WITH vault AS
  (SELECT DISTINCT dm.PLNT_PRIM_STAT_DURB_ID,
                   dm.PLNT_PRIM_STAT_CD,
                   dm.PGM_YR,
                   dm.DATA_EFF_STRT_DT,
                   dm.PLNT_PRIM_STAT_NM
   FROM
     (SELECT PLNT_PRIM_STAT_RH.DURB_ID PLNT_PRIM_STAT_DURB_ID,
             COALESCE (dv_dr .PLNT_PRIM_STAT_CD,
                       '-') PLNT_PRIM_STAT_CD,
                      dv_dr .PGM_YR PGM_YR,
                      COALESCE (dv_dr .DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr .PLNT_PRIM_STAT_NM PLNT_PRIM_STAT_NM,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , PLNT_PRIM_STAT_RH.DURB_ID,
                                                                                                         dv_dr.PGM_YR
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.PLNT_PRIM_STAT_RS dv_dr
      LEFT JOIN edv.PLNT_PRIM_STAT_RH ON COALESCE (dv_dr.PLNT_PRIM_STAT_CD,
                                                   '-') = PLNT_PRIM_STAT_RH.PLNT_PRIM_STAT_CD
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND PLNT_PRIM_STAT_RH.DURB_ID IS NOT NULL
        AND dv_dr.PGM_YR IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.PLNT_PRIM_STAT_DURB_ID
    , mart.PGM_YR
  FROM CAR_DM_STG.PLNT_PRIM_STAT_DIM mart
  JOIN vault
  ON vault.PLNT_PRIM_STAT_DURB_ID = mart.PLNT_PRIM_STAT_DURB_ID
     AND vault.PGM_YR = mart.PGM_YR
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.PLNT_PRIM_STAT_NM,
                    'X') <> COALESCE (mart.PLNT_PRIM_STAT_NM,
                                      'X')))
INSERT INTO CAR_DM_STG.PLNT_PRIM_STAT_DIM (PLNT_PRIM_STAT_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, PLNT_PRIM_STAT_CD, PGM_YR, PLNT_PRIM_STAT_NM)
SELECT vault.PLNT_PRIM_STAT_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.PLNT_PRIM_STAT_CD,
                                     vault.PGM_YR,
                                     vault.PLNT_PRIM_STAT_NM
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.PLNT_PRIM_STAT_DURB_ID = mart.PLNT_PRIM_STAT_DURB_ID
       AND vault.PGM_YR = mart.PGM_YR )