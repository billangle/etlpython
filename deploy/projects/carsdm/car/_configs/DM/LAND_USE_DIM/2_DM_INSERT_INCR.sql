WITH vault AS
  (SELECT DISTINCT dm.LAND_USE_DURB_ID,
                   dm.LAND_USE_CD,
                   dm.PGM_YR,
                   dm.DATA_EFF_STRT_DT,
                   dm.LAND_USE_DESC
   FROM
     (SELECT LAND_USE_RH.DURB_ID AS LAND_USE_DURB_ID,
             COALESCE (dv_dr.LAND_USE_CD,
                       '-') AS LAND_USE_CD,
                      dv_dr.PGM_YR,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.LAND_USE_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , LAND_USE_RH.DURB_ID,
                                                                                                         dv_dr.PGM_YR
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.LAND_USE_RS dv_dr
      JOIN edv.LAND_USE_RH ON (COALESCE (dv_dr.LAND_USE_CD,
                                         '-') =LAND_USE_RH.LAND_USE_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND LAND_USE_RH.DURB_ID IS NOT NULL
        AND dv_dr.PGM_YR IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.LAND_USE_DURB_ID
    , mart.PGM_YR
  FROM CAR_DM_STG.LAND_USE_DIM mart
  JOIN vault
  ON vault.LAND_USE_DURB_ID = mart.LAND_USE_DURB_ID
  WHERE vault.PGM_YR = mart.PGM_YR
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.LAND_USE_DESC,
                    'X') <> COALESCE (mart.LAND_USE_DESC,
                                      'X')))
INSERT INTO CAR_DM_STG.LAND_USE_DIM (LAND_USE_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, LAND_USE_CD, PGM_YR, LAND_USE_DESC)
SELECT vault.LAND_USE_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.LAND_USE_CD,
                                     vault.PGM_YR,
                                     vault.LAND_USE_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.LAND_USE_DURB_ID = mart.LAND_USE_DURB_ID
       AND vault.PGM_YR = mart.PGM_YR )