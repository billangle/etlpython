WITH vault AS
  (SELECT DISTINCT dm.PLNT_PRD_DURB_ID,
                   dm.PLNT_PRD_CD,
                   dm.DATA_EFF_STRT_DT,
                   dm.PLNT_PRD_DESC
   FROM
     (SELECT PLNT_PRD_RH.DURB_ID AS PLNT_PRD_DURB_ID,
             COALESCE (dv_dr.PLNT_PRD_CD,
                       '-4') AS PLNT_PRD_CD,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.PLNT_PRD_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , PLNT_PRD_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.PLNT_PRD_RS dv_dr
      LEFT JOIN edv.PLNT_PRD_RH ON (COALESCE (dv_dr.PLNT_PRD_CD,
                                              '--') = PLNT_PRD_RH.PLNT_PRD_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND PLNT_PRD_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.PLNT_PRD_DURB_ID
  FROM CAR_DM_STG.PLNT_PRD_DIM mart
  JOIN vault
  ON vault.PLNT_PRD_DURB_ID = mart.PLNT_PRD_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.PLNT_PRD_DESC,
                    'X') <> COALESCE (mart.PLNT_PRD_DESC,
                                      'X')))
INSERT INTO CAR_DM_STG.PLNT_PRD_DIM (PLNT_PRD_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, PLNT_PRD_CD, PLNT_PRD_DESC)
SELECT vault.PLNT_PRD_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.PLNT_PRD_CD,
                                     vault.PLNT_PRD_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.PLNT_PRD_DURB_ID = mart.PLNT_PRD_DURB_ID )