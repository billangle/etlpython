WITH vault AS
  (SELECT DISTINCT dm.CNCUR_PLNT_DURB_ID,
                   dm.DATA_EFF_STRT_DT,
                   dm.CNCUR_PLNT_CD,
                   dm.CNCUR_PLNT_DESC
   FROM
     (SELECT CNCUR_PLNT_RH.DURB_ID CNCUR_PLNT_DURB_ID,
             COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.CNCUR_PLNT_CD,
                                '-') CNCUR_PLNT_CD,
                               dv_dr.CNCUR_PLNT_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , CNCUR_PLNT_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.CNCUR_PLNT_RS dv_dr
      LEFT JOIN edv.CNCUR_PLNT_RH ON (COALESCE (dv_dr.CNCUR_PLNT_CD,
                                                '-') = CNCUR_PLNT_RH.CNCUR_PLNT_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND CNCUR_PLNT_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.CNCUR_PLNT_DURB_ID
  FROM CAR_DM_STG.CNCUR_PLNT_DIM mart
   JOIN vault
   ON vault.CNCUR_PLNT_DURB_ID = mart.CNCUR_PLNT_DURB_ID
     WHERE mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.CNCUR_PLNT_CD,
                    'X') <> COALESCE (mart.CNCUR_PLNT_CD,
                                      'X')
          OR COALESCE (vault.CNCUR_PLNT_DESC,
                       'X') <> COALESCE (mart.CNCUR_PLNT_DESC,
                                         'X')) )
INSERT INTO CAR_DM_STG.CNCUR_PLNT_DIM (CNCUR_PLNT_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, CNCUR_PLNT_CD, CNCUR_PLNT_DESC)
SELECT vault.CNCUR_PLNT_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.CNCUR_PLNT_CD,
                                     vault.CNCUR_PLNT_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.CNCUR_PLNT_DURB_ID = mart.CNCUR_PLNT_DURB_ID )