WITH vault AS
  (SELECT DISTINCT dm.CROP_RPT_UNIT_CD,
                   dm.DATA_EFF_STRT_DT,
                   dm.CROP_RPT_UNIT_DESC,
                   dm.DURB_ID
   FROM
     (SELECT COALESCE (dv_dr.CROP_RPT_UNIT_CD,
                       '-') CROP_RPT_UNIT_CD,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.CROP_RPT_UNIT_DESC,
                               CROP_RPT_UNIT_RH.DURB_ID DURB_ID,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , CROP_RPT_UNIT_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.CROP_RPT_UNIT_RS dv_dr
      LEFT JOIN edv.CROP_RPT_UNIT_RH ON (COALESCE (dv_dr.CROP_RPT_UNIT_CD,
                                                   '-') = CROP_RPT_UNIT_RH.CROP_RPT_UNIT_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND CROP_RPT_UNIT_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.CROP_RPT_UNIT_DURB_ID 
  FROM CAR_DM_STG.CROP_RPT_UNIT_DIM mart
  JOIN vault
  ON vault.DURB_ID = mart.CROP_RPT_UNIT_DURB_ID
     WHERE mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.CROP_RPT_UNIT_CD,
                    'X') <> COALESCE (mart.CROP_RPT_UNIT_CD,
                                      'X')
          OR COALESCE (vault.CROP_RPT_UNIT_DESC,
                       'X') <> COALESCE (mart.CROP_RPT_UNIT_DESC,
                                         'X')) )
INSERT INTO CAR_DM_STG.CROP_RPT_UNIT_DIM (CROP_RPT_UNIT_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, CROP_RPT_UNIT_CD, CROP_RPT_UNIT_DESC)
SELECT vault.DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'),
       CURRENT_TIMESTAMP,
       vault.CROP_RPT_UNIT_CD,
       vault.CROP_RPT_UNIT_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.DURB_ID = mart.CROP_RPT_UNIT_DURB_ID )