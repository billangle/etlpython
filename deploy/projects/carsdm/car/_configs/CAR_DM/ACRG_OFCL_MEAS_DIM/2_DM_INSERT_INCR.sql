WITH vault AS
  (SELECT DISTINCT dm.ACRG_OFCL_MEAS_DURB_ID,
                   dm.ACRG_OFCL_MEAS_CD,
                   dm.DATA_EFF_STRT_DT,
                   dm.ACRG_OFCL_MEAS_DESC
   FROM
     (SELECT ACRG_OFCL_MEAS_RH.DURB_ID AS ACRG_OFCL_MEAS_DURB_ID,
             COALESCE (dv_dr.ACRG_OFCL_MEAS_CD,
                       '-') AS ACRG_OFCL_MEAS_CD,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.ACRG_OFCL_MEAS_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , ACRG_OFCL_MEAS_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.ACRG_OFCL_MEAS_RS dv_dr
      LEFT JOIN edv.ACRG_OFCL_MEAS_RH ON (COALESCE (dv_dr.ACRG_OFCL_MEAS_CD,
                                                    '-') = ACRG_OFCL_MEAS_RH.ACRG_OFCL_MEAS_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND ACRG_OFCL_MEAS_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.ACRG_OFCL_MEAS_DURB_ID
   FROM CAR_DM_STG.ACRG_OFCL_MEAS_DIM mart
   JOIN vault
   ON vault.ACRG_OFCL_MEAS_DURB_ID = mart.ACRG_OFCL_MEAS_DURB_ID
   WHERE mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.ACRG_OFCL_MEAS_DESC,
                    'X') <> COALESCE (mart.ACRG_OFCL_MEAS_DESC,
                                      'X')))
INSERT INTO CAR_DM_STG.ACRG_OFCL_MEAS_DIM (ACRG_OFCL_MEAS_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, ACRG_OFCL_MEAS_CD, ACRG_OFCL_MEAS_DESC)
SELECT vault.ACRG_OFCL_MEAS_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.ACRG_OFCL_MEAS_CD,
                                     vault.ACRG_OFCL_MEAS_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.ACRG_OFCL_MEAS_DURB_ID = mart.ACRG_OFCL_MEAS_DURB_ID )