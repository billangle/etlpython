WITH vault AS
  (SELECT DISTINCT dm.CROP_PRAC_DURB_ID,
                   dm.DATA_EFF_STRT_DT,
                   dm.CROP_PRAC_CD,
                   dm.CROP_PRAC_DESC
   FROM
     (SELECT CROP_PRAC_RH.DURB_ID CROP_PRAC_DURB_ID,
             COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.CROP_PRAC_CD,
                                '-4') CROP_PRAC_CD,
                               dv_dr.CROP_PRAC_DESC CROP_PRAC_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , CROP_PRAC_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.CROP_PRAC_RS dv_dr
      LEFT JOIN edv.CROP_PRAC_RH ON (COALESCE (dv_dr.CROP_PRAC_CD,
                                               '-1') = COALESCE (CROP_PRAC_RH.CROP_PRAC_CD,
                                                                 '-1'))
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND CROP_PRAC_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.CROP_PRAC_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.CROP_PRAC_DURB_ID = mart.CROP_PRAC_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.CROP_PRAC_CD,
                    '-4') <> COALESCE (mart.CROP_PRAC_CD,
                                      '-4')
          OR COALESCE (vault.CROP_PRAC_DESC,
                       'X') <> COALESCE (mart.CROP_PRAC_DESC,
                                         'X'))