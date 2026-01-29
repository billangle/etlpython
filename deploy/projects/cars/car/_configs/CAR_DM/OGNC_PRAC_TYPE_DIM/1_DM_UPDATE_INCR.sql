WITH vault AS
  (SELECT DISTINCT dm.OGNC_PRAC_TYPE_CD,
                   dm.DATA_EFF_STRT_DT,
                   dm.OGNC_PRAC_TYPE_DESC,
                   dm.DURB_ID
   FROM
     (SELECT COALESCE (dv_dr.OGNC_PRAC_TYPE_CD,
                       '-4') OGNC_PRAC_TYPE_CD,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.OGNC_PRAC_TYPE_DESC,
                               OGNC_PRAC_TYPE_RH.DURB_ID,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , OGNC_PRAC_TYPE_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.OGNC_PRAC_TYPE_RS dv_dr
      LEFT JOIN edv.OGNC_PRAC_TYPE_RH ON (COALESCE (dv_dr.OGNC_PRAC_TYPE_CD,
                                                    '--') = OGNC_PRAC_TYPE_RH.OGNC_PRAC_TYPE_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND OGNC_PRAC_TYPE_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.OGNC_PRAC_TYPE_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.DURB_ID = mart.OGNC_PRAC_TYPE_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.OGNC_PRAC_TYPE_CD,
                    'X') <> COALESCE (mart.OGNC_PRAC_TYPE_CD,
                                      'X')
          OR COALESCE (vault.OGNC_PRAC_TYPE_DESC,
                       'X') <> COALESCE (mart.OGNC_PRAC_TYPE_DESC,
                                         'X'))