WITH vault AS
  (SELECT DISTINCT dm.PLNT_SCND_STAT_CD,
                   dm.PGM_YR,
                   dm.PLNT_SCND_STAT_NM,
                   dm.PLNT_SCND_STAT_DURB_ID,
                   dm.DATA_EFF_STRT_DT
   FROM
     (SELECT COALESCE (dv_dr.PLNT_SCND_STAT_CD,
                       '-') AS PLNT_SCND_STAT_CD,
                      dv_dr.PGM_YR,
                      dv_dr.PLNT_SCND_STAT_NM,
                      PLNT_SCND_STAT_RH.DURB_ID AS PLNT_SCND_STAT_DURB_ID,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , PLNT_SCND_STAT_RH.DURB_ID,
                                                                                                         dv_dr.PGM_YR
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.PLNT_SCND_STAT_RS dv_dr
      JOIN edv.PLNT_SCND_STAT_RH ON (COALESCE (dv_dr.PLNT_SCND_STAT_CD,
                                               '-') =PLNT_SCND_STAT_RH.PLNT_SCND_STAT_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND PLNT_SCND_STAT_RH.DURB_ID IS NOT NULL
        AND dv_dr.PGM_YR IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.PLNT_SCND_STAT_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.PLNT_SCND_STAT_DURB_ID = mart.PLNT_SCND_STAT_DURB_ID
     AND vault.PGM_YR = mart.PGM_YR
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.PLNT_SCND_STAT_NM,
                    'X') <> COALESCE (mart.PLNT_SCND_STAT_NM,
                                      'X'))