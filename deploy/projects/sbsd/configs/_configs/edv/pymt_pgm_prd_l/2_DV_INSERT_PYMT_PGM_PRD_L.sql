INSERT INTO EDV.PYMT_PGM_PRD_L (PYMT_PGM_PRD_L_ID, PYMT_PGM_NM, SBSD_PRD_STRT_YR, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.PYMT_PGM_NM || stg.SBSD_PRD_STRT_YR) PYMT_PGM_PRD_L_ID,
          stg.PYMT_PGM_NM,
          stg.SBSD_PRD_STRT_YR,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (PYMT_PGM.PGM_NM,
                           '[NULL IN SOURCE]') PYMT_PGM_NM,
                             PYMT_PGM_PRD.SBSD_PRD_STRT_YR  SBSD_PRD_STRT_YR,
                              PYMT_PGM_PRD.LOAD_DT LOAD_DT,
                              PYMT_PGM_PRD.DATA_SRC_NM DATA_SRC_NM,
                              ROW_NUMBER () OVER (PARTITION BY COALESCE (PYMT_PGM.PGM_NM,
                                                                    '[NULL IN SOURCE]') , PYMT_PGM_PRD.SBSD_PRD_STRT_YR
                                                  ORDER BY PYMT_PGM_PRD.CDC_DT DESC, PYMT_PGM_PRD.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PYMT_PGM_PRD
      JOIN EDV.V_PYMT_PGM PYMT_PGM ON (PYMT_PGM_PRD.PYMT_PGM_ID = PYMT_PGM.PYMT_PGM_ID)) stg
   LEFT JOIN EDV.PYMT_PGM_PRD_L dv ON (stg.PYMT_PGM_NM = dv.PYMT_PGM_NM
                                       AND stg.SBSD_PRD_STRT_YR = dv.SBSD_PRD_STRT_YR)
   WHERE (dv.PYMT_PGM_NM IS NULL
          OR dv.SBSD_PRD_STRT_YR IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )