INSERT INTO EDV.PYMT_PGM_RH (PYMT_PGM_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.PYMT_PGM_NM,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (PYMT_PGM.PGM_NM,
                           '[NULL IN SOURCE]') PYMT_PGM_NM,
                          PYMT_PGM.LOAD_DT LOAD_DT,
                          PYMT_PGM.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY PYMT_PGM.PGM_NM
                                              ORDER BY PYMT_PGM.CDC_DT DESC, PYMT_PGM.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PYMT_PGM) stg
   LEFT JOIN EDV.PYMT_PGM_RH dv ON (stg.PYMT_PGM_NM = dv.PYMT_PGM_NM)
   WHERE (dv.PYMT_PGM_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )