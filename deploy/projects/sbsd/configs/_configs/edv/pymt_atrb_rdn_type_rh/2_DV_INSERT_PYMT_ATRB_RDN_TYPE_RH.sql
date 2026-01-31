INSERT INTO EDV.PYMT_ATRB_RDN_TYPE_RH (PYMT_ATRB_RDN_TYPE_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.PYMT_ATRB_RDN_TYPE_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (trim(PYMT_ATRB_RDN_TYPE.PYMT_ATRB_RDN_TYPE_CD),
                           '--') PYMT_ATRB_RDN_TYPE_CD,
                          PYMT_ATRB_RDN_TYPE.LOAD_DT LOAD_DT,
                          PYMT_ATRB_RDN_TYPE.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY COALESCE (trim(PYMT_ATRB_RDN_TYPE.PYMT_ATRB_RDN_TYPE_CD),
                                                                '--')
                                              ORDER BY PYMT_ATRB_RDN_TYPE.CDC_DT DESC, PYMT_ATRB_RDN_TYPE.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PYMT_ATRB_RDN_TYPE
      WHERE PYMT_ATRB_RDN_TYPE.cdc_oper_cd IN ('I',
                                               'UN') ) stg
   LEFT JOIN EDV.PYMT_ATRB_RDN_TYPE_RH dv ON (stg.PYMT_ATRB_RDN_TYPE_CD = dv.PYMT_ATRB_RDN_TYPE_CD)
   WHERE (dv.PYMT_ATRB_RDN_TYPE_CD IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )