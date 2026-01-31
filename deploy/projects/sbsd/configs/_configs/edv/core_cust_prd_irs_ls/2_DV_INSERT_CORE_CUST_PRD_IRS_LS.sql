INSERT INTO EDV.CORE_CUST_PRD_IRS_LS (CORE_CUST_PRD_IRS_L_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, SRC_DATA_RCV_DT, AGI_CNST_FORM_IRS_IPUT_DT, BAT_PROC_STAT_EFF_DT, BAT_PROC_STAT_CD, CUST_DATA_IRS_VLD_CD, IRS_900K_AGI_ELG_DTER_CD, IRS_AGI_ELG_ERR_TYPE_NBR, IRS_AGI_ELG_ERR_CAT_CD, IRS_AGI_ELG_ERR_TYPE_ID, CORE_CUST_ID, PGM_YR_IRS_BUS_PTY_2014_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (COALESCE (PGM_YR_IRS_BUS_PTY_2014.CORE_CUST_ID::varchar, '-1') ||PGM_YR_IRS_BUS_PTY_2014.PGM_YR) AS CORE_CUST_PRD_IRS_L_ID,
                      PGM_YR_IRS_BUS_PTY_2014.LAST_CHG_DT AS LOAD_DT,
                      PGM_YR_IRS_BUS_PTY_2014.CDC_DT,
                      PGM_YR_IRS_BUS_PTY_2014.DATA_SRC_NM,
                      PGM_YR_IRS_BUS_PTY_2014.DATA_STAT_CD,
                      PGM_YR_IRS_BUS_PTY_2014.CRE_DT,
                      PGM_YR_IRS_BUS_PTY_2014.CRE_USER_NM,
                      PGM_YR_IRS_BUS_PTY_2014.LAST_CHG_DT,
                      PGM_YR_IRS_BUS_PTY_2014.LAST_CHG_USER_NM,
                      PGM_YR_IRS_BUS_PTY_2014.DATA_RCV_DT,
                      PGM_YR_IRS_BUS_PTY_2014.AGI_CNST_FORM_IRS_IPUT_DT,
                      PGM_YR_IRS_BUS_PTY_2014.BAT_PROC_STAT_EFF_DT,
                      PGM_YR_IRS_BUS_PTY_2014.BAT_PROC_STAT_CD,
                      PGM_YR_IRS_BUS_PTY_2014.CUST_DATA_IRS_VLD_CD,
                      PGM_YR_IRS_BUS_PTY_2014.IRS_900K_AGI_ELG_DTER_CD,
                      IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_NBR,
                      IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_CAT_CD,
                      PGM_YR_IRS_BUS_PTY_2014.IRS_AGI_ELG_ERR_TYPE_ID,
                      PGM_YR_IRS_BUS_PTY_2014.CORE_CUST_ID,
                      PGM_YR_IRS_BUS_PTY_2014.PGM_YR_IRS_BUS_PTY_2014_ID,
                      to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                      to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.PGM_YR_IRS_BUS_PTY_2014
      LEFT OUTER JOIN EDV.V_IRS_AGI_ELG_ERR_TYPE IRS_AGI_ELG_ERR_TYPE ON (PGM_YR_IRS_BUS_PTY_2014.IRS_AGI_ELG_ERR_TYPE_ID=IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_ID)
      WHERE PGM_YR_IRS_BUS_PTY_2014.CDC_OPER_CD<>'D'
        AND DATE_TRUNC('day',PGM_YR_IRS_BUS_PTY_2014.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND PGM_YR_IRS_BUS_PTY_2014.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.PGM_YR_IRS_BUS_PTY_2014)
      ORDER BY PGM_YR_IRS_BUS_PTY_2014.CDC_DT) stg
   LEFT JOIN EDV.CORE_CUST_PRD_IRS_LS dv ON (dv.CORE_CUST_PRD_IRS_L_ID= stg.CORE_CUST_PRD_IRS_L_ID
                                             AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                             AND coalesce(stg.AGI_CNST_FORM_IRS_IPUT_DT, current_date) = coalesce(dv.AGI_CNST_FORM_IRS_IPUT_DT, current_date)
                                             AND coalesce(stg.BAT_PROC_STAT_EFF_DT, current_date) = coalesce(dv.BAT_PROC_STAT_EFF_DT, current_date)
                                             AND coalesce(stg.BAT_PROC_STAT_CD, 'X') = coalesce(dv.BAT_PROC_STAT_CD, 'X')
                                             AND coalesce(stg.CUST_DATA_IRS_VLD_CD, 'X') = coalesce(dv.CUST_DATA_IRS_VLD_CD, 'X')
                                             AND coalesce(stg.IRS_900K_AGI_ELG_DTER_CD, 'X') = coalesce(dv.IRS_900K_AGI_ELG_DTER_CD, 'X')
                                             AND coalesce(stg.IRS_AGI_ELG_ERR_TYPE_NBR, 'X') = coalesce(dv.IRS_AGI_ELG_ERR_TYPE_NBR, 'X')
                                             AND coalesce(stg.IRS_AGI_ELG_ERR_CAT_CD, 'X') = coalesce(dv.IRS_AGI_ELG_ERR_CAT_CD, 'X')
                                             AND coalesce(stg.IRS_AGI_ELG_ERR_TYPE_ID, 0) = coalesce(dv.IRS_AGI_ELG_ERR_TYPE_ID, 0)
                                             AND coalesce(stg.CORE_CUST_ID, 0) = coalesce(dv.CORE_CUST_ID, 0)
                                             AND coalesce(stg.PGM_YR_IRS_BUS_PTY_2014_ID, 0) = coalesce(dv.PGM_YR_IRS_BUS_PTY_2014_ID, 0)
                                             AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.CORE_CUST_PRD_IRS_L_ID IS NULL )