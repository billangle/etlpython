SELECT DISTINCT
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative.afl_init_id AFL_INIT_ID,
affiliated_initiative.data_stat_cd DATA_STAT_CD,
affiliated_initiative.cre_dt CRE_DT,
affiliated_initiative.last_chg_dt LAST_CHG_DT,
affiliated_initiative.cre_user_nm CRE_USER_NM,
affiliated_initiative.last_chg_user_nm LAST_CHG_USER_NM,
--OP AS CDC_OPER_CD,
1 AS row_num_part
FROM  affiliated_initiative
--WHERE OP <> 'D'
UNION
SELECT DISTINCT
affiliated_initiative.afl_init_nm AFL_INIT_NM,
affiliated_initiative.afl_init_id AFL_INIT_ID,
affiliated_initiative.data_stat_cd DATA_STAT_CD,
affiliated_initiative.cre_dt CRE_DT,
affiliated_initiative.last_chg_dt LAST_CHG_DT,
affiliated_initiative.cre_user_nm CRE_USER_NM,
affiliated_initiative.last_chg_user_nm LAST_CHG_USER_NM,
--'D' CDC_OPER_CD,
1 AS row_num_part
FROM affiliated_initiative
--WHERE OP = 'D'