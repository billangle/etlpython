SELECT loss_unit_id LOSS_UNIT_ID,
loss_unit_nm LOSS_UNIT_NM,
cre_dt CRE_DT,
last_chg_dt LAST_CHG_DT,
last_chg_user_nm LAST_CHG_USER_NM,
data_stat_cd DATA_STAT_CD,
 OP As CDC_OPER_CD ,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV-CS'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT
FROM "fsa-{env}-cnsv-cdc".loss_unit
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'