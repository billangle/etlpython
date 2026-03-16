SELECT GVT_LVL_DESC,
GVT_LVL_ID,
WEB_PAGE_URL,
AUTH_ROLE_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT FROM (
SELECT * FROM (
SELECT DISTINCT
government_level.GVT_LVL_DESC GVT_LVL_DESC,
government_level.GOVT_LVL_ID GVT_LVL_ID,
government_level.WEB_PAGE_URL WEB_PAGE_URL,
government_level.AUTH_ROLE_NM AUTH_ROLE_NM,
government_level.DATA_STAT_CD DATA_STAT_CD,
government_level.CRE_DT CRE_DT,
government_level.LAST_CHG_DT LAST_CHG_DT,
government_level.LAST_CHG_USER_NM LAST_CHG_USER_NM,
OP AS CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".government_level government_level
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and OP <> 'D'
UNION
SELECT DISTINCT
government_level.GVT_LVL_DESC GVT_LVL_DESC,
government_level.GOVT_LVL_ID GVT_LVL_ID,
government_level.WEB_PAGE_URL WEB_PAGE_URL,
government_level.AUTH_ROLE_NM AUTH_ROLE_NM,
government_level.DATA_STAT_CD DATA_STAT_CD,
government_level.CRE_DT CRE_DT,
government_level.LAST_CHG_DT LAST_CHG_DT,
government_level.LAST_CHG_USER_NM LAST_CHG_USER_NM,
'D' OP,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".government_level government_level
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and OP = 'D'
))