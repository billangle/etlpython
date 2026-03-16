-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_RINST_EVNT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_RINST_EVNT/incremental/CCMS_CTR_RINST_EVNT.sql
-- =============================================================================

SELECT * FROM
(
SELECT DISTINCT ADM_ST_FSA_CD,
ADM_CNTY_FSA_CD,
SGNP_TYPE_NM,
SGNP_SUB_CAT_NM,
SGNP_NBR,
SGNP_STYPE_AGR_NM,
CTR_NBR,
CTR_SFX_NBR,
CTR_ACTV_RSN_TYPE_DESC,
CTR_RINST_EVNT_ID,
CTR_DET_ID,
CTR_ACTV_RSN_TYPE_CD,
CTR_ACTV_TYPE_CD,
DAFP_APRV_DT,
CTR_TERM_DT,
CTR_RINST_RSN_TXT,
TERM_OT_ACTV_RSN_DESC,
DATA_STAT_CD,
LAST_CHG_DT,
CRE_DT,
LAST_CHG_USER_NM,
CRE_USER_NM,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
CTR_RINST_EVNT_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
contract_reinstated_event.op,
1 AS TBL_PRIORITY
From "fsa-{env}-ccms-cdc".contract_reinstated_event
Left Join "fsa-{env}-ccms-cdc"CONTRACT_ACTIVITY_REASON_TYPE 
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left join "fsa-{env}-ccms-cdc"CONTRACT_ACTIVITY_TYPE 
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE contract_reinstated_event.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
CONTRACT_ACTIVITY_REASON_TYPE.op,
2 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE 
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE CONTRACT_ACTIVITY_REASON_TYPE.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
CONTRACT_ACTIVITY_TYPE.op,
3 AS TBL_PRIORITY
From "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
 Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
 On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
  Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE 
 On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE CONTRACT_ACTIVITY_TYPE.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
CONTRACT_DETAIL.op,
4 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
 Left Join  "fsa-{env}-ccms-cdc".MASTER_CONTRACT
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE CONTRACT_DETAIL.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
MASTER_CONTRACT.op,
5 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".MASTER_CONTRACT
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
 Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE MASTER_CONTRACT.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
SIGNUP.op,
6 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".SIGNUP
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE 
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE SIGNUP.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
SIGNUP_TYPE.op,
7 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".SIGNUP_TYPE
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
On contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE 
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
WHERE SIGNUP_TYPE.op <> 'D'

UNION
SELECT 
MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE ADM_ST_FSA_CD,
MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE ADM_CNTY_FSA_CD,
SIGNUP_TYPE.SIGNUP_TYPE_NAME SGNP_TYPE_NM,
SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME SGNP_SUB_CAT_NM,
SIGNUP.signup_number SGNP_NBR,
SIGNUP.SIGNUP_SUBTYPE_AGREEMENT_NAME SGNP_STYPE_AGR_NM,
MASTER_CONTRACT.CONTRACT_NUMBER CTR_NBR,
CONTRACT_DETAIL.CONTRACT_SUFFIX_NUMBER CTR_SFX_NBR,
CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_DESCRIPTION CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
SIGNUP_SUB_CATEGORY.op,
8 AS TBL_PRIORITY
From  "fsa-{env}-ccms-cdc".SIGNUP_SUB_CATEGORY
Left Join "fsa-{env}-ccms-cdc".SIGNUP
On SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE = SIGNUP.SIGNUP_SUB_CATEGORY_CODE
Left Join "fsa-{env}-ccms-cdc".SIGNUP_TYPE
On SIGNUP_TYPE.SIGNUP_TYPE_CODE = SIGNUP.SIGNUP_TYPE_CODE
Left Join "fsa-{env}-ccms-cdc".MASTER_CONTRACT
On SIGNUP.SIGNUP_IDENTIFIER = MASTER_CONTRACT.SIGNUP_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_DETAIL
on CONTRACT_DETAIL.CONTRACT_IDENTIFIER = MASTER_CONTRACT.CONTRACT_IDENTIFIER
Join "fsa-{env}-ccms-cdc".contract_reinstated_event 
On contract_reinstated_event.contract_detail_identifier = CONTRACT_DETAIL.CONTRACT_DETAIL_IDENTIFIER
Left Join "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_TYPE
ON contract_reinstated_event.contract_activity_type_code = CONTRACT_ACTIVITY_TYPE.CONTRACT_ACTIVITY_TYPE_CODE
Left join   "fsa-{env}-ccms-cdc".CONTRACT_ACTIVITY_REASON_TYPE 
On contract_reinstated_event.contract_activity_reason_type_code = CONTRACT_ACTIVITY_REASON_TYPE.CONTRACT_ACTIVITY_REASON_TYPE_CODE
WHERE SIGNUP_SUB_CATEGORY.op <> 'D'
) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND contract_reinstated_event.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT DISTINCT
NULL ADM_ST_FSA_CD,
NULL ADM_CNTY_FSA_CD,
NULL SGNP_TYPE_NM,
NULL SGNP_SUB_CAT_NM,
NULL SGNP_NBR,
NULL SGNP_STYPE_AGR_NM,
NULL CTR_NBR,
NULL CTR_SFX_NBR,
NULL CTR_ACTV_RSN_TYPE_DESC,
contract_reinstated_event.contract_reinstated_event_identifier CTR_RINST_EVNT_ID,
contract_reinstated_event.contract_detail_identifier CTR_DET_ID,
contract_reinstated_event.contract_activity_reason_type_code CTR_ACTV_RSN_TYPE_CD,
contract_reinstated_event.contract_activity_type_code CTR_ACTV_TYPE_CD,
contract_reinstated_event.dafp_approved_date DAFP_APRV_DT,
contract_reinstated_event.contract_terminated_date CTR_TERM_DT,
contract_reinstated_event.contract_reinstated_reason_text CTR_RINST_RSN_TXT,
contract_reinstated_event.TERMINATED_OTHER_ACTIVITY_REASON_DESCRIPTION TERM_OT_ACTV_RSN_DESC,
contract_reinstated_event.data_status_code DATA_STAT_CD,
contract_reinstated_event.last_change_date LAST_CHG_DT,
contract_reinstated_event.creation_date CRE_DT,
contract_reinstated_event.last_change_user_name LAST_CHG_USER_NM,
contract_reinstated_event.creation_user_name CRE_USER_NM,
'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-{env}-ccms-cdc".contract_reinstated_event
WHERE contract_reinstated_event.op = 'D'