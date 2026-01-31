SELECT domain_value_identifier As DMN_VAL_ID,
domain_identifier As DMN_ID,
LTrim(RTrim(domain_character_value) ) As DMN_CHAR_VAL,
LTrim(RTrim(default_value_indicator) ) As DFLT_VAL_IND,
display_sequence_number As DPLY_SEQ_NBR,
LTrim(RTrim(domain_value_name) ) As DMN_VAL_NM,
LTrim(RTrim(domain_value_description) ) As DMN_VAL_DESC,
creation_date As CRE_DT,
last_change_date As LAST_CHG_DT,
LTrim(RTrim(last_change_user_name) ) As LAST_CHG_USER_NM
FROM farm_records_reporting.domain_value