SELECT creation_date As CRE_DT,
LTrim(RTrim(domain_description) ) As DMN_DESC,
domain_identifier As DMN_ID,
LTrim(RTrim(domain_name) ) As DMN_NM,
last_change_date As LAST_CHG_DT,
LTrim(RTrim(last_change_user_name) ) As LAST_CHG_USER_NM
FROM farm_records_reporting.domain