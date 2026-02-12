--FBP_RPT.FBP_ACCT_TYPE
select distinct rownumber as FBP_ACCT_TYPE_ID, "column 2"  as ACCT_TYPE_DESC, 'A' as data_status_code
from "fsa-prod-dmcleanstart"."ca_spec_multi_text_fields"
where "column 2" is not null and "column 2" <> ' '
;

--FBP_RPT.FBP_CUST
as client_info
(SELECT "Client_Info_ID",
"Name",
"Location",
"Tier 3 Location" as Tier_3_Location,
"FSA Core Customer ID" as FSA_Core_Customer_ID 
FROM "fsa-prod-dmcleanstart"."client_info" where "Client_Info_ID" is not null --and "Client_Info_ID" <>' '
),
as fbp_acct_type
(SELECT FBP_ACCT_TYPE_ID, 
FBP_ACCT_TYPE_DESC 
FROM FBP_RPT.FBP_ACCT_TYPE where FBP_ACCT_TYPE_DESC in ('Active Account','Special Classification Act.') --switch to redshift join
),
as FBP_CUST
(SELECT
client_info_id,
"column 2" as ACCT_TYPE_DESC   
from "fsa-prod-dmcleanstart"."ca_spec_multi_text_fields"
where "column 2" is not null and "column 2" <> ' '
and "Client_Info_ID" is not null and "column 2" in ('Active Account','Special Classification Act.')
),

select 

; 

--FBP_RPT.FBP_SCOR_DET
SELECT "Client_Info_ID","CA Description" as CA_Description,"Score Date" as Score_Date,"Overall Score Value" as Overall_Score_Value FROM "fsa-prod-dmcleanstart"."ca_scoring_and_rating"

--Athena queries

SELECT ci."Client_Info_ID",
ci."Name",
ci."Location",
ci."Tier 3 Location" as Tier_3_Location,
ci."FSA Core Customer ID" as FSA_Core_Customer_ID,
ca."column 2" as ACCT_TYPE_DESC 
FROM "fsa-prod-dmcleanstart"."client_info" ci 
join "fsa-prod-dmcleanstart"."ca_spec_multi_text_fields" ca 
    on ci."Client_Info_ID" = ca."Client_Info_ID" 
where ca."column 2" in ('Active Account','Special Classification Act.')
and ci."Client_Info_ID" is not null 
;


),
as fbp_acct_type
(SELECT FBP_ACCT_TYPE_ID, 
FBP_ACCT_TYPE_DESC 
FROM FBP_RPT.FBP_ACCT_TYPE where FBP_ACCT_TYPE_DESC in ('Active Account','Special Classification Act.') --switch to redshift join
),
as FBP_CUST
(SELECT
client_info_id,
"column 2" as ACCT_TYPE_DESC   
from "fsa-prod-dmcleanstart"."ca_spec_multi_text_fields"
where "column 2" is not null and "column 2" <> ' '
and "Client_Info_ID" is not null and "column 2" in ('Active Account','Special Classification Act.')
;
