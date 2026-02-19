-- CLU_YEAR INSERT INTO FARM_RECORDS_REPORTING
with time_pd as (
	select time_period_identifier,
		time_period_name,
		time_period_start_date,
		time_period_end_date
	from "FSA-PROD-rds-pg-edv"."farm_records_reporting"."time_period"
		where data_status_code = 'A'
),

sss_ibib as (
    SELECT 
        ibase,
        lpad(ZZFLD000002, 2, '0') as administrative_state,
        lpad(ZZFLD000003, 3, '0') as administrative_count,
        ZZFLD000000 as farm_number
        FROM "sss-farmrecords"."ibib"
        where ZZFLD0000AI = 'ACTV'
),

sss_ibst as (
    SELECT 
        ibase, 
        parent,
        valto,
        instance
    FROM "sss-farmrecords"."ibst"
    WHERE ibst.parent > '0'
    AND ibst.valto = 99991231235959
    
),

sss_ibsp as (
    SELECT
        case when ZZFLD00000T <> '0' 
            then LPAD(ZZFLD00000T, 7, '0')
        else case when ZZFLD00001O is not null and ZZFLD00001O <> ' '
                    then LPAD(split_part(ZZFLD00001O, '-', 4), 7, '0')
                else '0000000'
            end
        end as tract_number,
        trim(ZZFLD0000PG) as clu_alt_guid,
        trim(ZZFLD00001N) as field_number,
        lpad(ZZFLD000021, 2, '0') as state_ansi_code,
        lpad(ZZFLD000022, 3, '0') as county_ansi_code,
        case when trim(ZZFLD00000Y) is not null AND trim(ZZFLD00000Y) not in ('0','',')')
                then trim(ZZFLD00000Y)
            else '0'
        end as congressional_district_code,
	    case when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '1' then 1549 --external I
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '2' then 1542 --external B
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '3' then 1547 --external G
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '4' then 1543 --external C
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '5' then 1550 --external J
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '6' then 1544 --external D
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '7' then 1541 --external A
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '8' then 1548 --external H
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '9' then 1549 --external F
	         when ZZFLD00001P is not null AND trim(ZZFLD00001P) = '10' then 1545 --external E
             else 0
        end as land_classification_identifier,
        case when ZZFLD00001Q is not null
                then ZZFLD00001Q
            else 0.0
        end as clu_acreage,
        case when ZZFLD00001R is not null AND trim(ZZFLD00001R) = 'EHEL' then 1683
            when ZZFLD00001R is not null AND trim(ZZFLD00001R) = 'HEL' then 1680
            when ZZFLD00001R is not null AND trim(ZZFLD00001R) = 'UHEL' then 1682
            when ZZFLD00001R is not null AND trim(ZZFLD00001R) = 'NHEL' then 1681
            else 0
        end as hel_status_identifier,
        case when trim(ZZFLD0000HR) is not null AND trim(ZZFLD0000HR) not in ('0','',')')
                then trim(ZZFLD0000HR)
            else '0'
        end as cropland_indicator_3cm,
        trim(ZZFLD00001O) as clu_description,
        trim(ZZFLD0000MY) as crp_contract_number,
        ZZFLD00001U as expiration_date,
        ZZFLD0000PY as sod_broken_out_date,
        ibsp.instance as instance,
        CASE WHEN ZZFLD00001U is not null
                THEN CAST(date_format(ZZFLD00001U, '%Y-%m-%d') AS DATE)
		END AS crp_contract_expiration_date,
        CASE WHEN ZZFLD0000PY is not null
                THEN CAST(date_format(ZZFLD0000PY, '%Y-%m-%d') AS DATE)
		END AS native_sod_conversion_date,
		case when ZZFLD00001W is not null AND trim(ZZFLD00001W) not in ('0','',')')
                then SUBSTR(ZZFLD00001W, STRPOS(ZZFLD00001W, 'C'))
            else '0'
        end as conservation_practice_identifier,
        try_cast(ZZFLD0000PZ as decimal) as sod_conversion_crop_year_1, 
        try_cast(ZZFLD0000Q0 as decimal) as sod_conversion_crop_year_2, 
        try_cast(ZZFLD0000Q1 as decimal) as sod_conversion_crop_year_3, 
        try_cast(ZZFLD0000Q2 as decimal) as sod_conversion_crop_year_4,
        CASE WHEN ZZFLD0000B8 ='ACTV' THEN 'A'
			WHEN ZZFLD0000B8 ='IACT' THEN 'I'
			WHEN ZZFLD0000B8 ='DELE' THEN 'D'
			WHEN ZZFLD0000B8 ='PEND' THEN 'P'
			ELSE 'A'
		END AS data_status_code,
		CASE WHEN ibsp.crtim is not null
                THEN CAST(date_format(ibsp.crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS creation_date,
        CASE WHEN ibsp.UPTIM is not null
                THEN CAST(date_format(ibsp.UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS last_change_date,
        case when ibsp.upnam is not null and trim(ibsp.upnam) not in ('0','',')')
                then trim(ibsp.upnam)
            when ibsp.crnam is not null and trim(ibsp.crnam) not in ('0','',')')
                then trim(ibsp.crnam)
            else 'BLANK'
        end as last_change_user_name,
        ibsp.ZZFLD000021 as phy_fld_st,
        ibsp.ZZFLD000022 as phy_fld_cnty
    from "sss-farmrecords"."ibsp" ibsp
),

domain_value_sss_ibsp as (
    select conservation_practice_identifier_mapped as conservation_practice_identifier_mapped,
    sss_ibsp.*
    from sss_ibsp
    left join (
        select dv.domain_value_identifier as conservation_practice_identifier_mapped, 
        dv.domain_character_value
        from "FSA-PROD-rds-pg-edv"."farm_records_reporting"."domain_value" dv
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."domain" d 
            on d.domain_identifier = dv.domain_identifier
        where d.domain_description = 'conservation practice identifiers') domain
        on trim(sss_ibsp.conservation_practice_identifier) = trim(domain.domain_character_value)
),

clu_year_current as (
    SELECT
        coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
        ibsp.clu_alt_guid as clu_alternate_identifier,
        LPAD(ibsp.field_number, 7, '0') as field_number,
        ibsp.phy_fld_cnty as location_county_fsa_code,
        ibsp.phy_fld_st as location_state_fsa_code,
        ibsp.state_ansi_code as state_ansi_code,
	    ibsp.county_ansi_code as county_ansi_code,
	    LPAD(ibsp.congressional_district_code, 2, '0') as congressional_district_code,
        ibsp.land_classification_identifier as land_classification_identifier,
        ibsp.clu_acreage as clu_acreage,
        ibsp.hel_status_identifier as hel_status_identifier,
        ibsp.cropland_indicator_3cm as cropland_indicator_3cm,
        ibsp.clu_description as clu_description,
        ibsp.crp_contract_number as crp_contract_number,
        ibsp.crp_contract_expiration_date as crp_contract_expiration_date,
        ibsp.conservation_practice_identifier_mapped as conservation_practice_identifier,
        ibsp.native_sod_conversion_date as native_sod_conversion_date,
        ibsp.sod_conversion_crop_year_1 as sod_conversion_crop_year_1,
        ibsp.sod_conversion_crop_year_2 as sod_conversion_crop_year_2,
        ibsp.sod_conversion_crop_year_3 as sod_conversion_crop_year_3,
        ibsp.sod_conversion_crop_year_4 as sod_conversion_crop_year_4,
        ibsp.last_change_date as last_change_date,
        ibsp.last_change_user_name as last_change_user_name,
        ibsp.creation_date as creation_date,
        ibsp.data_status_code as data_status_code,
        ibib.farm_number as farm_number,
        -- ibst.parent as tract_number
        ibsp.tract_number
        FROM sss_ibib ibib
        join time_pd timepd
            on cast(case
                        when month(current_date) >= 10 
                            then year(current_date) + 1
                        else year(current_date)
                    end as varchar) = trim(timepd.time_period_name)
        inner join sss_ibst ibst
            on ibib.ibase = ibst.ibase
        inner join domain_value_sss_ibsp ibsp
            on ibst.instance = ibsp.instance
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
            on lpad(ibib.administrative_state, 2, '0') || lpad(ibib.administrative_count, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
        where timepd.time_period_name >= '2014'
),

clu_year_tbl as (
    select * 
    from (
    SELECT
        typg.tract_year_identifier as tract_year_identifier,
        clu_year_current.*,
        row_number() over (partition by typg.tract_year_identifier, clu_year_current.field_number, clu_year_current.location_county_fsa_code, clu_year_current.location_state_fsa_code ORDER BY clu_year_current.creation_date desc, clu_year_current.last_change_date desc) AS rownum
        from clu_year_current
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
        on farmpg.county_office_control_identifier = clu_year_current.county_office_control_identifier
        and lpad(farmpg.farm_number, 7, '0') = lpad(clu_year_current.farm_number, 7, '0')
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract" tractpg
        on tractpg.county_office_control_identifier = clu_year_current.county_office_control_identifier
        and lpad(tractpg.tract_number, 7, '0') = lpad(clu_year_current.tract_number, 7, '0')
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
        on farmpg.farm_identifier =  farmyearpg.farm_identifier
        and clu_year_current.time_period_identifier =  farmyearpg.time_period_identifier
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_year" typg
        on tractpg.tract_identifier = typg.tract_identifier
        and farmyearpg.farm_year_identifier = typg.farm_year_identifier)a
    where a.rownum = 1
)

select --clu_year_identifier,
    tract_year_identifier,
    clu_alternate_identifier,
    field_number,
    location_county_fsa_code,
    location_state_fsa_code,
    state_ansi_code,
    county_ansi_code,
    congressional_district_code,
    land_classification_identifier,
    clu_acreage,
    hel_status_identifier,
    cropland_indicator_3cm,
    clu_description,
    crp_contract_number,
    crp_contract_expiration_date,
    conservation_practice_identifier,
    native_sod_conversion_date,
    sod_conversion_crop_year_1,
    sod_conversion_crop_year_2,
    sod_conversion_crop_year_3,
    sod_conversion_crop_year_4,
    last_change_date,
    last_change_user_name,
    creation_date,
    data_status_code,
	'I' as cdc_oper_cd,
    now() as cdc_dt
from (
	select
	    clu.*,
		--clur.clu_year_identifier as clu_year_identifier,
		case
			when clur.clu_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when clu.tract_year_identifier <> clur.tract_year_identifier
            and clu.clu_alternate_identifier <> clur.clu_alternate_identifier
            and clu.field_number <> clur.field_number
            and clu.location_county_fsa_code <> clur.location_county_fsa_code
            and clu.location_state_fsa_code <> clur.location_state_fsa_code
            and clu.state_ansi_code <> clur.state_ansi_code
            and clu.county_ansi_code <> clur.county_ansi_code
            and clu.congressional_district_code <> clur.congressional_district_code
            and clu.land_classification_identifier <> clur.land_classification_identifier
            and clu.clu_acreage <> clur.clu_acreage
            and clu.hel_status_identifier <> clur.hel_status_identifier
            and clu.cropland_indicator_3cm <> clur.cropland_indicator_3cm
            and clu.clu_description <> clur.clu_description
            and clu.crp_contract_number <> clur.crp_contract_number
            and clu.crp_contract_expiration_date <> clur.crp_contract_expiration_date
            and cast(clu.conservation_practice_identifier as integer) <> clur.conservation_practice_identifier
            and clu.native_sod_conversion_date <> clur.native_sod_conversion_date
            and clu.sod_conversion_crop_year_1 <> clur.sod_conversion_crop_year_1
            and clu.sod_conversion_crop_year_2 <> clur.sod_conversion_crop_year_2
            and clu.sod_conversion_crop_year_3 <> clur.sod_conversion_crop_year_3
            and clu.sod_conversion_crop_year_4 <> clur.sod_conversion_crop_year_4
            and clu.data_status_code <> clur.data_status_code
     	    then 'DIFF' else 'SAME'
     	end as updte
	from clu_year_tbl clu
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."clu_year" clur
			on clu.tract_year_identifier = clur.tract_year_identifier
            and clu.field_number = clur.field_number
            and clu.location_county_fsa_code = clur.location_county_fsa_code
            and clu.location_state_fsa_code = clur.location_state_fsa_code
            and clu.data_status_code = clur.data_status_code
	)
where cdc_op = 'U'