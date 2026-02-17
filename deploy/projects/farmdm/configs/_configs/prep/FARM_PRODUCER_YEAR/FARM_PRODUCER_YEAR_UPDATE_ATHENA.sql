-- FARM_PRODUCER_YEAR UPDATE INTO FARM_RECORDS_REPORTING
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
        ib_guid_16,
        CASE WHEN crtim is not null
                THEN CAST(date_format(crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS creation_date,
        CASE WHEN UPTIM is not null
                THEN CAST(date_format(UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS last_change_date,
        case when upnam is not null and trim(upnam) not in ('0','',')')
                then trim(upnam)
            when crnam is not null and trim(crnam) not in ('0','',')')
                then trim(crnam)
            else 'BLANK'
        end as last_change_user_name,
        lpad(ZZFLD000002, 2, '0') as state_fsa_code,
        lpad(ZZFLD000003, 3, '0') as county_fsa_code,
        ZZFLD000000 as farm_number,
        cstatus,
        CASE WHEN ZZFLD0000AI = 'ACTV' THEN 'A'
            WHEN ZZFLD0000AI = 'IACT' THEN 'I'
            WHEN ZZFLD0000AI = 'INCR' THEN 'I'
            WHEN ZZFLD0000AI = 'PEND' THEN 'P'
            WHEN ZZFLD0000AI = 'DRFT' THEN 'D'
            WHEN ZZFLD0000AI = 'X' THEN 'I'
            ELSE 'A'
        END as data_status_code,
        upnam,
        crnam,
        ibase,
        client,
        case when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'GF' then 3
            when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'NA' then 2
            when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'LT' then 1
            when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'EH' then 6
            when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'NAR' then 5
            when ZZFLD0000EZ is not null and trim(ZZFLD0000EZ) = 'AR' then 4
        end as farm_producer_hel_exception_code,
        case when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'WR' then 13
             when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'TP' then 14
             when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'NP' then 15
             when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'AR' then 10
             when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'NAR' then 11
             when ZZFLD0000F0 is not null and trim(ZZFLD0000F0) = 'GF' then 12
        end as farm_producer_cw_exception_code,
        case when ZZFLD0000F1 is not null and trim(ZZFLD0000F1) = 'AR' then 20
             when ZZFLD0000F1 is not null and trim(ZZFLD0000F1) = 'NAR' then 20
             when ZZFLD0000F1 is not null and trim(ZZFLD0000F1) = 'GF' then 22
        end as farm_producer_pcw_exception_code,
        case when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'GF' then 3
            when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'NA' then 2
            when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'LT' then 1
            when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'EH' then 6
            when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'NAR' then 5
            when ZZFLD0000V3 is not null and trim(ZZFLD0000V3) = 'AR' then 4
        end as farm_producer_rma_hel_exception_code,
        case when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'WR' then 13
             when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'TP' then 14
             when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'NP' then 15
             when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'AR' then 10
             when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'NAR' then 11
             when ZZFLD0000V4 is not null and trim(ZZFLD0000V4) = 'GF' then 12
        end as farm_producer_rma_cw_exception_code,
        case when ZZFLD0000V5 is not null and trim(ZZFLD0000V5) = 'AR' then 20
             when ZZFLD0000V5 is not null and trim(ZZFLD0000V5) = 'NAR' then 20
             when ZZFLD0000V5 is not null and trim(ZZFLD0000V5) = 'GF' then 22
        end as farm_producer_rma_pcw_exception_code,
        CASE WHEN ZZFLD0000UT is not null
                    THEN CAST(date_format(ZZFLD0000UT, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS hel_appeals_exhausted_date,
        CASE WHEN ZZFLD0000UU is not null
                    THEN CAST(date_format(ZZFLD0000UU, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS cw_appeals_exhausted_date,
        CASE WHEN ZZFLD0000UV is not null
                    THEN CAST(date_format(ZZFLD0000UV, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS pcw_appeals_exhausted_date
    FROM "sss-farmrecords"."ibib"
    where ZZFLD0000AI = 'ACTV'
),

sss_zmi_farm_partner as (
    select 
        frg_guid,
        zzk0011,
        ZZK0010 as producer_involvement_code,
        case when VALID_FROM is not null AND VALID_FROM <> 0
                then CAST(date_parse(cast(VALID_FROM as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_start_date,
        case when VALID_TO is not null AND VALID_TO <> 0
                then CAST(date_parse(cast(VALID_TO as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_end_date
    FROM "sss-farmrecords"."zmi_farm_partn" 
),

sss_ibpart as (
    select *,
    row_number() over (partition by segment_recno ORDER BY crtim desc) AS rownumpart
    from "sss-farmrecords"."ibpart"
),

farm_producer_year_current as (
    select distinct 
        coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
        cast(c.bpext as integer) as core_customer_identifier,
        CASE when crmd_partner.partner_fct = 'ZOPTR'
        	    then 160
        end as producer_involvement_code,   
        NULL as producer_involvement_interrupted_indicator, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        -- zba.producer_involvement_start_date as producer_involvement_start_date,
        -- zba.producer_involvement_end_date as producer_involvement_end_date,
        ibib.farm_producer_hel_exception_code,
        ibib.farm_producer_cw_exception_code,
        ibib.farm_producer_pcw_exception_code,
        ibib.farm_producer_rma_hel_exception_code,
        ibib.farm_producer_rma_cw_exception_code,
        ibib.farm_producer_rma_pcw_exception_code,
        ibib.data_status_code,
        ibib.creation_date,
        ibib.last_change_date,
        ibib.last_change_user_name,
        ibib.state_fsa_code,
        ibib.county_fsa_code,
        lpad(ibib.farm_number, 7, '0') as farm_number,
        ibib.hel_appeals_exhausted_date,
        ibib.cw_appeals_exhausted_date,
        ibib.pcw_appeals_exhausted_date
    FROM sss_ibib ibib
    join time_pd timepd
		on cast(case
					when month(current_date) >= 10
						then year(current_date) + 1
					else year(current_date)
				end as varchar) = trim(timepd.time_period_name)
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
        on lpad(ibib.state_fsa_code, 2, '0') || lpad(ibib.county_fsa_code, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
    join sss_ibpart ibpart
        on ibib.ib_guid_16 = ibpart.segment_recno
        and ibpart.segment = 1 
        and ibpart.valto = 99991231235959
        -- and rownumpart = 1 
    join "sss-farmrecords"."crmd_partner" crmd_partner
        on ibpart.partnerset = crmd_partner.guid
    left join "FSA-PROD-rds-pg-edv"."crm_ods"."but000" c
        on crmd_partner.partner_no = c.partner_guid
    where timepd.time_period_name >= '2014'  
),

farm_producer_year_tbl as (
	select * 
	from (
    	select 
    		farmpg.farm_identifier as farm_identifier, 
            farmyearpg.farm_year_identifier as farm_year_identifier,
            farmpdcryr.*,
            row_number() over (partition by farmpdcryr.core_customer_identifier, farmyearpg.farm_year_identifier ORDER BY farmpdcryr.creation_date desc, farmpdcryr.last_change_date desc) AS rownum
        from farm_producer_year_current farmpdcryr
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
            on farmpg.county_office_control_identifier = farmpdcryr.county_office_control_identifier
            and lpad(farmpg.farm_number, 7, '0') = lpad(farmpdcryr.farm_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
            on farmpg.farm_identifier =  farmyearpg.farm_identifier
            and farmpdcryr.time_period_identifier =  farmyearpg.time_period_identifier) a
        where rownum = 1
)

select farm_producer_year_identifier,
    core_customer_identifier,
    farm_year_identifier,
    producer_involvement_code,
    --producer_involvement_interrupted_indicator,
    -- producer_involvement_start_date,
    -- producer_involvement_end_date,
    farm_producer_hel_exception_code,
    farm_producer_cw_exception_code,
    farm_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    time_period_identifier,
    state_fsa_code,
    county_fsa_code,
    farm_identifier,
    farm_number,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    farm_producer_rma_hel_exception_code,
    farm_producer_rma_cw_exception_code,
    farm_producer_rma_pcw_exception_code,
    'U' as cdc_oper_cd,
    now() as cdc_dt
from (
	select
	    fpy.*,
		fpyr.farm_producer_year_identifier as farm_producer_year_identifier,
		case
			when fpyr.farm_producer_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when fpy.core_customer_identifier <> fpyr.core_customer_identifier
            and fpy.farm_year_identifier <> fpyr.farm_year_identifier
            and fpy.producer_involvement_code <> fpyr.producer_involvement_code
            -- and fpy.producer_involvement_start_date <> fpyr.producer_involvement_start_date
            -- and fpy.producer_involvement_end_date <> fpyr.producer_involvement_end_date
            and fpy.farm_producer_hel_exception_code <> fpyr.farm_producer_hel_exception_code
            and fpy.farm_producer_cw_exception_code <> fpyr.farm_producer_cw_exception_code
            and fpy.farm_producer_pcw_exception_code <> fpyr.farm_producer_pcw_exception_code
            and fpy.farm_producer_rma_hel_exception_code <> fpyr.farm_producer_rma_hel_exception_code
            and fpy.farm_producer_rma_cw_exception_code <> fpyr.farm_producer_rma_cw_exception_code
            and fpy.farm_producer_rma_pcw_exception_code <> fpyr.farm_producer_rma_pcw_exception_code
            and fpy.data_status_code <> fpyr.data_status_code
            and fpy.time_period_identifier <> fpyr.time_period_identifier
            and fpy.farm_number <> fpyr.farm_number
            and fpy.hel_appeals_exhausted_date <> fpyr.hel_appeals_exhausted_date
            and fpy.cw_appeals_exhausted_date <> fpyr.cw_appeals_exhausted_date
            and fpy.pcw_appeals_exhausted_date <> fpyr.pcw_appeals_exhausted_date
     	    then 'DIFF' else 'SAME'
     	end as updte
	from farm_producer_year_tbl fpy
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_producer_year" fpyr
			on fpy.core_customer_identifier = fpyr.core_customer_identifier
            and fpy.farm_year_identifier = fpyr.farm_year_identifier
            and fpy.data_status_code = fpyr.data_status_code
            -- and fpyr.data_status_code = 'A'
	)
where cdc_op = 'U'
 and updte = 'DIFF';
