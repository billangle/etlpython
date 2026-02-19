-- CROP_TRACT_CONTRACT UPDATE INTO FARM_RECORDS_REPORTING
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
        client
    FROM "sss-farmrecords"."ibib"
    where ZZFLD0000AI = 'ACTV'
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
        instance,
        lpad(ZZFLD00001Z, 2, '0') as admin_state,
        lpad(ZZFLD000020, 3, '0') as admin_county,
        CASE WHEN ZZFLD0000B8 ='ACTV' THEN 'A'
			WHEN ZZFLD0000B8 ='IACT' THEN 'I'
			WHEN ZZFLD0000B8 ='DELE' THEN 'D'
			WHEN ZZFLD0000B8 ='PEND' THEN 'P'
			ELSE 'A'
		END AS data_status_code,
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
        case when trim(ZZFLD0000MY) is not null and trim(ZZFLD0000MY) not in ('','0')
                    then trim(ZZFLD0000MY)
            else '0'
        end as crp_contract_number,
        client
    FROM "sss-farmrecords"."ibsp"
),

sss_ibst as (
    SELECT
        ibase,
        parent,
        valto,
        instance
    FROM "sss-farmrecords"."ibst"
    where parent = '0'
    and valto = 99991231235959
),

sss_zmi_base_acre as (
	select 
		trim(zmi.ZZK0010) as crop_code,
		trim(zmi.ZZK0011) as crop_year,
        case when zmi.VALID_TO is not null AND zmi.VALID_TO <> 0
                then YEAR(CAST(date_parse(cast(zmi.VALID_TO as varchar), '%Y%m%d%H%i%S') AS DATE))
            else 0
        end as reduction_acreage_start_year,
		zmi.ZZ0012 as reduction_acreage,
        case when zmi.ZZ0020 is not null and trim(zmi.ZZ0020) not in ('0','',')')
                then cast(zmi.ZZ0020 as int)
            else 0
		end as crop_program_payment_yield,
		--NULL as crop_program_alternate_payment_yield,
		zmi.frg_guid,
        crop.crop_identifier
	from "sss-farmrecords"."zmi_base_acre" zmi
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."crop" crop
        on lpad(trim(zmi.ZZK0010),4, '0') = crop.fsa_crop_code
),

crop_tract_contract_current as (
    select
        coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
		ibsp.crp_contract_number as contract_number,
		case when zba.reduction_acreage_start_year is null 
			then cast(timepd.time_period_name as int)
		else zba.reduction_acreage_start_year end as reduction_acreage_start_year,
		zba.reduction_acreage,
		zba.crop_program_payment_yield,
		--zba.crop_program_alternate_payment_yield,
		ibib.data_status_code,
		ibib.creation_date,
		ibib.last_change_date,
		ibib.last_change_user_name,
		zba.crop_code as crop_name,
		zba.crop_year,
        zba.crop_identifier,
        ibib.farm_number,
        ibsp.tract_number
    FROM sss_ibib ibib
    join time_pd timepd
		on cast(case
					when month(current_date) >= 10 
						then year(current_date) + 1
					else year(current_date)
				end as varchar) = trim(timepd.time_period_name)
    join sss_ibst ibst
        on ibib.ibase = ibst.ibase
    join sss_ibsp ibsp
        on ibst.instance = ibsp.instance
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
        on lpad(ibib.state_fsa_code, 2, '0') || lpad(ibib.county_fsa_code, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
    join "sss-farmrecords"."ibin" ibin 
        on ibsp.instance = ibin.instance 
        and ibsp.client = ibin.client 
    join "sss-farmrecords"."z_ibase_comp_detail" zic 
        on ibin.instance = cast(zic.instance as varchar) 
        and ibin.ibase = cast(zic.ibase as varchar)
    join "sss-farmrecords"."comm_pr_frg_rel" cpf
        on zic.prod_objnr = cpf.product_guid
    inner join sss_zmi_base_acre zba
        on zba.frg_guid = cpf.fragment_guid
),

crop_tract_contract_tbl as (
	select *
    from (
        select 
            tractyearpg.tract_year_identifier as tract_year_identifier,
            ctccurr.*,
            row_number() over (partition by ctccurr.crop_identifier, tractyearpg.tract_year_identifier, ctccurr.contract_number, ctccurr.reduction_acreage_start_year ORDER BY ctccurr.creation_date desc, ctccurr.last_change_date desc) AS rownum
        from crop_tract_contract_current ctccurr
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
            on farmpg.county_office_control_identifier = ctccurr.county_office_control_identifier
            and lpad(farmpg.farm_number, 7, '0') = lpad(ctccurr.farm_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract" tractpg
            on tractpg.county_office_control_identifier = ctccurr.county_office_control_identifier
            and lpad(tractpg.tract_number, 7, '0') = lpad(ctccurr.tract_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
            on farmpg.farm_identifier =  farmyearpg.farm_identifier
            and ctccurr.time_period_identifier = farmyearpg.time_period_identifier
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_year" tractyearpg
            on farmyearpg.farm_year_identifier =  tractyearpg.farm_year_identifier
            and tractpg.tract_identifier = tractyearpg.tract_identifier
        -- where reduction_acreage_start_year >= 2014
        ) a
    where rownum = 1
)

select crop_tract_contract_identifier,
    crop_identifier,
    contract_number,
    reduction_acreage_start_year,
    reduction_acreage,
    crop_program_payment_yield,
    --crop_program_alternate_payment_yield,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    tract_year_identifier,
    'U' as cdc_oper_cd,
    now() as cdc_dt
from (
	select
	    ctc.*,
		ctcr.crop_tract_contract_identifier as crop_tract_contract_identifier,
		case
			when ctcr.crop_tract_contract_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when ctc.crop_identifier <> ctcr.crop_identifier
            and ctc.contract_number <> ctcr.contract_number
            and ctc.reduction_acreage_start_year <> ctcr.reduction_acreage_start_year
            and ctc.reduction_acreage <> ctcr.reduction_acreage
            and ctc.crop_program_payment_yield <> ctcr.crop_program_payment_yield
            and ctc.data_status_code <> ctcr.data_status_code
            and ctc.creation_date <> ctcr.creation_date
            and ctc.last_change_date <> ctcr.last_change_date
            and ctc.last_change_user_name <> ctcr.last_change_user_name
            and ctc.tract_year_identifier <> ctcr.tract_year_identifier
     	    then 'DIFF' else 'SAME'
     	end as updte
	from crop_tract_contract_tbl ctc
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."crop_tract_contract" ctcr
			on ctc.crop_identifier = ctcr.crop_identifier
            and ctc.contract_number = ctcr.contract_number
            and ctc.reduction_acreage_start_year = ctcr.reduction_acreage_start_year
            and ctc.data_status_code = ctcr.data_status_code
            and ctc.tract_year_identifier = ctcr.tract_year_identifier
	)
where cdc_op = 'U'
 and updte = 'DIFF';
