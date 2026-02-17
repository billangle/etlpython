-- CLU_PRODUCER_YEAR INSERT INTO FARM_RECORDS_REPORTING
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
        ibsp.instance,
        lpad(ZZFLD00001Z, 2, '0') as admin_state,
        lpad(ZZFLD000020, 3, '0') as admin_county,
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
        ibsp.client,
        trim(ZZFLD00001N) as field_number,
        ibsp.ZZFLD000021 as phy_fld_st,
        ibsp.ZZFLD000022 as phy_fld_cnty
    FROM "sss-farmrecords"."ibsp"

),

sss_ibst as (
    SELECT
        ibase,
        parent,
        valto,
        instance
    FROM "sss-farmrecords"."ibst"
    where parent != '0' 
    and valto = 99991231235959
),

sss_zmi_field_excp as (
    select 
        frg_guid,
        -- case when ZZ0016 is not null AND trim(ZZ0016) not in ('0','',')')
        --             then CAST(ZZ0016 AS INTEGER)
        --         else 0
        -- end as rma_hel_exception_code,
        -- case when ZZ0017 is not null AND trim(ZZ0017) not in ('0','',')')
        --             then CAST(ZZ0017 AS INTEGER)
        --         else 0
        -- end as rma_cw_exception_code,
        -- case when ZZ0018 is not null AND trim(ZZ0018) not in ('0','',')')
        --             then CAST(ZZ0018 AS INTEGER)
        --         else 0
        -- end as rma_pcw_exception_code,
        CASE WHEN ZZ0013 is not null
                    THEN CAST(date_format(ZZ0013, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS hel_appeals_exhausted_date,
        CASE WHEN ZZ0014 is not null
                    THEN CAST(date_format(ZZ0014, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS cw_appeals_exhausted_date,
        CASE WHEN ZZ0015 is not null
                    THEN CAST(date_format(ZZ0015, '%Y-%m-%d') AS DATE)
                ELSE null
        END AS pcw_appeals_exhausted_date,
        case when ZZ0010 is not null and trim(ZZ0010) = 'GF' then 3
             when ZZ0010 is not null and trim(ZZ0010) = 'NA' then 2
             when ZZ0010 is not null and trim(ZZ0010) = 'LT' then 1
             when ZZ0010 is not null and trim(ZZ0010) = 'EH' then 6
             when ZZ0010 is not null and trim(ZZ0010) = 'NAR' then 5
             when ZZ0010 is not null and trim(ZZ0010) = 'AR' then 4
        end as hel_exception_code,
        case when ZZ0011 is not null AND trim(ZZ0011) = 'WR' then 13
             when ZZ0011 is not null AND trim(ZZ0011) = 'TP' then 14
             when ZZ0011 is not null AND trim(ZZ0011) = 'NP' then 15
             when ZZ0011 is not null AND trim(ZZ0011) = 'AR' then 10
             when ZZ0011 is not null AND trim(ZZ0011) = 'NAR' then 11
             when ZZ0011 is not null AND trim(ZZ0011) = 'GF' then 12
        end as cw_exception_code,
        case when ZZ0012 is not null and trim(ZZ0012) = 'AR' then 20
             when ZZ0012 is not null and trim(ZZ0012) = 'NAR' then 20
             when ZZ0012 is not null and trim(ZZ0012) = 'GF' then 22
        end as pcw_exception_code,
        case when ZZ0016 is not null and trim(ZZ0016) = 'GF' then 3
             when ZZ0016 is not null and trim(ZZ0016) = 'NA' then 2
             when ZZ0016 is not null and trim(ZZ0016) = 'LT' then 1
             when ZZ0016 is not null and trim(ZZ0016) = 'EH' then 6
             when ZZ0016 is not null and trim(ZZ0016) = 'NAR' then 5
             when ZZ0016 is not null and trim(ZZ0016) = 'AR' then 4
        end as rma_hel_exception_code,
        case when ZZ0017 is not null AND trim(ZZ0017) = 'WR' then 13
             when ZZ0017 is not null AND trim(ZZ0017) = 'TP' then 14
             when ZZ0017 is not null AND trim(ZZ0017) = 'NP' then 15
             when ZZ0017 is not null AND trim(ZZ0017) = 'AR' then 10
             when ZZ0017 is not null AND trim(ZZ0017) = 'NAR' then 11
             when ZZ0017 is not null AND trim(ZZ0017) = 'GF' then 12
        end as rma_cw_exception_code,
        case when ZZ0018 is not null and trim(ZZ0018) = 'AR' then 20
             when ZZ0018 is not null and trim(ZZ0018) = 'NAR' then 20
             when ZZ0018 is not null and trim(ZZ0018) = 'GF' then 22
        end as rma_pcw_exception_code,
        case when VALID_FROM is not null AND VALID_FROM <> 0
                then CAST(date_parse(cast(VALID_FROM as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_start_date,
        case when VALID_TO is not null AND VALID_TO <> 0
                then CAST(date_parse(cast(VALID_TO as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_end_date,
        zzk0011
    from "sss-farmrecords"."zmi_field_excp"
),

sss_ibpart as (
    select *,
    row_number() over (partition by segment_recno ORDER BY crtim desc) AS rownumpart
    from "sss-farmrecords"."ibpart"
    where segment = 2 
),

clu_producer_year_current as (
	select coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
        lpad(ibib.farm_number, 7, '0') as farm_number,
        ibsp.tract_number,
        LPAD(ibsp.field_number, 7, '0') as field_number,
        cast(c.bpext as integer) as core_customer_identifier,
        CASE WHEN crmd_partner.partner_fct = 'ZOTNT'
        		then 165
        end as producer_involvement_code,
        zfe.hel_exception_code as clu_producer_hel_exception_code,
        zfe.cw_exception_code as clu_producer_cw_exception_code, 
        zfe.pcw_exception_code as clu_producer_pcw_exception_code,
        ibib.data_status_code AS data_status_code,
        ibib.creation_date AS creation_date,
        ibib.last_change_date AS last_change_date,
        ibib.last_change_user_name AS last_change_user_name,
        zfe.hel_appeals_exhausted_date as hel_appeals_exhausted_date,
        zfe.cw_appeals_exhausted_date as cw_appeals_exhausted_date,
        zfe.pcw_appeals_exhausted_date as pcw_appeals_exhausted_date,
        zfe.rma_hel_exception_code as clu_producer_rma_hel_exception_code,
        zfe.rma_cw_exception_code as clu_producer_rma_cw_exception_code,
        zfe.rma_pcw_exception_code as clu_producer_rma_pcw_exception_code
    FROM sss_ibib ibib
    join time_pd timepd
		on cast(case
					when month(current_date) >= 10 
						then year(current_date) + 1
					else year(current_date)
				end as varchar) = trim(timepd.time_period_name)
    join "sss-farmrecords"."ibin" ibin 
        on ibib.ibase = ibin.ibase
        and ibib.client = ibin.client
    join sss_ibpart ibpart
        on ibin.in_guid = ibpart.segment_recno
        and ibpart.segment = 2 --farm_level data segment = 1
        and ibpart.valto = 99991231235959
    join "sss-farmrecords"."crmd_partner" crmd_partner
        on ibpart.partnerset = crmd_partner.guid
    join sss_ibst ibst
        on ibib.ibase = ibst.ibase
    join sss_ibsp ibsp
        on ibst.instance = ibsp.instance
        and ibst.parent !='0'
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
        on lpad(ibib.state_fsa_code, 2, '0') || lpad(ibib.county_fsa_code, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
    join "sss-farmrecords"."z_ibase_comp_detail" zic 
        on ibin.instance = cast(zic.instance as varchar) 
        and ibin.ibase = cast(zic.ibase as varchar)
    left join "sss-farmrecords"."comm_pr_frg_rel" cpf
        on zic.prod_objnr = cpf.product_guid
    left join sss_zmi_field_excp zfe 
        on zfe.frg_guid = cpf.fragment_guid -- field execptions
    left join "FSA-PROD-rds-pg-edv"."crm_ods"."but000" c
        on crmd_partner.partner_no = c.partner_guid
    where timepd.time_period_name >= '2014'
    and c.bpext not in ('DUPLICATE','11876423_D')
),

clu_producer_year_tbl as (
	select * 
    from (
        select
            cluyearpg.clu_year_identifier as clu_year_identifier,
            curr.*,
            row_number() over (partition by cluyearpg.clu_year_identifier, curr.core_customer_identifier ORDER BY curr.creation_date desc, curr.last_change_date desc) AS rownum
        FROM clu_producer_year_current curr
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
            on farmpg.county_office_control_identifier = curr.county_office_control_identifier
            and lpad(farmpg.farm_number, 7, '0') = lpad(curr.farm_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract" tractpg
            on tractpg.county_office_control_identifier = curr.county_office_control_identifier
            and lpad(tractpg.tract_number, 7, '0') = lpad(curr.tract_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
            on farmpg.farm_identifier =  farmyearpg.farm_identifier
            and curr.time_period_identifier = farmyearpg.time_period_identifier
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_year" tractyearpg
            on farmyearpg.farm_year_identifier =  tractyearpg.farm_year_identifier
            and tractpg.tract_identifier = tractyearpg.tract_identifier
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."clu_year" cluyearpg
            on cluyearpg.tract_year_identifier = tractyearpg.tract_year_identifier
            and cluyearpg.field_number = curr.field_number
            -- and cluyearpg.location_county_fsa_code = tractpg.location_county_fsa_code
            -- and cluyearpg.location_state_fsa_code = tractpg.location_state_fsa_code
            ) a
    where rownum = 1
)

select --clu_producer_year_identifier,
    clu_year_identifier,
    core_customer_identifier,
    producer_involvement_code,
    clu_producer_hel_exception_code,
    clu_producer_cw_exception_code,
    clu_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    clu_producer_rma_hel_exception_code,
    clu_producer_rma_cw_exception_code,
    clu_producer_rma_pcw_exception_code,
    'I' as cdc_oper_cd,
    now() as cdc_dt
from (
	select
	    cpy.*,
		--cpyr.clu_producer_year_identifier as clu_producer_year_identifier,
		case
			when cpyr.clu_producer_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when cpy.clu_year_identifier <> cpyr.clu_year_identifier
            and cpy.core_customer_identifier <> cpyr.core_customer_identifier
            and cpy.producer_involvement_code <> cpyr.producer_involvement_code
            and cpy.clu_producer_hel_exception_code <> cpyr.clu_producer_hel_exception_code
            and cpy.clu_producer_cw_exception_code <> cpyr.clu_producer_cw_exception_code
            and cpy.clu_producer_pcw_exception_code <> cpyr.clu_producer_pcw_exception_code
            and cpy.data_status_code <> cpyr.data_status_code
            and cpy.hel_appeals_exhausted_date <> cpyr.hel_appeals_exhausted_date
            and cpy.cw_appeals_exhausted_date <> cpyr.cw_appeals_exhausted_date
            and cpy.pcw_appeals_exhausted_date <> cpyr.pcw_appeals_exhausted_date
            and cpy.clu_producer_rma_hel_exception_code <> cpyr.clu_producer_rma_hel_exception_code
            and cpy.clu_producer_rma_cw_exception_code <> cpyr.clu_producer_rma_cw_exception_code
            and cpy.clu_producer_rma_pcw_exception_code <> cpyr.clu_producer_rma_pcw_exception_code
     	    then 'DIFF' else 'SAME'
     	end as updte
	from clu_producer_year_tbl cpy
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."clu_producer_year" cpyr
			on cpy.clu_year_identifier = cpyr.clu_year_identifier
            and cpy.core_customer_identifier = cpyr.core_customer_identifier
            and cpy.data_status_code = cpyr.data_status_code 
	)
where cdc_op = 'I'