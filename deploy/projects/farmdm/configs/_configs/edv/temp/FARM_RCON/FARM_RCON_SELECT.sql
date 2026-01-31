SELECT DISTINCT
    farm_reconstitution_identifier,
    reconstitution_identifier,
    parent_farm_year_identifier,
    resulting_farm_year_identifier,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name
FROM
    farm_records_reporting.farm_reconstitution;