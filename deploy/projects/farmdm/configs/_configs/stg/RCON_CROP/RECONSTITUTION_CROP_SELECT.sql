SELECT DISTINCT
    reconstitution_crop_identifier,
    reconstitution_identifier,
    crop_identifier,
    reconstitution_division_method_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name
FROM
    farm_records_reporting.reconstitution_crop;