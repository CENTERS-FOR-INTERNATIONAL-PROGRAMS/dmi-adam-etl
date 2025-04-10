SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    given || ' ' || family || ' (' || type_of_case || ')' AS contact_tree_label,
    mform_id::text AS mform_id,
    mform_event::text AS mform_event,
    event_ident::text AS event_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    (CASE WHEN df_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS completed,
    (CASE WHEN df_parent_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS parent_completed,
    created_role::text AS created_role,
    modified_role::text AS modified_role,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    syndrome::text AS syndrome,
    disease::text AS disease,
    CASE WHEN date_of_onset_rash ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_onset_rash, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_onset_rash ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_onset_rash, 'DD/MM/YYYY'), 'YYYY"-"IW') ELSE NULL END AS epi_week,
    epid::text AS epid,
    date_of_investigation::text AS date_of_investigation,
    type_of_case::text AS type_of_case,
    given::text AS given_name,
    family::text AS family_name,
    sex::text AS sex,
    CASE 
        WHEN age_years ~ '^[0-9]+(\.[0-9]+)?$' THEN age_years::float
        ELSE NULL
    END AS age_years,
    CASE 
        WHEN age_months ~ '^[0-9]+(\.[0-9]+)?$' THEN age_months::float
        ELSE NULL
    END AS age_months,
    CASE 
        WHEN age_days ~ '^[0-9]+(\.[0-9]+)?$' THEN age_days::float
        ELSE NULL
    END AS age_days,
    CASE
        WHEN NOT (age_years ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN age_years::float < 2 THEN '0-2 yrs'
        WHEN age_years::float BETWEEN 2 AND 5 THEN '2-5 yrs'
        WHEN age_years::float BETWEEN 5 AND 16 THEN '5-16 yrs'
        WHEN age_years::float > 16 THEN '16+ yrs'
        ELSE 'Unknown'
    END AS age_group,
    date_of_birth::text AS date_of_birth,
    phone_number::text AS phone_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    marital_status::text AS marital_status,
    case_is_pregnant::text AS case_is_pregnant,
    case_has_insurance_cover::text AS case_has_insurance_cover,
    level_of_education::text AS level_of_education,
    type_of_residence::text AS type_of_residence,
    country::text AS country,
    county::text AS county,
    subcounty::text AS subcounty,
    ward::text AS ward,
    town_village_camp::text AS town_village_camp,
    landmark::text AS landmark,
    title_residence::text AS title_residence,
    title_parent_guardian::text AS title_parent_guardian,
    guardian_family_name::text AS guardian_family_name,
    guardian_given_name::text AS guardian_given_name,
    guardian_phone_number::text AS guardian_phone_number,
    title_respondent::text AS title_respondent,
    respondent::text AS respondent,
    respondent_family_name::text AS respondent_family_name,
    respondent_given_name::text AS respondent_given_name,
    respondent_address::text AS respondent_address,
    respondent_phone_number::text AS respondent_phone_number,
    respondent_relationship::text AS respondent_relationship,
    case_has_travelled::text AS case_has_travelled,
    title_origin::text AS title_origin,
    origin_country::text AS origin_country,
    origin_county::text AS origin_county,
    origin_city_town_illage_camp::text AS origin_city_town_illage_camp,
    date_of_departure::text AS date_of_departure,
    title_destination::text AS title_destination,
    destination_country::text AS destination_country,
    destination_county::text AS destination_county,
    destination_city_town_village_camp::text AS destination_city_town_village_camp,
    date_of_arrival::text AS date_of_arrival,
    vaccination_exists::text AS vaccination_exists,
    name_of_vaccine::text AS name_of_vaccine,
    doses_of_vaccine::text AS doses_of_vaccine,
    date_of_vaccination::text AS date_of_vaccination,
    were_samples_collected::text AS were_samples_collected,
    laboratory_samples_collected::text AS laboratory_samples_collected,
    date_of_sample_collection::text AS date_of_sample_collection,
    laboratory_results::text AS laboratory_results,
    date_of_laboratory_testing_results::text AS date_of_laboratory_testing_results,
    title_laboratory_facility::text AS title_laboratory_facility,
    laboratory_facility_name::text AS laboratory_facility_name,
    laboratory_facility_mfl_number::text AS laboratory_facility_mfl_number,
    symptoms::text AS symptoms,
    symptoms_other::text AS symptoms_other,
    rash_type::text AS rash_type,
    date_of_onset_rash::text AS date_of_onset,
    residence_visited::text AS residence_visited,
    date_of_residence_visit::text AS date_of_residence_visit,
    case_epilinked::text AS case_epilinked,
    outcome::text AS outcome,
    date_of_death::text AS date_of_death,
    status_of_patient::text AS status_of_patient,
    date_of_discharge::text AS date_of_discharge
FROM {{ ref('stg_measles') }}
WHERE df_complete::text = 'true'
