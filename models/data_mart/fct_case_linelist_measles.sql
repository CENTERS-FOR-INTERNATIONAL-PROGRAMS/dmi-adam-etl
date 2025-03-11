{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['syndrome']},
      {'columns': ['disease']},
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
      {'columns': ['suspected']},
      {'columns': ['tested']},
      {'columns': ['confirmed']},
      {'columns': ['admitted']},
      {'columns': ['discharged']},
      {'columns': ['died']},
      {'columns': ['probable']},
      {'columns': ['contact']},
      {'columns': ['completed']},
    ]
)}}

SELECT
    id,
    parent_id,
    contact_tree_label,
    mform_id,
    mform_event,
    event_id,
    form_id,
    case_unique_id,
    completed,
    parent_completed,
    created_role,
    modified_role,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    location_accuracy,
    location_latitude,
    location_longitude,
    syndrome,
    'Measles' AS disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    date_of_birth,
    phone_number,
    occupation,
    occupation_other,
    marital_status,
    case_is_pregnant,
    case_has_insurance_cover,
    level_of_education,
    type_of_residence,
    country,
    county,
    subcounty,
    ward,
    town_village_camp,
    landmark,
    title_residence,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    case_has_travelled,
    title_origin,
    origin_country,
    origin_county,
    origin_city_town_illage_camp,
    date_of_departure,
    title_destination,
    destination_country,
    destination_county,
    destination_city_town_village_camp,
    date_of_arrival,
    vaccination_exists,
    name_of_vaccine,
    doses_of_vaccine,
    date_of_vaccination,
    were_samples_collected,
    laboratory_samples_collected,
    date_of_sample_collection,
    laboratory_results,
    date_of_laboratory_testing_results,
    title_laboratory_facility,
    laboratory_facility_name,
    laboratory_facility_mfl_number,
    symptoms,
    symptoms_other,
    rash_type,
    date_of_onset,
    residence_visited,
    date_of_residence_visit,
    case_epilinked,
    outcome,
    date_of_death,
    status_of_patient,
    date_of_discharge,
    (CASE WHEN type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
    (CASE WHEN were_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN laboratory_results = 'Positive' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN outcome = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN outcome = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_measles') }}
