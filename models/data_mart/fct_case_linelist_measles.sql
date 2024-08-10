{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
    ]
)}}

SELECT
    mform_id,
    form_id,
    case_unique_id,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    location_accuracy,
    location_latitude,
    location_longitude,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_group,
    date_of_birth,
    phone_number,
    occupation,
    occupation_other,
    country,
    county,
    subcounty,
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
    date_of_discharge
FROM {{ ref('int_measles') }}
